(ns otel.trace-test
  (:require [clojure.test :refer [deftest is testing]]
            [otel.context :as ctx]
            [otel.id :as id]
            [otel.trace :as trace]))

;; --- span context -----------------------------------------------------------

(deftest span-context-carries-its-identity
  (let [t (id/trace-id) s (id/span-id)
        sc (trace/span-context {:trace-id t :span-id s})]
    (is (= t (:trace-id sc)))
    (is (= s (:span-id sc)))
    (testing "defaults: not sampled, not remote, empty trace state"
      (is (not (trace/sampled? sc)))
      (is (not (:remote? sc)))
      (is (= [] (:trace-state sc))))))

(deftest sampled-flag-round-trips
  (let [sc (trace/span-context {:trace-id (id/trace-id) :span-id (id/span-id) :sampled? true})]
    (is (trace/sampled? sc))
    (is (= 1 (:trace-flags sc))))
  (let [sc (trace/span-context {:trace-id (id/trace-id) :span-id (id/span-id) :sampled? false})]
    (is (not (trace/sampled? sc)))
    (is (= 0 (:trace-flags sc)))))

(deftest sampled-reads-only-the-low-bit
  (testing "unknown flag bits must be preserved but must not affect sampled?"
    (let [sc (trace/span-context {:trace-id (id/trace-id) :span-id (id/span-id) :trace-flags 0xfe})]
      (is (not (trace/sampled? sc))))
    (let [sc (trace/span-context {:trace-id (id/trace-id) :span-id (id/span-id) :trace-flags 0xff})]
      (is (trace/sampled? sc)))))

(deftest span-context-validity
  (is (trace/valid? (trace/span-context {:trace-id (id/trace-id) :span-id (id/span-id)})))
  (testing "an all-zero component makes the context invalid"
    (is (not (trace/valid? (trace/span-context {:trace-id id/invalid-trace-id
                                                :span-id (id/span-id)}))))
    (is (not (trace/valid? (trace/span-context {:trace-id (id/trace-id)
                                                :span-id id/invalid-span-id})))))
  (is (not (trace/valid? trace/invalid-span-context)))
  (is (not (trace/valid? nil))))

;; --- non-recording spans ----------------------------------------------------

(deftest non-recording-span-keeps-its-context
  (testing "a non-recording span still propagates — that is how an extracted
            remote context reaches downstream calls when no SDK is installed"
    (let [sc (trace/span-context {:trace-id (id/trace-id) :span-id (id/span-id) :sampled? true})
          sp (trace/non-recording-span sc)]
      (is (= sc (trace/span-context-of sp)))
      (is (not (trace/recording? sp))))))

(deftest non-recording-span-mutations-are-no-ops
  (let [sp (trace/non-recording-span
             (trace/span-context {:trace-id (id/trace-id) :span-id (id/span-id)}))]
    (testing "every mutating call is accepted and does nothing, so instrumentation
              never has to check whether an SDK is present"
      (is (= sp (trace/set-attribute! sp :k 1)))
      (is (= sp (trace/set-attributes! sp {:k 1})))
      (is (= sp (trace/add-event! sp "e")))
      (is (= sp (trace/set-status! sp :error "bad")))
      (is (= sp (trace/update-name! sp "other")))
      (is (= sp (trace/record-exception! sp (ex-info "x" {}))))
      (is (nil? (trace/end! sp))))))

;; --- context integration ----------------------------------------------------

(deftest no-active-span-yields-the-invalid-span
  (testing "reading the current span outside any span gives a valid object, not nil"
    (let [sp (trace/current-span)]
      (is (not (trace/recording? sp)))
      (is (not (trace/valid? (trace/span-context-of sp)))))))

(deftest a-span-can-be-made-current
  (let [sc (trace/span-context {:trace-id (id/trace-id) :span-id (id/span-id) :sampled? true})
        sp (trace/non-recording-span sc)]
    (trace/with-current-span sp
      (is (= sc (trace/span-context-of (trace/current-span))))
      (is (= sc (trace/current-span-context))))
    (testing "and is removed on the way out"
      (is (not (trace/valid? (trace/current-span-context)))))))

(deftest context-with-span-round-trips
  (let [sp (trace/non-recording-span
             (trace/span-context {:trace-id (id/trace-id) :span-id (id/span-id)}))
        c (trace/context-with-span ctx/root sp)]
    (is (= sp (trace/span-from-context c)))
    (testing "a context with no span reports the invalid span"
      (is (not (trace/valid? (trace/span-context-of (trace/span-from-context ctx/root))))))))

;; --- status and kind --------------------------------------------------------

(deftest status-codes-are-the-spec-set
  (is (= #{:unset :ok :error} (set trace/status-codes))))

(deftest span-kinds-are-the-spec-set
  (is (= #{:internal :server :client :producer :consumer} (set trace/span-kinds))))
