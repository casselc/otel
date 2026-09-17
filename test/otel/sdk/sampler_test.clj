(ns otel.sdk.sampler-test
  (:require [clojure.test :refer [deftest is testing]]
            [otel.context :as ctx]
            [otel.id :as id]
            [otel.sdk.sampler :as sampler]
            [otel.trace :as trace]))

(defn- params
  ([] (params {}))
  ([m] (merge {:parent-context ctx/root
               :trace-id (id/trace-id)
               :name "op"
               :kind :internal
               :attributes {}
               :links []}
              m)))

(defn- parent-ctx [sampled? remote?]
  (trace/context-with-span
    ctx/root
    (trace/non-recording-span
      (trace/span-context {:trace-id (id/trace-id)
                           :span-id (id/span-id)
                           :sampled? sampled?
                           :remote? remote?}))))

(deftest always-on-samples-everything
  (is (= :record-and-sample (:decision (sampler/should-sample sampler/always-on (params)))))
  (is (= "AlwaysOnSampler" (sampler/description sampler/always-on))))

(deftest always-off-drops-everything
  (is (= :drop (:decision (sampler/should-sample sampler/always-off (params)))))
  (is (= "AlwaysOffSampler" (sampler/description sampler/always-off))))

(deftest ratio-sampler-bounds
  (testing "ratio 1.0 samples every trace id and 0.0 samples none"
    (let [on (sampler/trace-id-ratio 1.0)
          off (sampler/trace-id-ratio 0.0)]
      (dotimes [_ 100]
        (let [t (id/trace-id)]
          (is (= :record-and-sample (:decision (sampler/should-sample on (params {:trace-id t})))))
          (is (= :drop (:decision (sampler/should-sample off (params {:trace-id t}))))))))))

(deftest ratio-sampler-is-approximately-the-ratio
  (let [s (sampler/trace-id-ratio 0.25)
        n 4000
        sampled (count (filter (fn [_]
                                 (= :record-and-sample
                                    (:decision (sampler/should-sample s (params {:trace-id (id/trace-id)})))))
                               (range n)))
        rate (/ (double sampled) n)]
    (is (< 0.20 rate 0.30) (str "sampled rate was " rate))))

(deftest ratio-sampler-is-deterministic-for-a-trace-id
  (testing "the same trace id must decide the same way in every process, or a
            distributed trace would be sampled in one service and not the next"
    (let [s (sampler/trace-id-ratio 0.5)
          t (id/trace-id)
          d1 (:decision (sampler/should-sample s (params {:trace-id t})))
          d2 (:decision (sampler/should-sample s (params {:trace-id t :name "other"})))]
      (is (= d1 d2)))))

(deftest ratio-sampler-reconstructs-the-complete-signed-low-half
  (doseq [prefix ["1000000000000000" "fedcba9876543210"]
          [suffix expected]
          [["0000000000000000" 0]
           ["0000000000000001" 1]
           ["7fffffffffffffff" 9223372036854775807]
           ["8000000000000000" -9223372036854775808]
           ["ffffffffffffffff" -1]]]
    (let [trace-id (str prefix suffix)]
      (is (= expected (#'sampler/trace-id-random-part trace-id)))
      (is (= :record-and-sample
             (:decision (sampler/should-sample (sampler/trace-id-ratio 1.0)
                                               (params {:trace-id trace-id})))))
      (is (= :drop
             (:decision (sampler/should-sample (sampler/trace-id-ratio 0.0)
                                               (params {:trace-id trace-id}))))))))

(deftest ratio-sampler-compares-both-signed-magnitude-thresholds
  (let [s (sampler/trace-id-ratio 0.5)]
    (doseq [prefix ["1000000000000000" "fedcba9876543210"]
            [suffix expected]
            [["3fffffffffffffff" :record-and-sample]
             ["4000000000000000" :drop]
             ["c000000000000000" :drop]
             ["c000000000000001" :record-and-sample]]]
      (is (= expected (:decision (sampler/should-sample
                                   s (params {:trace-id (str prefix suffix)}))))))))

(deftest ratio-sampler-rejects-malformed-low-halves-and-accepts-uppercase-hex
  (doseq [suffix ["00000001+0000001" "00000001-0000001"
                  "+000000100000001" "-000000100000001"
                  "000000010000000g"]]
    (let [error (try (#'sampler/trace-id-random-part (str "1000000000000000" suffix))
                     nil (catch Throwable error error))]
      (is (some? error))
      (is (= {:type :otel.sdk.sampler/invalid-trace-id-low-half} (ex-data error)))
      (is (= "Invalid trace ID low half" (ex-message error)))
      (is (nil? (ex-cause error)))))
  (let [trace-id "1000000000000000FFFFFFFFFFFFFFFF"]
    (is (= -1 (#'sampler/trace-id-random-part trace-id)))
    (is (= :record-and-sample
           (:decision (sampler/should-sample (sampler/trace-id-ratio 1.0)
                                             (params {:trace-id trace-id})))))
    (is (= :drop
           (:decision (sampler/should-sample (sampler/trace-id-ratio 0.0)
                                             (params {:trace-id trace-id})))))))

(deftest ratio-sampler-describes-its-ratio
  (is (= "TraceIdRatioBased{0.25}" (sampler/description (sampler/trace-id-ratio 0.25)))))

(deftest ratio-sampler-rejects-a-bad-ratio
  (is (thrown? Exception (sampler/trace-id-ratio -0.1)))
  (is (thrown? Exception (sampler/trace-id-ratio 1.1))))

(deftest parent-based-follows-a-sampled-parent
  (let [s (sampler/parent-based {})]
    (is (= :record-and-sample
           (:decision (sampler/should-sample s (params {:parent-context (parent-ctx true false)})))))
    (is (= :record-and-sample
           (:decision (sampler/should-sample s (params {:parent-context (parent-ctx true true)})))))))

(deftest parent-based-follows-an-unsampled-parent
  (let [s (sampler/parent-based {})]
    (is (= :drop
           (:decision (sampler/should-sample s (params {:parent-context (parent-ctx false false)})))))
    (is (= :drop
           (:decision (sampler/should-sample s (params {:parent-context (parent-ctx false true)})))))))

(deftest parent-based-uses-the-root-sampler-with-no-parent
  (testing "no parent means this span starts a trace, so the root sampler decides"
    (is (= :record-and-sample
           (:decision (sampler/should-sample (sampler/parent-based {:root sampler/always-on}) (params)))))
    (is (= :drop
           (:decision (sampler/should-sample (sampler/parent-based {:root sampler/always-off}) (params)))))))

(deftest parent-based-root-defaults-to-always-on
  (is (= :record-and-sample (:decision (sampler/should-sample (sampler/parent-based {}) (params))))))

(deftest parent-based-honours-per-case-overrides
  (testing "a remote sampled parent can be overridden independently of a local one"
    (let [s (sampler/parent-based {:remote-parent-sampled sampler/always-off})]
      (is (= :drop
             (:decision (sampler/should-sample s (params {:parent-context (parent-ctx true true)})))))
      (testing "the local case is unaffected"
        (is (= :record-and-sample
               (:decision (sampler/should-sample s (params {:parent-context (parent-ctx true false)})))))))))

(deftest parent-based-ignores-an-invalid-parent
  (testing "a context holding the invalid span is not a parent — the root sampler decides"
    (let [c (trace/context-with-span ctx/root trace/invalid-span)]
      (is (= :drop
             (:decision (sampler/should-sample (sampler/parent-based {:root sampler/always-off})
                                               (params {:parent-context c}))))))))

(deftest sampler-may-contribute-attributes
  (testing "every result carries an attribute map for the caller to merge onto the span"
    (is (map? (:attributes (sampler/should-sample sampler/always-on (params)))))
    (is (map? (:attributes (sampler/should-sample sampler/always-off (params)))))))

(deftest decision-helpers
  (is (sampler/sampled? {:decision :record-and-sample}))
  (is (not (sampler/sampled? {:decision :record-only})))
  (is (not (sampler/sampled? {:decision :drop})))
  (is (sampler/recording? {:decision :record-and-sample}))
  (is (sampler/recording? {:decision :record-only}))
  (is (not (sampler/recording? {:decision :drop}))))
