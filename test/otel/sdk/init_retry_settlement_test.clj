(ns otel.sdk.init-retry-settlement-test
  (:require [clojure.test :refer [deftest is]]
            [otel.instrument.runtime :as runtime]
            [otel.sdk :as sdk]
            [otel.sdk.export :as export]
            [otel.sdk.lifecycle :as lifecycle]
            [otel.sdk.tracer :as tracer]
            [otel.trace :as trace]))

(deftest init-failure-retries-refresh-real-exporter-settlement-without-replay
  (doseq [release-kind [:false :throw]]
    (let [receipt (lifecycle/construction-receipt) owner (atom nil)
          started (promise) finish (promise)
          user (Thread. #(do (deliver started true) @finish))
          release-requested? (atom false) closes (atom 0)
          failure (ex-info "controlled registration failure" {})
          release-failure (ex-info "controlled exporter release failure" {})
          exporter (reify export/SpanExporter
                     (export-spans! [_ _] true) (flush-exporter! [_] true)
                     (shutdown-exporter! [_]
                       (swap! closes inc) (reset! release-requested? true)
                       (if (= :throw release-kind) (throw release-failure) false))
                     lifecycle/SettlementWitness
                     ;; Truthful permanent fixture contract: this independent
                     ;; exporter user must really exit before proof confirms.
                     (settlement-status [_]
                       {:quiescence (if (and @release-requested? (not (.isAlive user)))
                                      :confirmed :unconfirmed)}))
          batch export/batch-processor]
      (.setDaemon user true) (.start user)
      (try
        (is (= true (deref started 2000 :timeout)))
        (is (nil? (sdk/tracer-provider)))
        (let [error
              (with-redefs [export/batch-processor
                            (fn [& args] (let [acquired (apply batch args)]
                                          (reset! owner acquired) acquired))
                            runtime/register! (fn [_] (throw failure))]
                (try (sdk/init! {:exporter exporter :metrics? true :runtime-metrics? true
                                 :schedule-delay-ms 60000 :construction-receipt receipt})
                     nil (catch Throwable error error)))
              retry-stop! (:retry-stop! (ex-data error))]
          (is (= #{:otel.sdk/error :retry-stop!} (set (keys (ex-data error)))))
          (is (= :construction-cleanup-incomplete (:otel.sdk/error (ex-data error))))
          (is (nil? (.getCause error)))
          (is (not (identical? failure error)))
          (is (fn? retry-stop!))
          (is (identical? exporter (:exporter @owner)))
          (is (not (.isAlive (:worker @owner))))
          (is (.isAlive user))
          (is (= 1 @closes))
          (is (= :unconfirmed (:quiescence (lifecycle/component-settlement @owner))))
          (is (= :unconfirmed (:quiescence (lifecycle/construction-failure-status receipt error))))
          (is (= :unconfirmed (:quiescence (lifecycle/construction-failure-status receipt failure))))
          (is (= :closing (:status (retry-stop!))))
          (is (= 1 @closes))
          (deliver finish true) (.join user 2000)
          (is (not (.isAlive user)))
          (let [results [(promise) (promise)]
                retries (mapv #(Thread. (fn [] (deliver % (retry-stop!)))) results)]
            (doseq [worker retries] (.start worker))
            (try
              (doseq [result results]
                (is (= :closed (:status (deref result 2000 :timeout)))))
              (finally
                (doseq [worker retries]
                  (.join worker 2000) (is (not (.isAlive worker)))))))
          (is (= :confirmed (:quiescence (lifecycle/component-settlement @owner))))
          (is (= :confirmed (:quiescence (lifecycle/construction-failure-status receipt error))))
          (is (= :closed (:status (retry-stop!))))
          (is (= 1 @closes))
          (is (nil? (sdk/tracer-provider)))
          (is (nil? (sdk/meter-provider))))
        (finally
          (deliver finish true) (.join user 2000)
          (when @owner
            (try (export/shutdown! @owner) (catch Throwable _ nil))
            (.join (:worker @owner) 2000)
            (is (not (.isAlive (:worker @owner)))))
          (lifecycle/retire-construction! receipt)
          (is (not (.isAlive user)))
          (is (= 1 @closes)))))))

(deftest successful-init-outside-error-and-reused-receipt-remain-unknown
  (let [receipt (lifecycle/construction-receipt) handle (atom nil)
        failure (ex-info "outside successful initialization" {})]
    (try
      (let [error (try (reset! handle (sdk/init! {:exporter :none :metrics? false
                                               :construction-receipt receipt}))
                       (throw failure) (catch Throwable error error))]
        (is (identical? failure error))
        (is (some? @handle))
        (is (= :unknown (:outcome (lifecycle/construction-failure-status receipt error))))
        (is (= :unknown (:outcome (lifecycle/construction-failure-status nil error))))
        (let [reuse-error (try (sdk/init! {:exporter :none :metrics? false
                                         :construction-receipt receipt})
                              nil (catch Throwable error error))]
          (is (= :invalid-construction-receipt (:otel.sdk/error (ex-data reuse-error))))
          (is (= :unknown (:outcome (lifecycle/construction-failure-status receipt reuse-error)))))
        (sdk/shutdown! @handle)
        (is (= :unknown (:outcome (lifecycle/construction-failure-status receipt error))))
        (is (nil? (sdk/tracer-provider))))
      (finally (when @handle (sdk/shutdown! @handle))))))

(deftest actual-init-worker-timeout-denies-release-until-independent-retirement
  (let [receipt (lifecycle/construction-receipt) owner (atom nil) provider (atom nil)
        entered (promise) finish (promise) seam-observation (atom nil)
        sdk-closes (atom 0) external-closes (atom 0) externally-retired? (atom false)
        failure (ex-info "controlled registration after real export begins" {})
        exporter (reify export/SpanExporter
                   (export-spans! [_ _]
                     (deliver entered true)
                     ;; Interruption cannot certify this real worker's exit.
                     (loop []
                       (let [done? (try @finish (catch InterruptedException _ false))]
                         (when-not done? (recur))))
                     true)
                   (flush-exporter! [_] true)
                   (shutdown-exporter! [_] (swap! sdk-closes inc) true)
                   lifecycle/SettlementWitness
                   (settlement-status [_]
                     {:quiescence (if (and @externally-retired? @owner
                                           (not (.isAlive (:worker @owner))))
                                    :confirmed :unconfirmed)}))
        batch export/batch-processor make-provider tracer/tracer-provider]
    (try
      (let [error
            (with-redefs [export/batch-processor
                          (fn [& args] (let [acquired (apply batch args)]
                                        (reset! owner acquired) acquired))
                          tracer/tracer-provider
                          (fn [& args] (let [acquired (apply make-provider args)]
                                        (reset! provider acquired) acquired))
                          lifecycle/shutdown-cancel-grace-ms 1
                          lifecycle/shutdown-join-bound-ms 5
                          runtime/register!
                          (fn [_]
                            (trace/end! (trace/start-span
                                          (tracer/get-tracer @provider {:name "timeout-fixture"})
                                          "held-real-export"))
                            (reset! seam-observation (deref entered 2000 :timeout))
                            (throw failure))]
              (try (sdk/init! {:exporter exporter :metrics? true :runtime-metrics? true
                               :schedule-delay-ms 1 :max-export-batch-size 1
                               :construction-receipt receipt})
                   nil (catch Throwable error error)))
            retry-stop! (:retry-stop! (ex-data error))]
        (is (= true @seam-observation))
        (is (= :construction-cleanup-incomplete (:otel.sdk/error (ex-data error))))
        (is (nil? (.getCause error)))
        (is (identical? exporter (:exporter @owner)))
        (is (.isAlive (:worker @owner)))
        (is (true? (:shutdown? @(:state @owner))))
        (is (true? (:shutdown-cancelled? @(:state @owner))))
        (is (= 1 (:worker-interrupt-count @(:state @owner))))
        (is (= :threw (lifecycle/terminal-status (:terminal @owner))))
        (is (zero? @sdk-closes))
        (is (= :unconfirmed (:quiescence (lifecycle/construction-failure-status receipt error))))
        (is (= :closing (:status (retry-stop!))))
        (is (zero? @sdk-closes))
        (deliver finish true)
        (.join (:worker @owner) 2000)
        (is (not (.isAlive (:worker @owner))))
        (is (= :confirmed (:sdk-quiescence (lifecycle/component-settlement @owner))))
        (is (= :unconfirmed (:exporter-quiescence (lifecycle/component-settlement @owner))))
        (is (= :closing (:status (retry-stop!))))
        (is (zero? @sdk-closes))
        ;; Independent fixture-owned resource retirement AFTER actual join.
        ;; This is not replayed SDK release or an SDK delivery guarantee.
        (swap! external-closes inc)
        (reset! externally-retired? true)
        (is (= :closed (:status (retry-stop!))))
        (is (= :confirmed (:quiescence (lifecycle/construction-failure-status receipt error))))
        (is (zero? @sdk-closes))
        (is (= 1 @external-closes))
        (is (nil? (sdk/tracer-provider))))
      (finally
        (deliver finish true)
        (when @owner
          (.join (:worker @owner) 2000)
          (try (export/shutdown! @owner) (catch Throwable _ nil))
          (is (not (.isAlive (:worker @owner))))
          (when-not @externally-retired?
            (swap! external-closes inc) (reset! externally-retired? true)))
        (lifecycle/retire-construction! receipt)
        (is (zero? @sdk-closes))
        (is (= 1 @external-closes))))))
