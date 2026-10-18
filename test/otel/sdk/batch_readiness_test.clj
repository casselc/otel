(ns otel.sdk.batch-readiness-test
  (:require [clojure.test :refer [deftest is testing]]
            [otel.exporter.memory :as memory]
            [otel.sdk.export :as export]
            [otel.sdk.lifecycle :as lifecycle]))

(def ^:private sampled {:span-context {:trace-flags 1}})

(deftest a-ready-batch-exports-before-the-long-deadline
  (let [entered (promise) exported (promise)
        wait-var (ns-resolve 'otel.sdk.export 'sleep-until-due)
        original @wait-var
        exporter (reify export/SpanExporter
                   (export-spans! [_ spans] (deliver exported (count spans)) true)
                   (flush-exporter! [_] true)
                   (shutdown-exporter! [_] true))]
    (with-redefs-fn
      {wait-var (fn [& args] (deliver entered true) (apply original args))}
      (fn []
        (let [processor (export/batch-processor
                          exporter {:max-export-batch-size 2 :schedule-delay-ms 60000})]
          (try
            (is (= true (deref entered 1000 ::timeout)) "actual worker entered its wait")
            (export/on-end processor sampled)
            (export/on-end processor sampled)
            (is (= 2 (deref exported 1000 ::timeout))
                "queue readiness, not force-flush or the 60s timer, triggers export")
            (finally (is (true? (export/shutdown! processor))))))))))

(deftest wait-policy-checks-readiness-after-each-bounded-slice
  (let [wait! (ns-resolve 'otel.sdk.export 'sleep-until-due)
        sleep-var (ns-resolve 'otel.sdk.export 'sleep-slice!)]
    (testing "a ready queue and a stopped owner do not sleep"
      (doseq [state [{:queue [sampled sampled]} {:queue [] :shutdown? true}]]
        (let [slices (atom [])]
          (with-redefs-fn {sleep-var #(swap! slices conj %)}
            #(wait! (atom state) 60000 2))
          (is (= [] @slices)))))
    (testing "a partial batch retains the original deadline"
      (let [slices (atom [])]
        (with-redefs-fn {sleep-var #(swap! slices conj %)}
          #(wait! (atom {:queue [sampled]}) 125 2))
        (is (= [50 50 25] @slices))))
    (testing "enqueue between the check and a sleep cannot lose the demand"
      (let [state (atom {:queue [sampled]}) slices (atom [])]
        (with-redefs-fn
          {sleep-var (fn [ms] (swap! slices conj ms)
                       (swap! state update :queue conj sampled))}
          #(wait! state 60000 2))
        (is (= [50] @slices))))))

(deftest a-partial-batch-still-exports-at-its-deadline
  (let [exported (promise)
        exporter (reify export/SpanExporter
                   (export-spans! [_ spans] (deliver exported (count spans)) true)
                   (flush-exporter! [_] true)
                   (shutdown-exporter! [_] true))
        processor (export/batch-processor
                    exporter {:max-export-batch-size 2 :schedule-delay-ms 20})]
    (try
      (export/on-end processor sampled)
      (is (= 1 (deref exported 1000 ::timeout)))
      (finally (is (true? (export/shutdown! processor)))))))

(deftest a-held-exporter-does-not-block-producers-or-allow-export-overlap
  (let [entered (promise) release (promise) producer-done (promise)
        active (atom 0) maximum (atom 0) total (atom 0)
        exporter (reify export/SpanExporter
                   (export-spans! [_ spans]
                     (swap! maximum max (swap! active inc))
                     (try
                       (deliver entered true)
                       @release
                       (swap! total + (count spans))
                       true
                       (finally (swap! active dec))))
                   (flush-exporter! [_] true)
                   (shutdown-exporter! [_] true))
        processor (export/batch-processor
                    exporter {:max-queue-size 2 :max-export-batch-size 1
                              :schedule-delay-ms 60000})
        producer (Thread. (fn []
                            (dotimes [_ 10] (export/on-end processor sampled))
                            (deliver producer-done true)))]
    (try
      (export/on-end processor sampled)
      (is (= true (deref entered 1000 ::timeout)))
      (.start producer)
      (is (= true (deref producer-done 1000 ::timeout))
          "producers do not acquire the monitor held across exporter I/O")
      (is (= 2 (export/queue-size processor)))
      (is (= 8 (export/dropped-count processor)))
      (is (= 1 @maximum))
      (deliver release true)
      (is (true? (export/force-flush! processor)))
      (is (= 3 @total))
      (is (= 1 @maximum))
      (finally
        (deliver release true)
        (.join producer 2000)
        (is (true? (export/shutdown! processor)))))))

(deftest impossible-batch-size-is-rejected-before-worker-acquisition
  (let [starts (atom 0) start lifecycle/start-owned-worker!
        exporter (memory/exporter)]
    (with-redefs [lifecycle/start-owned-worker!
                  (fn [& args] (swap! starts inc) (apply start args))]
      (is (thrown? Exception
                   (export/batch-processor exporter
                     {:max-queue-size 1 :max-export-batch-size 2})))
      (is (zero? @starts))
      (is (false? (memory/shutdown? exporter))))
    (with-redefs [export/batch-processor (fn [& _] (swap! starts inc) ::unused)]
      (is (thrown? Exception
                   (export/independent-batch-pipelines
                     (array-map :first {:exporter exporter :config {}}
                                :invalid {:exporter exporter
                                          :config {:max-queue-size 1
                                                   :max-export-batch-size 2}}))))
      (is (zero? @starts)
          "validate the whole destination graph before constructing any child"))))
