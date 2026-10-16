(ns otel.sdk.metric-reader-policy-test
  (:require [clojure.test :refer [deftest is run-tests]]
            [otel.exporter.memory :as memory]
            [otel.sdk :as sdk]
            [otel.sdk.export :as export]
            [otel.sdk.lifecycle :as lifecycle]
            [otel.sdk.metrics :as metrics]))

(defn error-data [f]
  (try (f) nil (catch Throwable error (ex-data error))))

(deftest duration-domains-and-defaults
  (is (= {:shutdown-cancel-grace-ms 250 :shutdown-join-bound-ms 2000}
         (lifecycle/shutdown-policy {})))
  (is (= {:shutdown-cancel-grace-ms 0 :shutdown-join-bound-ms 2147483647}
         (lifecycle/shutdown-policy {:shutdown-cancel-grace-ms 0
                                     :shutdown-join-bound-ms 2147483647})))
  (doseq [field [:shutdown-cancel-grace-ms :shutdown-join-bound-ms]
          value [nil -1 1.5 Double/NaN Double/POSITIVE_INFINITY
                 2147483648 "1" true]]
    (is (= {:otel.sdk/error :invalid-shutdown-options :field field}
           (error-data #(lifecycle/shutdown-policy {field value})))))
  (is (= {:otel.sdk/error :invalid-shutdown-options :field :shutdown-join-bound-ms}
         (error-data #(lifecycle/shutdown-policy {:shutdown-join-bound-ms 0})))))

(deftest invalid-policy-precedes-worker-acquisition
  (let [starts (atom 0) target (memory/multisignal-exporter)]
    (with-redefs [lifecycle/start-owned-worker! (fn [& _] (swap! starts inc))]
      (is (= {:otel.sdk/error :invalid-shutdown-options :field :shutdown-cancel-grace-ms}
             (error-data #(metrics/periodic-reader {} target
                            {:shutdown-cancel-grace-ms -1}))))
      (is (zero? @starts)))
    (is (false? (:shutdown? @(:state target))))))

(deftest invalid-sdk-policy-precedes-exporter-factories
  (let [factories (atom 0)]
    (with-redefs [sdk/build-span-exporter (fn [& _] (swap! factories inc))]
      (doseq [options [false [] {:interval-ms 1} {"private-key" "private-value"}]]
        (is (= {:otel.sdk/error :invalid-metric-reader-options}
               (error-data #(sdk/init! {:metric-reader-options options})))))
      (is (= {:otel.sdk/error :invalid-shutdown-options :field :shutdown-join-bound-ms}
             (error-data #(sdk/init! {:metric-reader-options {:shutdown-join-bound-ms 0}}))))
      (is (zero? @factories)))))

(deftest sdk-forwards-explicit-owner-policy-without-changing-global-defaults
  (let [policy {:shutdown-cancel-grace-ms 2000 :shutdown-join-bound-ms 3000}
        handle (sdk/init! {:exporter (memory/multisignal-exporter)
                          :processor :simple :runtime-metrics? false
                          :metric-interval-ms 600000 :metric-reader-options policy})]
    (try
      (is (= policy (select-keys (:config (:reader handle)) (keys policy))))
      (is (= 600000 (get-in handle [:reader :config :interval-ms])))
      (is (= 250 lifecycle/shutdown-cancel-grace-ms))
      (is (= 2000 lifecycle/shutdown-join-bound-ms))
      (finally (is (true? (sdk/shutdown! handle)))))))

(deftest reader-policies-are-independent-owner-state
  (let [pa (metrics/meter-provider {}) pb (metrics/meter-provider {})
        a (metrics/periodic-reader pa (memory/multisignal-exporter)
                                  {:interval-ms 600000 :shutdown-cancel-grace-ms 1500
                                   :shutdown-join-bound-ms 1111})
        b (metrics/periodic-reader pb (memory/multisignal-exporter)
                                  {:interval-ms 600000 :shutdown-cancel-grace-ms 2500
                                   :shutdown-join-bound-ms 2222})]
    (try
      (is (= 1500 (get-in a [:config :shutdown-cancel-grace-ms])))
      (is (= 2500 (get-in b [:config :shutdown-cancel-grace-ms])))
      (is (= 1111 (get-in a [:config :shutdown-join-bound-ms])))
      (is (= 2222 (get-in b [:config :shutdown-join-bound-ms])))
      (is (= 250 lifecycle/shutdown-cancel-grace-ms))
      (finally
        (is (true? (export/shutdown! a)))
        (is (true? (export/shutdown! b)))
        (metrics/shutdown! pa) (metrics/shutdown! pb)))))

(deftest explicit-zero-grace-cancels-without-an-unbounded-join
  (let [entered (promise) interrupted (atom false)
        state (atom {:shutdown? true :worker-export-active? true :worker-interrupt-count 0})
        worker (Thread. (fn []
                          (deliver entered true)
                          (try (Thread/sleep 10000)
                               (catch InterruptedException _ (reset! interrupted true))
                               (finally (swap! state assoc :worker-export-active? false)))))]
    (.start worker)
    (try
      (is (= true (deref entered 2000 :timeout)))
      (let [result (future (lifecycle/await-owned-worker!
                            worker state {:shutdown-cancel-grace-ms 0
                                          :shutdown-join-bound-ms 1000}))]
        ;; If join(0) accidentally blocks, cleanup still interrupts the worker.
        (is (= true (deref result 2000 :timeout))))
      (is @interrupted)
      (is (= true (:shutdown-cancelled? @state)))
      (is (= 1 (:worker-interrupt-count @state)))
      (finally (.interrupt worker) (.join worker 2000)))))

(defn -main [& _]
  (let [{:keys [fail error]} (run-tests 'otel.sdk.metric-reader-policy-test)]
    (System/exit (if (zero? (+ fail error)) 0 1))))
