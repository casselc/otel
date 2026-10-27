(ns otel.sdk.init-preparation-test
  "Initialization routing/ownership controls, not a native persistence test."
  (:require [clojure.test :refer [deftest is]]
            [otel.instrument.runtime :as runtime]
            [otel.logs :as logs]
            [otel.metrics :as metrics]
            [otel.sdk :as sdk]
            [otel.sdk.export :as export]
            [otel.sdk.lifecycle :as lifecycle]
            [otel.sdk.logs :as sdk-logs]
            [otel.trace :as trace]))

(defn- fixture []
  (let [state (atom {:ordinary-spans 0 :confirmed-spans 0 :metric-exports 0
                     :log-records 0 :releases {:spans 0 :metrics 0 :logs 0}})
        live (atom #{})
        exporter
        (reify export/SpanExporter
          (export-spans! [_ spans] (swap! state update :ordinary-spans + (count spans)) true)
          (flush-exporter! [_] true)
          (shutdown-exporter! [_] (swap! state update-in [:releases :spans] inc) true)
          export/MetricExporter
          (export-metrics! [_ _ _] (swap! state update :metric-exports inc) true)
          (shutdown-metric-exporter! [_] (swap! state update-in [:releases :metrics] inc) true)
          sdk-logs/LogRecordExporter
          (export-logs! [_ records] (swap! state update :log-records + (count records)) true)
          (shutdown-log-exporter! [_] (swap! state update-in [:releases :logs] inc) true))
        adapter
        {:reserve! (fn [n] (let [cap (atom {:count n})] (swap! live conj cap) cap))
         :prepare! (fn [_ _] true)
         :submit! (fn [_] true)
         ;; This explicit test adapter simulates confirmation only. Native
         ;; persistence and adapter ordering are qualified in separate suites.
         :await! (fn [cap] (swap! state update :confirmed-spans + (:count @cap)) true)
         :abort! (fn [_] true)
         :release! (fn [_] true)
         :retire! (fn [cap] (swap! live disj cap) true)
         :fence! (fn [] true)}]
    {:exporter exporter :adapter adapter :state state :live live}))

(defn- options [exporter]
  {:exporter exporter :metrics? true :logs? true
   :runtime-metrics? false :bridge-logging? false
   :schedule-delay-ms 60000 :metric-interval-ms 60000
   :max-export-batch-size 1 :max-queue-size 8})

(defn- emit-span! []
  (trace/with-span [span (sdk/tracer "init-preparation-control") "span"]
    (is (trace/recording? span))))

(deftest init-forwards-the-adapter-without-dropping-other-signals
  (let [{:keys [exporter adapter state live]} (fixture)
        handle (sdk/init! (assoc (options exporter) :experimental-span-preparation adapter))]
    (try
      (emit-span!)
      (metrics/add! (metrics/counter (sdk/meter "init-preparation-control") "count") 1)
      (logs/emit! (sdk/logger "init-preparation-control") {:body "bounded fixture" :severity :info})
      (is (true? (sdk/force-flush! handle)))
      (is (= 1 (:confirmed-spans @state)))
      (is (zero? (:ordinary-spans @state)) "must not silently select synchronous export")
      (is (pos? (:metric-exports @state)))
      (is (= 1 (:log-records @state)))
      (is (empty? @live))
      (finally (sdk/shutdown! handle)))
    (is (= :confirmed (:quiescence (sdk/shutdown-status handle))))
    (is (= {:spans 1 :metrics 1 :logs 1} (:releases @state)))
    (sdk/shutdown! handle)
    (is (= {:spans 1 :metrics 1 :logs 1} (:releases @state)))))

(deftest invalid-adapter-rejects-with-matching-construction-proof
  (doseq [invalid [true {} {:reserve! identity}]]
    (let [{:keys [exporter state]} (fixture)
          receipt (lifecycle/construction-receipt)
          outcome (try {:handle (sdk/init! (assoc (options exporter)
                                            :metrics? false :logs? false
                                            :construction-receipt receipt
                                            :experimental-span-preparation invalid))}
                       (catch Throwable error {:error error}))]
      (try
        (is (= :invalid-preparation-adapter (:otel.sdk/error (ex-data (:error outcome)))))
        (is (= :confirmed (:quiescence (lifecycle/construction-failure-status receipt (:error outcome)))))
        (is (= {:spans 1 :metrics 0 :logs 0} (:releases @state)))
        (finally (when-let [handle (:handle outcome)] (sdk/shutdown! handle)))))))

(deftest later-init-failure-retires-the-actual-preparation-worker
  (let [{:keys [exporter adapter state live]} (fixture)
        receipt (lifecycle/construction-receipt) owners (atom [])
        failure (ex-info "controlled post-construction failure" {})
        start lifecycle/start-owned-worker!
        outcome
        (with-redefs [lifecycle/start-owned-worker!
                      (fn [child owner worker]
                        (let [result (start child owner worker)]
                          (swap! owners conj owner) result))
                      runtime/register! (fn [_] (throw failure))]
          (try {:handle (sdk/init! (assoc (options exporter)
                                    :runtime-metrics? true :construction-receipt receipt
                                    :experimental-span-preparation adapter))}
               (catch Throwable error {:error error})))]
    (try
      (is (identical? failure (:error outcome)))
      (is (= 3 (count @owners)))
      (is (some #(identical? adapter (get-in % [:config :experimental-span-preparation])) @owners))
      (is (every? #(not (.isAlive (:worker %))) @owners))
      (is (= :confirmed (:quiescence (lifecycle/construction-failure-status receipt failure))))
      (is (= {:spans 1 :metrics 1 :logs 1} (:releases @state)))
      (is (empty? @live))
      (is (nil? (sdk/tracer-provider)))
      (is (nil? (sdk/meter-provider)))
      (is (nil? (sdk/logger-provider)))
      (finally (when-let [handle (:handle outcome)] (sdk/shutdown! handle))))))

(deftest omitted-or-nil-adapter-keeps-ordinary-export
  (doseq [extra [{} {:experimental-span-preparation nil}]]
    (let [{:keys [exporter state]} (fixture)
          handle (sdk/init! (merge (options exporter) {:metrics? false :logs? false} extra))]
      (try
        (emit-span!)
        (is (true? (sdk/force-flush! handle)))
        (is (= 1 (:ordinary-spans @state)))
        (is (zero? (:confirmed-spans @state)))
        (finally (sdk/shutdown! handle))))))
