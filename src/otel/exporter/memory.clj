(ns otel.exporter.memory
  "An exporter that keeps everything in memory. For tests: assert on what
  instrumentation actually produced, without a collector or a network."
  (:require [otel.sdk.export :as export]
            [otel.sdk.logs :as sdk-logs]))

(defrecord InMemoryExporter [state]
  export/SpanExporter
  (export-spans! [_ spans]
    (let [{:keys [shutdown?]} @state]
      (if shutdown?
        false
        (do (swap! state (fn [s] (-> s
                                     (update :spans into spans)
                                     ;; batches are kept separately so a test can
                                     ;; assert on how the spans were grouped, not
                                     ;; just that they arrived.
                                     (update :batches conj (vec spans)))))
            true))))
  (flush-exporter! [_] true)
  (shutdown-exporter! [_] (swap! state assoc :shutdown? true) true))

(defn exporter
  "A fresh in-memory exporter."
  []
  (->InMemoryExporter (atom {:spans [] :batches [] :shutdown? false})))

(defn spans
  "Every span handed to the exporter, in order."
  [e]
  (:spans @(:state e)))

(defn batches
  "The spans grouped as the processor delivered them."
  [e]
  (:batches @(:state e)))

(defn shutdown?
  "Whether the exporter has been shut down."
  [e]
  (:shutdown? @(:state e)))

(defn reset!!
  "Forget everything collected so far."
  [e]
  (swap! (:state e) assoc :spans [] :batches [])
  e)

;; --- metrics ----------------------------------------------------------------

(defrecord InMemoryMetricExporter [state]
  export/MetricExporter
  (export-metrics! [_ resource collected]
    (if (:shutdown? @state)
      false
      (do (swap! state update :collections conj {:resource resource :collected collected})
          true)))
  (shutdown-metric-exporter! [_] (swap! state assoc :shutdown? true) true))

(defn metric-exporter
  "A fresh in-memory metric exporter."
  []
  (->InMemoryMetricExporter (atom {:collections [] :shutdown? false})))

(defn collections
  "Every metric collection handed to the exporter, in order."
  [e]
  (:collections @(:state e)))

(defn metrics
  "Every metric from every collection, flattened."
  [e]
  (mapcat (fn [c] (mapcat :metrics (:collected c))) (collections e)))

;; --- logs -------------------------------------------------------------------

(defrecord InMemoryLogExporter [state]
  sdk-logs/LogRecordExporter
  (export-logs! [_ records]
    (if (:shutdown? @state)
      false
      (do (swap! state update :records into records) true)))
  (shutdown-log-exporter! [_] (swap! state assoc :shutdown? true) true))

(defn log-exporter
  "A fresh in-memory log record exporter."
  []
  (->InMemoryLogExporter (atom {:records [] :shutdown? false})))

(defn records
  "Every log record handed to the exporter, in order."
  [e]
  (:records @(:state e)))
