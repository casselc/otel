(ns otel.exporter.stdout
  "Exporters that write spans to a stream instead of a collector.

  `exporter` prints a compact human-readable line per span — the fastest way to
  see whether instrumentation is doing what you think without standing up any
  infrastructure. `json-exporter` writes the exact OTLP/JSON payload that would
  have gone over the wire, which is what you want when debugging an encoding
  problem or feeding a file to a collector after the fact."
  (:require [otel.otlp.encode :as enc]
            [otel.otlp.json :as json]
            [otel.sdk.export :as export]))

(defn- duration-ms [s]
  (/ (double (- (or (:end-time-unix-nano s) 0) (or (:start-time-unix-nano s) 0))) 1000000.0))

(defn- format-span [s]
  (let [sc (:span-context s)
        status (:status s)]
    (str (format "%-8.3fms " (duration-ms s))
         (:name s)
         " [" (name (:kind s)) "]"
         " trace=" (subs (str (:trace-id sc)) 0 16) "…"
         " span=" (:span-id sc)
         (when-let [p (:parent-span-id s)] (str " parent=" p))
         (when (not= :unset (:code status))
           (str " status=" (name (:code status))
                (when-let [d (:description status)] (str "(" d ")"))))
         (when (seq (:attributes s)) (str " " (pr-str (:attributes s))))
         (when (seq (:events s))
           (str " events=" (pr-str (mapv :name (:events s))))))))

(defrecord StdoutExporter [writer state]
  export/SpanExporter
  (export-spans! [_ spans]
    (if (:shutdown? @state)
      false
      (do (doseq [s spans] (writer (format-span s)))
          true)))
  (flush-exporter! [_] (flush) true)
  (shutdown-exporter! [_] (swap! state assoc :shutdown? true) true))

(defn exporter
  "Print one line per span. `:writer` overrides where the line goes (default
  `println`)."
  ([] (exporter {}))
  ([{:keys [writer]}]
   (->StdoutExporter (or writer println) (atom {:shutdown? false}))))

(defrecord JsonExporter [writer state]
  export/SpanExporter
  (export-spans! [_ spans]
    (if (:shutdown? @state)
      false
      (do (writer (json/write-str (enc/traces-request spans)))
          true)))
  (flush-exporter! [_] (flush) true)
  (shutdown-exporter! [_] (swap! state assoc :shutdown? true) true))

(defn json-exporter
  "Print each batch as the OTLP/JSON request that would have been posted."
  ([] (json-exporter {}))
  ([{:keys [writer]}]
   (->JsonExporter (or writer println) (atom {:shutdown? false}))))

;; --- metrics ----------------------------------------------------------------

(defn- format-metric [m]
  (str (:name m)
       (when (:unit m) (str " (" (:unit m) ")"))
       " " (name (:type m))
       " " (pr-str (mapv (fn [p]
                           (cond-> {:attrs (:attributes p)}
                             (contains? p :value) (assoc :value (:value p))
                             (contains? p :count) (assoc :count (:count p) :sum (:sum p))))
                         (:data-points m)))))

(defrecord StdoutMetricExporter [writer state]
  export/MetricExporter
  (export-metrics! [_ _ collected]
    (if (:shutdown? @state)
      false
      (do (doseq [{:keys [metrics]} collected
                  m metrics]
            (writer (format-metric m)))
          true)))
  (shutdown-metric-exporter! [_] (swap! state assoc :shutdown? true) true))

(defn metric-exporter
  "Print one line per metric on every collection."
  ([] (metric-exporter {}))
  ([{:keys [writer]}]
   (->StdoutMetricExporter (or writer println) (atom {:shutdown? false}))))
