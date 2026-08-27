(ns otel.otlp.http-receiver
  "Bounded host-neutral Ring adapter for OTLP/HTTP JSON signals."
  (:require [clojure.string :as str]
            [otel.otlp.json :as json]
            [otel.otlp.signal-decode :as signal-decode]
            [otel.otlp.trace-decode :as trace-decode]
            [otel.sdk.export :as export]
            [otel.sdk.logs :as logs]))

(def traces-path "/v1/traces")
(def logs-path "/v1/logs")
(def metrics-path "/v1/metrics")
(def receiver-paths #{traces-path logs-path metrics-path})
(def suppress-telemetry-key ::suppress-telemetry?)
(defn receiver-request? [request] (contains? receiver-paths (:uri request)))
(defn suppress-telemetry [request] (assoc request suppress-telemetry-key true))
(defn telemetry-suppressed? [request] (true? (get request suppress-telemetry-key)))
(defn wrap-suppress-receiver-telemetry [ring-handler]
  (fn [request]
    (ring-handler (if (receiver-request? request) (suppress-telemetry request) request))))

(defn body-too-large
  ([limit] (body-too-large limit nil))
  ([limit actual]
   (ex-info "OTLP request body exceeds the configured encoded-byte limit"
            (cond-> {:type ::body-too-large :limit limit}
              (some? actual) (assoc :actual actual)))))
(defn export-timeout [timeout-ms]
  ;; Preserve the original public exception message; routing supplies the
  ;; signal-accurate HTTP response message.
  (ex-info "OTLP span export timed out" {:type ::export-timeout :timeout-ms timeout-ms}))

(defn- header [request header-name]
  (let [target (str/lower-case header-name)]
    (some (fn [[k v]]
            (when (= target (str/lower-case (if (keyword? k) (name k) (str k)))) v))
          (:headers request))))
(defn- media-type [value]
  (some-> value str (str/split #";" 2) first str/trim str/lower-case))
(defn- content-length [request]
  (when-let [v (header request "content-length")]
    (try (let [n (parse-long (str/trim (str v)))]
           (when (and n (not (neg? n))) n))
         (catch :default _ nil))))
(defn- response
  ([status body] (response status body {}))
  ([status body headers]
   {:status status :headers (merge {"content-type" "application/json"} headers)
    :body (json/write-str body)}))
(defn- failure [status type message]
  (let [rpc-code (case status 400 3, 404 5, 405 3, 413 8, 415 3,
                   429 8, 504 4, 503 14, 13)]
    (assoc (response status {:code rpc-code :message message})
           ::failure {:type type :message message})))
(defn- error-message [errors]
  (->> errors (take 4)
       (map (fn [{:keys [path reason]}]
              (str (if (seq path) (pr-str path) "request") ": " (name reason))))
       (str/join "; ")))
(defn- success-response [decoded rejected-key partial-field]
  (let [rejected (get decoded rejected-key 0) errors (:errors decoded)]
    (if (or (pos? rejected) (seq errors))
      (response 200 {:partialSuccess
                     {partial-field (str rejected) :errorMessage (error-message errors)}})
      (response 200 {}))))
(defn- valid-parse-result? [x]
  (and (map? x) (contains? x :value) (integer? (:encoded-bytes x))
       (not (neg? (:encoded-bytes x)))))
(defn- acquire! [active limit]
  (loop []
    (let [n @active]
      (cond (>= n limit) false
            (compare-and-set! active n (inc n)) true
            :else (recur)))))

(defn- nonempty-items? [signal decoded]
  (case (:id signal)
    :traces (seq (:spans decoded))
    :logs (seq (:records decoded))
    :metrics (seq (mapcat (fn [{:keys [collected]}] (mapcat :metrics collected))
                          (:collections decoded)))))
(defn- invoke-export [signal decoded]
  (case (:id signal)
    :traces ((:export-fn signal) (:exporter signal) (:spans decoded))
    :logs ((:export-fn signal) (:exporter signal) (:records decoded))
    :metrics
    (reduce (fn [ok {:keys [resource collected]}]
              (let [has-metrics? (seq (mapcat :metrics collected))]
                (and (if has-metrics?
                       (boolean ((:export-fn signal) (:exporter signal) resource collected))
                       true)
                     ok)))
            true (:collections decoded))))

(defn handler
  "Build one Ring handler for POST /v1/traces, /v1/logs, and /v1/metrics.

  `:exporter` remains the trace exporter and is reused for other protocols it
  satisfies. Signal overrides are `:trace-exporter`, `:log-exporter`, and
  `:metric-exporter`; injectable calls are the matching `:export-...!` options.
  Every signal shares one body limit, admission counter, and timeout policy.
  `:parse-body` retains its original `(request limit) -> {:value ...
  :encoded-bytes n}` contract. A configured `:call-with-timeout` must not return
  until timed-out work has stopped, so releasing the shared slot stays truthful."
  [{:keys [parse-body exporter trace-exporter log-exporter metric-exporter
           max-body-bytes max-concurrency export-timeout-ms call-with-timeout
           export-spans! export-logs! export-metrics!]
    :or {max-body-bytes (* 4 1024 1024) max-concurrency 8
         export-spans! export/export-spans! export-logs! logs/export-logs!
         export-metrics! export/export-metrics!}}]
  (when-not (fn? parse-body)
    (throw (ex-info ":parse-body must be a function" {:type ::invalid-config :option :parse-body})))
  (let [trace-exporter (or trace-exporter exporter)
        log-exporter (or log-exporter
                         (when (and exporter (satisfies? logs/LogRecordExporter exporter)) exporter))
        metric-exporter (or metric-exporter
                            (when (and exporter (satisfies? export/MetricExporter exporter)) exporter))]
    (when-not (or trace-exporter log-exporter metric-exporter)
      (throw (ex-info ":exporter is required"
                      {:type ::invalid-config :option :exporter})))
    (when-not (and (integer? max-body-bytes) (pos? max-body-bytes))
      (throw (ex-info ":max-body-bytes must be positive" {:type ::invalid-config :option :max-body-bytes})))
    (when-not (and (integer? max-concurrency) (pos? max-concurrency))
      (throw (ex-info ":max-concurrency must be positive" {:type ::invalid-config :option :max-concurrency})))
    (when (and export-timeout-ms
               (not (and (integer? export-timeout-ms) (pos? export-timeout-ms))))
      (throw (ex-info ":export-timeout-ms must be positive" {:type ::invalid-config :option :export-timeout-ms})))
    (when (and export-timeout-ms (not (fn? call-with-timeout)))
      (throw (ex-info ":call-with-timeout is required with :export-timeout-ms"
                      {:type ::invalid-config :option :call-with-timeout})))
    (doseq [[option f] [[:export-spans! export-spans!] [:export-logs! export-logs!]
                        [:export-metrics! export-metrics!]]]
      (when-not (fn? f)
        (throw (ex-info (str option " must be a function")
                        {:type ::invalid-config :option option}))))
    (let [signals {traces-path {:id :traces :noun "trace" :export-noun "span"
                                :plural "traces"
                                :decoder trace-decode/decode-request
                                :rejected-key :rejected-spans :partial-field :rejectedSpans
                                :exporter trace-exporter :export-fn export-spans!}
                   logs-path {:id :logs :noun "log" :export-noun "log" :plural "logs"
                              :decoder signal-decode/decode-logs
                              :rejected-key :rejected-log-records
                              :partial-field :rejectedLogRecords
                              :exporter log-exporter :export-fn export-logs!}
                   metrics-path {:id :metrics :noun "metric" :export-noun "metric"
                                 :plural "metrics"
                                 :decoder signal-decode/decode-metrics
                                 :rejected-key :rejected-data-points
                                 :partial-field :rejectedDataPoints
                                 :exporter metric-exporter :export-fn export-metrics!}}
          active (atom 0)]
      (fn [request]
        (let [signal (get signals (:uri request))]
          (cond
            (nil? signal) (failure 404 ::not-found "OTLP signal endpoint not found")
            (not= :post (:request-method request))
            (assoc-in (failure 405 ::method-not-allowed
                               (str "OTLP " (:plural signal) " require POST"))
                      [:headers "allow"] "POST")
            (not= "application/json" (media-type (header request "content-type")))
            (failure 415 ::unsupported-media-type
                     (str "OTLP " (:plural signal) " require Content-Type application/json"))
            (let [encoding (some-> (header request "content-encoding") str str/trim str/lower-case)]
              (and encoding (not= "identity" encoding)))
            (failure 415 ::unsupported-content-encoding "compressed OTLP requests are not supported")
            (let [n (content-length request)] (and n (> n max-body-bytes)))
            (failure 413 ::body-too-large "OTLP request body is too large")
            (nil? (:exporter signal))
            (failure 503 ::exporter-unavailable
                     (str "OTLP " (:noun signal) " exporter is unavailable"))
            (not (acquire! active max-concurrency))
            (failure 429 ::too-many-requests "OTLP receiver concurrency limit reached")
            :else
            (try
              (let [parsed (parse-body (suppress-telemetry request) max-body-bytes)]
                (if-not (valid-parse-result? parsed)
                  (failure 500 ::invalid-parser-result
                           "OTLP body parser returned an invalid result")
                  (if (> (:encoded-bytes parsed) max-body-bytes)
                    (failure 413 ::body-too-large "OTLP request body is too large")
                    (let [decoded ((:decoder signal) (:value parsed))
                          rejected (get decoded (:rejected-key signal) 0)]
                      (if (and (seq (:errors decoded))
                               (not (nonempty-items? signal decoded))
                               (zero? rejected))
                        (failure 400 ::invalid-otlp-request
                                 (str "invalid OTLP " (:noun signal) " request: "
                                      (error-message (:errors decoded))))
                        (try
                          (let [thunk #(if (nonempty-items? signal decoded)
                                         (invoke-export signal decoded) true)
                                ok (if export-timeout-ms
                                     (call-with-timeout export-timeout-ms thunk)
                                     (thunk))]
                            (if ok
                              (success-response decoded (:rejected-key signal)
                                                (:partial-field signal))
                              (failure 503 ::export-failed
                                       (str "OTLP " (:export-noun signal) " export failed"))))
                          (catch :default export-error
                            (if (= ::export-timeout (:type (ex-data export-error)))
                              (failure 504 ::export-timeout
                                       (str "OTLP " (:export-noun signal) " export timed out"))
                              (failure 503 ::export-failed
                                       (str "OTLP " (:export-noun signal) " export failed"))))))))))
              (catch :default e
                (if (= ::body-too-large (:type (ex-data e)))
                  (failure 413 ::body-too-large "OTLP request body is too large")
                  (failure 400 ::invalid-body "OTLP request body is invalid")))
              (finally (swap! active dec)))))))))
