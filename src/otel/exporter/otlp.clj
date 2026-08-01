(ns otel.exporter.otlp
  "The OTLP/HTTP span exporter, using the spec's JSON Protobuf encoding.

  Configuration follows the standard OTLP environment variables, so a jolt
  service is configured the same way as any other OpenTelemetry service:

    OTEL_EXPORTER_OTLP_ENDPOINT          base endpoint (default http://localhost:4318)
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT   full traces URL, overriding the base
    OTEL_EXPORTER_OTLP_HEADERS           comma-separated key=value request headers
    OTEL_EXPORTER_OTLP_TIMEOUT           per-request timeout in milliseconds

  Both http and https endpoints work; TLS comes from jolt-lang/http-client over
  the system OpenSSL.

  Retries follow the OTLP spec: only the response codes the spec calls retryable
  are retried, with exponential backoff, and a `Retry-After` header is honoured.
  Everything else — a rejected payload, an unreachable collector, a timeout — is
  reported as a failed export and dropped. An exporter that retried forever would
  turn a collector outage into unbounded memory growth in the application."
  (:require [clojure.string :as str]
            [otel.otlp.encode :as enc]
            [otel.otlp.http :as http]
            [otel.otlp.json :as json]
            [otel.sdk.export :as export]
            [otel.sdk.logs :as sdk-logs]))

(def default-endpoint "http://localhost:4318")
(def traces-path "/v1/traces")

(def retryable-status
  "Response codes the OTLP spec designates as retryable."
  #{429 502 503 504})

(defn parse-headers
  "Parse an OTEL_EXPORTER_OTLP_HEADERS value: comma-separated key=value pairs."
  [s]
  (if (str/blank? s)
    {}
    (reduce (fn [acc entry]
              (let [i (str/index-of entry "=")]
                (if (and i (pos? i))
                  (assoc acc (str/trim (subs entry 0 i)) (str/trim (subs entry (inc i))))
                  acc)))
            {}
            (str/split s #","))))

(defn- env [k] (jolt.host/getenv k))

(defn traces-endpoint
  "Resolve the traces URL. A signal-specific endpoint is used verbatim; a base
  endpoint gets /v1/traces appended, which is what the spec requires and a
  frequent source of confusion when configuring collectors."
  [{:keys [endpoint traces-url]}]
  (or traces-url
      (env "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
      (let [base (or endpoint (env "OTEL_EXPORTER_OTLP_ENDPOINT") default-endpoint)]
        (str (str/replace base #"/+$" "") traces-path))))

(defn- backoff-ms
  "Exponential backoff with the collector's Retry-After taking precedence."
  [attempt retry-after]
  (or (when retry-after
        (try (* 1000 (Long/parseLong (str/trim retry-after))) (catch :default _ nil)))
      (min 30000 (* 1000 (long (Math/pow 2 attempt))))))

(defn- attempt-post
  [url payload headers timeout-ms insecure?]
  (try
    (http/post url payload {:headers (merge {"Content-Type" "application/json"} headers)
                            :timeout-ms timeout-ms
                            :insecure? insecure?})
    (catch :default e
      ;; A connection failure, a DNS miss, a TLS handshake rejection. Reported as
      ;; a failed export, never raised into the application.
      {:status nil :error e})))

(defn- report!
  "Report a failed export. Telemetry failures go to stderr and nowhere else —
  they must never be raised into the application, and logging them through the
  application's own logger risks a cycle when that logger is itself instrumented."
  [msg]
  (binding [*out* *err*]
    (println "otel: " msg)))

(defrecord OtlpHttpExporter [url headers timeout-ms max-retries insecure? state]
  export/SpanExporter
  (export-spans! [_ spans]
    (if (or (:shutdown? @state) (empty? spans))
      (not (:shutdown? @state))
      (let [payload (json/write-str (enc/traces-request spans))]
        (loop [attempt 0]
          (let [{:keys [status body error retry-after]} (attempt-post url payload headers timeout-ms insecure?)]
            (cond
              (and status (<= 200 status 299))
              (do
                ;; A 2xx may still be a PARTIAL success: the collector accepted the
                ;; request but rejected some records, and only the body says so.
                (when-let [rejected (json/find-number body "rejectedSpans")]
                  (when (pos? rejected)
                    (report! (str "collector rejected " rejected " of " (count spans) " spans"))))
                true)

              (and (contains? retryable-status status) (< attempt max-retries))
              (do (Thread/sleep (backoff-ms attempt retry-after))
                  (recur (inc attempt)))

              :else
              (do (report! (str "export of " (count spans) " spans failed: "
                                (cond error (or (ex-message error) (str error))
                                      status (str "HTTP " status)
                                      :else "no response")))
                  false)))))))
  (flush-exporter! [_] true)
  (shutdown-exporter! [_] (swap! state assoc :shutdown? true) true))

(defn exporter
  "An OTLP/HTTP span exporter.

  Options (all optional; each falls back to its OTEL_* environment variable):
    :endpoint     base URL, e.g. \"http://collector:4318\"
    :traces-url   full traces URL, overriding :endpoint
    :headers      map of extra request headers
    :timeout-ms   per-request timeout (default 10000)
    :max-retries  retryable-failure attempts after the first (default 3)
    :insecure?    skip TLS certificate verification (self-signed collector certs)"
  ([] (exporter {}))
  ([{:keys [headers timeout-ms max-retries insecure?] :as opts}]
   (let [url (traces-endpoint opts)]
     (when-not (http/supports-scheme? url)
       (throw (ex-info (str "otel: the OTLP endpoint must be http:// or https://, got " url)
                       {:url url})))
     (->OtlpHttpExporter url
                         (merge (parse-headers (env "OTEL_EXPORTER_OTLP_HEADERS")) headers)
                         (or timeout-ms
                             (some-> (env "OTEL_EXPORTER_OTLP_TIMEOUT") str/trim Long/parseLong)
                             10000)
                         (or max-retries 3)
                         (boolean insecure?)
                         (atom {:shutdown? false})))))

;; --- metrics ----------------------------------------------------------------

(def metrics-path "/v1/metrics")

(defn metrics-endpoint
  "Resolve the metrics URL, on the same base/signal-specific rules as traces."
  [{:keys [endpoint metrics-url]}]
  (or metrics-url
      (env "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
      (let [base (or endpoint (env "OTEL_EXPORTER_OTLP_ENDPOINT") default-endpoint)]
        (str (str/replace base #"/+$" "") metrics-path))))

(defrecord OtlpHttpMetricExporter [url headers timeout-ms max-retries insecure? state]
  export/MetricExporter
  (export-metrics! [_ resource collected]
    (if (:shutdown? @state)
      false
      (let [payload (json/write-str (enc/metrics-request resource collected))]
        (loop [attempt 0]
          (let [{:keys [status body error retry-after]} (attempt-post url payload headers timeout-ms insecure?)]
            (cond
              (and status (<= 200 status 299))
              (do (when-let [rejected (json/find-number body "rejectedDataPoints")]
                    (when (pos? rejected)
                      (report! (str "collector rejected " rejected " data points"))))
                  true)

              (and (contains? retryable-status status) (< attempt max-retries))
              (do (Thread/sleep (backoff-ms attempt retry-after))
                  (recur (inc attempt)))

              :else
              (do (report! (str "metric export failed: "
                                (cond error (or (ex-message error) (str error))
                                      status (str "HTTP " status)
                                      :else "no response")))
                  false)))))))
  (shutdown-metric-exporter! [_] (swap! state assoc :shutdown? true) true))

(defn metric-exporter
  "An OTLP/HTTP metric exporter. Same options as `exporter`, with :metrics-url in
  place of :traces-url."
  ([] (metric-exporter {}))
  ([{:keys [headers timeout-ms max-retries insecure?] :as opts}]
   (let [url (metrics-endpoint opts)]
     (when-not (http/supports-scheme? url)
       (throw (ex-info (str "otel: the OTLP endpoint must be http:// or https://, got " url)
                       {:url url})))
     (->OtlpHttpMetricExporter url
                               (merge (parse-headers (env "OTEL_EXPORTER_OTLP_HEADERS")) headers)
                               (or timeout-ms
                                   (some-> (env "OTEL_EXPORTER_OTLP_TIMEOUT") str/trim Long/parseLong)
                                   10000)
                               (or max-retries 3)
                               (boolean insecure?)
                               (atom {:shutdown? false})))))

;; --- logs -------------------------------------------------------------------

(def logs-path "/v1/logs")

(defn logs-endpoint
  "Resolve the logs URL, on the same base/signal-specific rules as traces."
  [{:keys [endpoint logs-url]}]
  (or logs-url
      (env "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT")
      (let [base (or endpoint (env "OTEL_EXPORTER_OTLP_ENDPOINT") default-endpoint)]
        (str (str/replace base #"/+$" "") logs-path))))

(defrecord OtlpHttpLogExporter [url headers timeout-ms max-retries insecure? state]
  sdk-logs/LogRecordExporter
  (export-logs! [_ records]
    (if (or (:shutdown? @state) (empty? records))
      (not (:shutdown? @state))
      (let [payload (json/write-str (enc/logs-request records))]
        (loop [attempt 0]
          (let [{:keys [status body error retry-after]} (attempt-post url payload headers timeout-ms insecure?)]
            (cond
              (and status (<= 200 status 299))
              (do (when-let [rejected (json/find-number body "rejectedLogRecords")]
                    (when (pos? rejected)
                      (report! (str "collector rejected " rejected " log records"))))
                  true)

              (and (contains? retryable-status status) (< attempt max-retries))
              (do (Thread/sleep (backoff-ms attempt retry-after))
                  (recur (inc attempt)))

              :else
              (do (report! (str "log export failed: "
                                (cond error (or (ex-message error) (str error))
                                      status (str "HTTP " status)
                                      :else "no response")))
                  false)))))))
  (shutdown-log-exporter! [_] (swap! state assoc :shutdown? true) true))

(defn log-exporter
  "An OTLP/HTTP log record exporter. Same options as `exporter`, with :logs-url
  in place of :traces-url."
  ([] (log-exporter {}))
  ([{:keys [headers timeout-ms max-retries insecure?] :as opts}]
   (let [url (logs-endpoint opts)]
     (when-not (http/supports-scheme? url)
       (throw (ex-info (str "otel: the OTLP endpoint must be http:// or https://, got " url)
                       {:url url})))
     (->OtlpHttpLogExporter url
                            (merge (parse-headers (env "OTEL_EXPORTER_OTLP_HEADERS")) headers)
                            (or timeout-ms
                                (some-> (env "OTEL_EXPORTER_OTLP_TIMEOUT") str/trim Long/parseLong)
                                10000)
                            (or max-retries 3)
                            (boolean insecure?)
                            (atom {:shutdown? false})))))
