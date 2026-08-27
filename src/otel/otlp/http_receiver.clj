(ns otel.otlp.http-receiver
  "A small, host-agnostic Ring adapter for OTLP/HTTP JSON traces.

  This namespace owns HTTP policy, not sockets, JSON parsing, or storage. The
  caller supplies `:parse-body` and a span exporter. See `handler` for the exact
  callback contracts."
  (:require [clojure.string :as str]
            [otel.otlp.json :as json]
            [otel.otlp.trace-decode :as decode]
            [otel.sdk.export :as export]))

(def traces-path "/v1/traces")
(def suppress-telemetry-key ::suppress-telemetry?)

(defn receiver-request?
  "True when a Ring request targets this receiver's trace endpoint."
  [request]
  (= traces-path (:uri request)))

(defn suppress-telemetry
  "Mark a Ring request so instrumentation can omit receiver-internal telemetry."
  [request]
  (assoc request suppress-telemetry-key true))

(defn telemetry-suppressed?
  "Predicate for instrumentation middleware and exporter filters."
  [request]
  (true? (get request suppress-telemetry-key)))

(defn wrap-suppress-receiver-telemetry
  "Mark `/v1/traces` before calling an already instrumented handler.

  Put this wrapper *outside* request instrumentation:

      (wrap-suppress-receiver-telemetry (instrument receiver))

  Instrumentation should call `telemetry-suppressed?` before starting a span."
  [ring-handler]
  (fn [request]
    (ring-handler (if (receiver-request? request)
                    (suppress-telemetry request)
                    request))))

(defn body-too-large
  "Exception a streaming body parser may throw as soon as `limit` is exceeded."
  ([limit]
   (body-too-large limit nil))
  ([limit actual]
   (ex-info "OTLP request body exceeds the configured encoded-byte limit"
            (cond-> {:type ::body-too-large :limit limit}
              (some? actual) (assoc :actual actual)))))

(defn export-timeout
  "Exception a `:call-with-timeout` implementation returns by throwing."
  [timeout-ms]
  (ex-info "OTLP span export timed out"
           {:type ::export-timeout :timeout-ms timeout-ms}))

(defn- header [request header-name]
  (let [target (str/lower-case header-name)]
    (some (fn [[k v]]
            (when (= target (str/lower-case (if (keyword? k)
                                              (clojure.core/name k)
                                              (str k))))
              v))
          (:headers request))))

(defn- media-type [value]
  (some-> value str (str/split #";" 2) first str/trim str/lower-case))

(defn- content-length [request]
  (when-let [v (header request "content-length")]
    (try
      (let [n (parse-long (str/trim (str v)))]
        (when (and n (not (neg? n))) n))
      (catch :default _ nil))))

(defn- response
  ([status body] (response status body {}))
  ([status body headers]
   {:status status
    :headers (merge {"content-type" "application/json"} headers)
    :body (json/write-str body)}))

(defn- failure [status type message]
  (let [rpc-code (case status
                   400 3
                   404 5
                   405 3
                   413 8
                   415 3
                   429 8
                   504 4
                   503 14
                   13)]
    (assoc (response status {:code rpc-code :message message})
           ::failure {:type type :message message})))

(defn- error-message [errors]
  (->> errors
       (take 4)
       (map (fn [{:keys [path reason]}]
              (str (if (seq path) (pr-str path) "request") ": " (name reason))))
       (str/join "; ")))

(defn- success-response [{:keys [rejected-spans errors]}]
  (if (or (pos? rejected-spans) (seq errors))
    (response 200 {:partialSuccess
                   {:rejectedSpans (str rejected-spans)
                    :errorMessage (error-message errors)}})
    (response 200 {})))

(defn- valid-parse-result? [x]
  (and (map? x)
       (contains? x :value)
       (integer? (:encoded-bytes x))
       (not (neg? (:encoded-bytes x)))))

(defn- acquire! [active limit]
  (loop []
    (let [n @active]
      (cond
        (>= n limit) false
        (compare-and-set! active n (inc n)) true
        :else (recur)))))

(defn handler
  "Build a pure Ring handler for `POST /v1/traces`.

  Required options:

  * `:parse-body` — `(fn [marked-request max-body-bytes]
                       {:value parsed-json :encoded-bytes actual-byte-count})`.
    It must count the encoded bytes it consumes (not decoded characters), stop
    at the supplied limit, and may throw `(body-too-large limit actual)`.
  * `:exporter` — an `otel.sdk.export/SpanExporter`.

  Options are `:max-body-bytes` (4 MiB), `:max-concurrency` (8),
  `:export-timeout-ms`, `:call-with-timeout`, and `:export-spans!`.
  When a timeout is configured, `:call-with-timeout` is required and is called
  as `(f timeout-ms thunk)`; it must either return the thunk result or throw
  `(export-timeout timeout-ms)`. The injectable seam lets a host use its own
  bounded executor without this library owning threads. A timeout wrapper must
  not return or throw until the timed-out work has stopped; otherwise the
  receiver cannot truthfully release its concurrency slot.

  Content-Length is only an early rejection. The parser's measured byte count
  is authoritative. Compressed requests are rejected. At most
  `:max-concurrency` requests enter parsing/export at once."
  [{:keys [parse-body exporter max-body-bytes max-concurrency
           export-timeout-ms call-with-timeout export-spans!]
    :or {max-body-bytes (* 4 1024 1024)
         max-concurrency 8
         export-spans! export/export-spans!}}]
  (when-not (fn? parse-body)
    (throw (ex-info ":parse-body must be a function"
                    {:type ::invalid-config :option :parse-body})))
  (when-not exporter
    (throw (ex-info ":exporter is required"
                    {:type ::invalid-config :option :exporter})))
  (when-not (and (integer? max-body-bytes) (pos? max-body-bytes))
    (throw (ex-info ":max-body-bytes must be positive"
                    {:type ::invalid-config :option :max-body-bytes})))
  (when-not (and (integer? max-concurrency) (pos? max-concurrency))
    (throw (ex-info ":max-concurrency must be positive"
                    {:type ::invalid-config :option :max-concurrency})))
  (when (and export-timeout-ms
             (not (and (integer? export-timeout-ms) (pos? export-timeout-ms))))
    (throw (ex-info ":export-timeout-ms must be positive"
                    {:type ::invalid-config :option :export-timeout-ms})))
  (when (and export-timeout-ms (not (fn? call-with-timeout)))
    (throw (ex-info ":call-with-timeout is required with :export-timeout-ms"
                    {:type ::invalid-config :option :call-with-timeout})))
  (when-not (fn? export-spans!)
    (throw (ex-info ":export-spans! must be a function"
                    {:type ::invalid-config :option :export-spans!})))
  (let [active (atom 0)
        invoke-export (fn [spans]
                        (if (empty? spans)
                          true
                          (let [thunk #(export-spans! exporter spans)]
                            (if export-timeout-ms
                              (call-with-timeout export-timeout-ms thunk)
                              (thunk)))))]
    (fn [request]
      (cond
        (not (receiver-request? request))
        (failure 404 ::not-found "OTLP trace endpoint not found")

        (not= :post (:request-method request))
        (assoc-in (failure 405 ::method-not-allowed "OTLP traces require POST")
                  [:headers "allow"] "POST")

        (not= "application/json" (media-type (header request "content-type")))
        (failure 415 ::unsupported-media-type
                 "OTLP traces require Content-Type application/json")

        (let [encoding (some-> (header request "content-encoding")
                               str str/trim str/lower-case)]
          (and encoding (not= "identity" encoding)))
        (failure 415 ::unsupported-content-encoding
                 "compressed OTLP requests are not supported")

        (let [n (content-length request)]
          (and n (> n max-body-bytes)))
        (failure 413 ::body-too-large "OTLP request body is too large")

        (not (acquire! active max-concurrency))
        (failure 429 ::too-many-requests "OTLP receiver concurrency limit reached")

        :else
        (try
          (let [parsed (parse-body (suppress-telemetry request) max-body-bytes)]
            (cond
              (not (valid-parse-result? parsed))
              (failure 500 ::invalid-parser-result
                       "OTLP body parser returned an invalid result")

              (> (:encoded-bytes parsed) max-body-bytes)
              (failure 413 ::body-too-large "OTLP request body is too large")

              :else
              (let [decoded (decode/decode-request (:value parsed))]
                (if (and (seq (:errors decoded))
                         (empty? (:spans decoded))
                         (zero? (:rejected-spans decoded)))
                  (failure 400 ::invalid-otlp-request
                           (str "invalid OTLP trace request: "
                                (error-message (:errors decoded))))
                  (try
                    (if (invoke-export (:spans decoded))
                      (success-response decoded)
                      (failure 503 ::export-failed "OTLP span export failed"))
                    (catch :default e
                      (if (= ::export-timeout (:type (ex-data e)))
                        (failure 504 ::export-timeout "OTLP span export timed out")
                        (failure 503 ::export-failed "OTLP span export failed"))))))))
          (catch :default e
            (if (= ::body-too-large (:type (ex-data e)))
              (failure 413 ::body-too-large "OTLP request body is too large")
              (failure 400 ::invalid-body "OTLP request body is invalid")))
          (finally
            (swap! active dec)))))))
