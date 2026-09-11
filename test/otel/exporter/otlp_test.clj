(ns otel.exporter.otlp-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as str]
            [otel.exporter.otlp :as otlp]
            [otel.exporter.stdout :as stdout]
            [otel.otlp.http :as http]
            [otel.resource :as res]
            [otel.sdk.export :as export]
            [otel.sdk.tracer :as sdk]
            [otel.trace :as trace]))

;; --- url handling -----------------------------------------------------------

(deftest parses-urls
  (is (= {:scheme "http" :host "localhost" :port 4318 :path "/v1/traces"}
         (http/parse-url "http://localhost:4318/v1/traces")))
  (testing "the default port comes from the scheme"
    (is (= 80 (:port (http/parse-url "http://example.com/x"))))
    (is (= 443 (:port (http/parse-url "https://example.com/x")))))
  (testing "a missing path is /"
    (is (= "/" (:path (http/parse-url "http://example.com")))))
  (testing "an IPv6 literal's colons are not a port"
    (is (= "[::1]" (:host (http/parse-url "http://[::1]/v1/traces"))))
    (is (= 4318 (:port (http/parse-url "http://[::1]:4318/v1/traces"))))))

(deftest rejects-non-http-urls
  (is (thrown? Exception (http/parse-url "ftp://x/y")))
  (is (thrown? Exception (http/parse-url "not a url"))))

(deftest endpoint-resolution
  (testing "a base endpoint gets the signal path appended"
    (is (= "http://collector:4318/v1/traces"
           (otlp/traces-endpoint {:endpoint "http://collector:4318"}))))
  (testing "a trailing slash does not produce a doubled path separator"
    (is (= "http://collector:4318/v1/traces"
           (otlp/traces-endpoint {:endpoint "http://collector:4318/"}))))
  (testing "an explicit traces url is used verbatim"
    (is (= "http://c/custom"
           (otlp/traces-endpoint {:endpoint "http://collector:4318" :traces-url "http://c/custom"})))))

(deftest environment-configuration-can-be-closed
  (let [ambient {"OTEL_EXPORTER_OTLP_ENDPOINT" "http://ambient-base:4318"
                 "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT" "http://ambient-traces/custom"
                 "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT" "http://ambient-metrics/custom"
                 "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT" "http://ambient-logs/custom"
                 "OTEL_EXPORTER_OTLP_HEADERS" "ambient=yes,shared=ambient"
                 "OTEL_EXPORTER_OTLP_TIMEOUT" "2468"}
        reads (atom [])]
    (with-redefs [jolt.host/getenv (fn [name]
                                     (swap! reads conj name)
                                     (get ambient name))]
      (testing "the backwards-compatible default honors signal URLs, headers, and timeout"
        (let [span (otlp/exporter {:endpoint "http://explicit-base:4318"
                                   :headers {"explicit" "yes" "shared" "explicit"}})
              metric (otlp/metric-exporter {})
              log (otlp/log-exporter {})]
          (is (= "http://ambient-traces/custom" (:url span)))
          (is (= "http://ambient-metrics/custom" (:url metric)))
          (is (= "http://ambient-logs/custom" (:url log)))
          (is (= {"ambient" "yes" "explicit" "yes" "shared" "explicit"}
                 (:headers span)))
          (is (= 2468 (:timeout-ms span)))
          (is (= 2468 (:timeout-ms metric)))
          (is (= 2468 (:timeout-ms log)))
          (is (= "http://ambient-traces/custom"
                 (otlp/traces-endpoint {:environment? true})))
          (is (seq @reads))))
      (reset! reads [])
      (testing "false ignores base and signal URLs and does not supplement explicit settings"
        (let [common {:environment? false
                      :endpoint "http://explicit-base:4318"
                      :headers {"explicit" "yes"}
                      :timeout-ms 1357}
              span (otlp/exporter common)
              metric (otlp/metric-exporter common)
              log (otlp/log-exporter common)]
          (is (= "http://explicit-base:4318/v1/traces" (:url span)))
          (is (= "http://explicit-base:4318/v1/metrics" (:url metric)))
          (is (= "http://explicit-base:4318/v1/logs" (:url log)))
          (is (= {"explicit" "yes"} (:headers span) (:headers metric) (:headers log)))
          (is (= 1357 (:timeout-ms span) (:timeout-ms metric) (:timeout-ms log)))))
      (testing "false also closes resolvers and uses library defaults when explicit values are absent"
        (is (= "http://localhost:4318/v1/traces"
               (otlp/traces-endpoint {:environment? false})))
        (is (= "http://localhost:4318/v1/metrics"
               (otlp/metrics-endpoint {:environment? false})))
        (is (= "http://localhost:4318/v1/logs"
               (otlp/logs-endpoint {:environment? false})))
        (let [span (otlp/exporter {:environment? false})]
          (is (= {} (:headers span)))
          (is (= 10000 (:timeout-ms span)))))
      (is (empty? @reads)
          "closed configuration must not read ambient OTLP variables"))))

(deftest ambient-base-endpoint-remains-the-default
  (with-redefs [jolt.host/getenv #(get {"OTEL_EXPORTER_OTLP_ENDPOINT"
                                        "http://ambient-base:4318"} %)]
    (is (= "http://ambient-base:4318/v1/traces" (:url (otlp/exporter {}))))
    (is (= "http://ambient-base:4318/v1/metrics" (:url (otlp/metric-exporter {}))))
    (is (= "http://ambient-base:4318/v1/logs" (:url (otlp/log-exporter {}))))))

(deftest environment-option-must-be-boolean
  (doseq [construct [(fn [] (otlp/traces-endpoint {:environment? nil}))
                     (fn [] (otlp/metrics-endpoint {:environment? :no}))
                     (fn [] (otlp/logs-endpoint {:environment? 0}))
                     (fn [] (otlp/exporter {:environment? "false"}))
                     (fn [] (otlp/metric-exporter {:environment? []}))
                     (fn [] (otlp/log-exporter {:environment? {}}))]]
    (let [error (try (construct) nil (catch Exception error error))]
      (is (= :invalid-option (:otel.exporter.otlp/error (ex-data error))))
      (is (= :environment? (:option (ex-data error)))))))

(deftest parses-header-config
  (is (= {"api-key" "secret"} (otlp/parse-headers "api-key=secret")))
  (is (= {"a" "1" "b" "2"} (otlp/parse-headers "a=1,b=2")))
  (is (= {} (otlp/parse-headers "")))
  (is (= {} (otlp/parse-headers nil)))
  (testing "a malformed entry is skipped, not fatal"
    (is (= {"b" "2"} (otlp/parse-headers "junk,b=2")))))

(deftest https-endpoints-are-supported
  (testing "TLS comes from http-client, so an https collector is a normal endpoint"
    (is (http/supports-scheme? "https://collector.example.com"))
    (is (http/supports-scheme? "http://localhost:4318"))
    (let [e (otlp/exporter {:endpoint "https://collector.example.com"})]
      (is (= "https://collector.example.com/v1/traces" (:url e))))))

(deftest a-non-http-scheme-is-still-rejected
  (testing "gRPC is not implemented, so an otlp-grpc endpoint must fail loudly at
            construction rather than being silently posted to as if it were HTTP"
    (is (thrown? Exception (otlp/exporter {:traces-url "grpc://collector:4317"})))))

(deftest insecure-is-off-by-default
  (is (false? (:insecure? (otlp/exporter {:endpoint "https://c.example.com"}))))
  (is (true? (:insecure? (otlp/exporter {:endpoint "https://c.example.com" :insecure? true})))))

;; --- stdout exporters -------------------------------------------------------

(deftest stdout-exporter-writes-a-line-per-span
  (let [lines (atom [])
        exporter (stdout/exporter {:writer (fn [l] (swap! lines conj l))})
        provider (sdk/tracer-provider {:resource res/empty-resource
                                       :processors [(export/simple-processor exporter)]})
        tracer (sdk/get-tracer provider {:name "s"})]
    (trace/with-span [sp tracer "checkout" {:kind :server}]
      (trace/set-attribute! sp :http.method "GET"))
    (is (= 1 (count @lines)))
    (let [line (first @lines)]
      (is (str/includes? line "checkout"))
      (is (str/includes? line "[server]"))
      (is (str/includes? line "http.method")))))

(deftest json-exporter-writes-an-otlp-payload
  (let [out (atom [])
        exporter (stdout/json-exporter {:writer (fn [l] (swap! out conj l))})
        provider (sdk/tracer-provider {:resource res/empty-resource
                                       :processors [(export/simple-processor exporter)]})
        tracer (sdk/get-tracer provider {:name "s"})]
    (trace/with-span [sp tracer "op"])
    (is (str/starts-with? (first @out) "{\"resourceSpans\":["))))

(deftest a-shutdown-exporter-stops-writing
  (let [lines (atom [])
        exporter (stdout/exporter {:writer (fn [l] (swap! lines conj l))})]
    (export/shutdown-exporter! exporter)
    (is (false? (export/export-spans! exporter [{:name "x"}])))
    (is (= [] @lines))))
