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

(deftest parses-header-config
  (is (= {"api-key" "secret"} (otlp/parse-headers "api-key=secret")))
  (is (= {"a" "1" "b" "2"} (otlp/parse-headers "a=1,b=2")))
  (is (= {} (otlp/parse-headers "")))
  (is (= {} (otlp/parse-headers nil)))
  (testing "a malformed entry is skipped, not fatal"
    (is (= {"b" "2"} (otlp/parse-headers "junk,b=2")))))

(deftest https-is-rejected-with-a-clear-error
  (testing "an https endpoint must fail at construction with an actionable message,
            not obscurely at the first export"
    (let [e (try (otlp/exporter {:endpoint "https://collector.example.com"}) nil
                 (catch :default e e))]
      (is (some? e))
      (is (str/includes? (ex-message e) "http://")))))

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
