(ns otel.resource-test
  (:require [clojure.test :refer [deftest is testing]]
            [otel.resource :as res]))

(deftest resource-holds-normalized-attributes
  (let [r (res/resource {:service.name "checkout" :replicas 3})]
    (is (= "checkout" (get (res/attributes r) "service.name")))
    (is (= 3 (get (res/attributes r) "replicas")))))

(deftest merge-prefers-the-updating-resource
  (testing "spec: on conflict the updating (right) resource wins"
    (let [a (res/resource {:service.name "a" :only.a 1})
          b (res/resource {:service.name "b" :only.b 2})
          m (res/merge-resources a b)]
      (is (= "b" (get (res/attributes m) "service.name")))
      (is (= 1 (get (res/attributes m) "only.a")))
      (is (= 2 (get (res/attributes m) "only.b"))))))

(deftest merge-keeps-the-old-schema-url-when-the-new-one-is-absent
  (let [a (res/resource {:a 1} {:schema-url "https://example/v1"})
        b (res/resource {:b 2})]
    (is (= "https://example/v1" (res/schema-url (res/merge-resources a b))))))

(deftest merge-takes-the-updating-schema-url
  (let [a (res/resource {:a 1} {:schema-url "https://example/v1"})
        b (res/resource {:b 2} {:schema-url "https://example/v2"})]
    (is (= "https://example/v2" (res/schema-url (res/merge-resources a b))))))

(deftest empty-resource-has-no-attributes
  (is (= {} (res/attributes res/empty-resource))))

(deftest telemetry-sdk-attributes-are-present
  (testing "every resource must identify the SDK that produced the telemetry"
    (let [a (res/attributes (res/default-resource))]
      (is (= "opentelemetry" (get a "telemetry.sdk.name")))
      (is (= "jolt" (get a "telemetry.sdk.language")))
      (is (string? (get a "telemetry.sdk.version"))))))

(deftest service-name-defaults-to-the-spec-fallback
  (let [a (res/attributes (res/default-resource))]
    (is (= "unknown_service:jolt" (get a "service.name")))))

(deftest explicit-service-name-wins-over-the-default
  (let [a (res/attributes (res/merge-resources (res/default-resource)
                                               (res/resource {:service.name "checkout"})))]
    (is (= "checkout" (get a "service.name")))))

(deftest process-attributes-describe-the-running-process
  (let [a (res/attributes (res/process-resource))]
    (is (pos? (get a "process.pid")))
    (is (= "Chez Scheme" (get a "process.runtime.name")))
    (is (string? (get a "process.runtime.version")))
    (is (string? (get a "process.runtime.description")))))

(deftest host-attributes-describe-the-machine
  (let [a (res/attributes (res/host-resource))]
    (is (string? (get a "host.arch")))
    (is (string? (get a "os.type")))))

(deftest parses-otel-resource-attributes-syntax
  (testing "the OTEL_RESOURCE_ATTRIBUTES W3C-baggage-style list"
    (is (= {"a" "1" "b" "2"} (res/parse-resource-attributes "a=1,b=2")))
    (is (= {"a" "1"} (res/parse-resource-attributes " a = 1 ")))
    (is (= {} (res/parse-resource-attributes "")))
    (is (= {} (res/parse-resource-attributes nil)))
    (testing "malformed entries are skipped, not fatal"
      (is (= {"b" "2"} (res/parse-resource-attributes "novalue,b=2"))))
    (testing "a value may contain = (only the first splits)"
      (is (= {"u" "k=v"} (res/parse-resource-attributes "u=k=v"))))
    (testing "percent-encoded values are decoded, per the spec's baggage syntax"
      (is (= {"a" "x y"} (res/parse-resource-attributes "a=x%20y"))))))
