(ns otel.otlp.trace-decode-test
  (:require [clojure.edn :as edn]
            [clojure.test :refer [deftest is testing]]
            [hegel.clojure-test :refer [with]]
            [hegel.generator :as g]
            [otel.otlp.encode :as encode]
            [otel.otlp.trace-decode :as decode]
            [otel.resource :as resource]))

(def fixture
  (edn/read-string (slurp "test/fixtures/otlp/traces-v1.edn")))

(deftest decodes-pinned-representative-request
  (let [{:keys [spans rejected-spans errors]} (decode/decode-request fixture)
        [parent child] spans
        event (first (:events parent))
        link (first (:links parent))]
    (is (= 2 (count spans)))
    (is (zero? rejected-spans))
    (is (empty? errors))
    (is (= "checkout" (get (:attributes (:resource parent)) "service.name")))
    (is (= 2 (:dropped-attributes-count (:resource parent))))
    (is (= {:name "checkout.http" :version "3.1.4"
            :schema-url "https://opentelemetry.io/schemas/1.26.0"
            :attributes {"scope.attr" true} :dropped-attributes-count 1}
           (:scope parent)))
    (is (= "3333333333333333" (:parent-span-id parent)))
    (is (= 257 (get-in parent [:span-context :trace-flags])))
    (is (= [["vendor" "opaque"] ["other" "value"]]
           (get-in parent [:span-context :trace-state])))
    (is (= :server (:kind parent)))
    (is (= 1785609674781645000 (:start-time-unix-nano parent)))
    (is (= {"http.request.method" "GET" "retry.count" 3 "cache.hits" [1 2]}
           (:attributes parent)))
    (is (= 4 (:dropped-attributes-count parent)))
    (is (= 6 (:dropped-events-count parent)))
    (is (= 8 (:dropped-links-count parent)))
    (is (= {:code :error :description "failed"} (:status parent)))
    (is (= 5 (:dropped-attributes-count event)))
    (is (= 513 (get-in link [:span-context :trace-flags])))
    (is (= 7 (:dropped-attributes-count link)))
    (is (:ended? parent))
    (is (nil? (:parent-span-id child)))
    (is (= :internal (:kind child)))
    (is (= {:code :unset :description nil} (:status child)))))

(deftest decoded-metadata-survives-reencoding
  (let [decoded (:spans (decode/decode-request fixture))
        encoded (encode/traces-request decoded)
        parent (get-in encoded [:resourceSpans 0 :scopeSpans 0 :spans 0])]
    (is (= 2 (get-in encoded [:resourceSpans 0 :resource
                              :droppedAttributesCount])))
    (is (= 1 (get-in encoded [:resourceSpans 0 :scopeSpans 0 :scope
                              :droppedAttributesCount])))
    (is (= 5 (get-in parent [:events 0 :droppedAttributesCount])))
    (is (= 513 (get-in parent [:links 0 :flags])))
    (is (= 7 (get-in parent [:links 0 :droppedAttributesCount])))))

(deftest rejects-only-the-invalid-individual-span
  (let [bad (assoc-in fixture
                      ["resourceSpans" 0 "scopeSpans" 0 "spans" 1 "spanId"]
                      "not-a-span-id")
        {:keys [spans rejected-spans errors]} (decode/decode-request bad)
        [error] errors]
    (is (= ["GET /cart"] (mapv :name spans)))
    (is (= 1 rejected-spans))
    (is (= 1 (count errors)))
    (is (= ["resourceSpans" 0 "scopeSpans" 0 "spans" 1 "spanId"]
           (:path error)))
    (is (= :invalid-id (:reason error)))))

(deftest reports-container-errors-with-paths
  (let [result (decode/decode-request {"resourceSpans" {}})]
    (is (empty? (:spans result)))
    (is (= :wrong-type (get-in result [:errors 0 :reason])))
    (is (= ["resourceSpans"] (get-in result [:errors 0 :path])))))

(deftest preserves-the-full-uint64-timestamp-domain
  (let [request (assoc-in fixture
                          ["resourceSpans" 0 "scopeSpans" 0 "spans" 1
                           "endTimeUnixNano"]
                          "18446744073709551615")
        result (decode/decode-request request)]
    (is (zero? (:rejected-spans result)))
    (is (= 18446744073709551615
           (get-in result [:spans 1 :end-time-unix-nano]))))
  (let [request (assoc-in fixture
                          ["resourceSpans" 0 "scopeSpans" 0 "spans" 1
                           "endTimeUnixNano"]
                          "18446744073709551616")
        result (decode/decode-request request)]
    (is (= 1 (:rejected-spans result)))
    (is (= :out-of-range (get-in result [:errors 0 :reason])))))

(deftest unsupported-any-values-are-not-silently-lost
  (let [bad (assoc-in fixture
                      ["resourceSpans" 0 "scopeSpans" 0 "spans" 1 "attributes"]
                      [{"key" "nested" "value" {"kvlistValue" {"values" []}}}])
        result (decode/decode-request bad)]
    (is (= 1 (:rejected-spans result)))
    (is (= :unsupported-any-value (get-in result [:errors 0 :reason])))
    (is (= ["resourceSpans" 0 "scopeSpans" 0 "spans" 1
            "attributes" 0 "value" "kvlistValue"]
           (get-in result [:errors 0 :path])))))

(deftest double-value-accepts-an-integer-json-token
  (let [request (assoc-in fixture
                          ["resourceSpans" 0 "scopeSpans" 0 "spans" 1 "attributes"]
                          [{"key" "ratio" "value" {"doubleValue" 1}}])
        result (decode/decode-request request)]
    (is (zero? (:rejected-spans result)))
    (is (= 1.0 (get-in result [:spans 1 :attributes "ratio"])))))

(deftest double-value-rejects-non-finite-host-values
  (let [request (assoc-in fixture
                          ["resourceSpans" 0 "scopeSpans" 0 "spans" 1 "attributes"]
                          [{"key" "ratio" "value" {"doubleValue" ##Inf}}])
        result (decode/decode-request request)]
    (is (= 1 (:rejected-spans result)))
    (is (= :wrong-type (get-in result [:errors 0 :reason])))))

(def id32-gen (g/regex-str #"[1-9a-f][0-9a-f]{31}"))
(def id16-gen (g/regex-str #"[1-9a-f][0-9a-f]{15}"))
(def text-gen (g/string {:max-size 24 :codec :utf-8}))

(deftest encoder-decoder-roundtrip-property
  (with {:test-cases 120 :database "" :verbosity :quiet}
    [trace-id id32-gen
     span-id id16-gen
     parent-id id16-gen
     span-name text-gen
     attr-key (g/string {:min-size 1 :max-size 16 :alphabet "abcdefghijklmnopqrstuvwxyz."})
     attr-value text-gen
     start (g/integer 0 9000000000000000000)
     duration (g/integer 0 1000000)
     kind (g/sampled-from [:internal :server :client :producer :consumer])
     status (g/sampled-from [:unset :ok :error])]
    (let [span {:name span-name
                :span-context {:trace-id trace-id :span-id span-id
                               :trace-flags 1 :trace-state [["vendor" "state"]]
                               :remote? false}
                :parent-span-id parent-id
                :kind kind
                :scope {:name "property" :version "1" :schema-url "scope-schema"
                        :attributes {}}
                :resource (resource/resource {"service.name" "property"}
                                             {:schema-url "resource-schema"})
                :start-time-unix-nano start
                :end-time-unix-nano (+ start duration)
                :attributes {attr-key attr-value}
                :events [] :links []
                :status {:code status :description (when (= :error status) "failed")}
                :dropped-attributes-count 0 :dropped-events-count 0
                :dropped-links-count 0 :ended? true}
          request (encode/traces-request [span])
          result (decode/decode-request request)]
      (is (zero? (:rejected-spans result)))
      (is (empty? (:errors result)))
      (is (= request (encode/traces-request (:spans result)))))))
