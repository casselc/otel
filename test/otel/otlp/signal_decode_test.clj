(ns otel.otlp.signal-decode-test
  (:require [clojure.test :refer [deftest is]]
            [otel.any-value :as any]
            [otel.exporter.memory :as memory]
            [otel.logs :as logs]
            [otel.metrics :as metrics]
            [otel.otlp.encode :as encode]
            [otel.otlp.signal-decode :as decode]
            [otel.resource :as resource]
            [otel.sdk.clock :as clock]
            [otel.sdk.logs :as sdk-logs]
            [otel.sdk.metrics :as sdk-metrics]
            [otel.trace :as trace]))

(def ^:private typed-attributes
  {"array" [0 false "" any/empty-value]
   "boolean" false
   "boolean.text" "false"
   "bytes" (any/bytes [0 255])
   "double" 1.5
   "empty.string" ""
   "empty.value" any/empty-value
   "integer" 42
   "integer.max" any/max-int64
   "integer.min" any/min-int64
   "integer.text" "42"
   "nested" {"enabled" false "items" [1 "two"]}
   "zero" 0})

(deftest sdk-log-records-are-the-canonical-receiver-records
  (let [exporter (memory/log-exporter)
        provider (sdk-logs/logger-provider
                  {:resource (resource/resource {"resource.boolean" false})
                   :clock (clock/fake-clock {:wall 1000 :mono 0})
                   :processors [(sdk-logs/simple-processor exporter)]})
        logger (sdk-logs/get-logger
                provider
                {:name "roundtrip" :version "1"
                 :attributes {"scope.integer" 42}})
        bodies ["text" false 42 any/min-int64 any/max-int64 1.5]
        correlated-context
        (trace/span-context
         {:trace-id "10000000000000000000000000000000"
          :span-id "2000000000000000"
          :trace-flags 0})]
    (doseq [body bodies]
      (logs/emit! logger {:body body :severity :info
                          :attributes typed-attributes}))
    (trace/with-current-span (trace/non-recording-span correlated-context)
      (logs/emit! logger {:body "correlated" :severity :info
                          :attributes typed-attributes}))
    (let [direct (vec (memory/records exporter))
          request (encode/logs-request direct)
          result (decode/decode-logs request)
          relayed (:records result)
          correlated (last relayed)]
      (is (zero? (:rejected-log-records result)))
      (is (empty? (:errors result)))
      (is (= direct relayed))
      (is (= (conj bodies "correlated") (mapv :body relayed)))
      (is (= typed-attributes (:attributes (first relayed))))
      (is (not (contains? (:attributes (first relayed)) "absent")))
      (doseq [value [(:resource (first relayed)) (:scope (first relayed))
                     (first relayed)]]
        (is (not (contains? value :dropped-attributes-count))))
      (doseq [record (butlast relayed)]
        (is (not (contains? record :trace-id)))
        (is (not (contains? record :span-id)))
        (is (not (contains? record :trace-flags))))
      (is (= (:trace-id correlated-context) (:trace-id correlated)))
      (is (= (:span-id correlated-context) (:span-id correlated)))
      (is (contains? correlated :trace-flags))
      (is (= 0 (:trace-flags correlated)))
      (let [counted (-> request
                        (assoc-in [:resourceLogs 0 :resource
                                   :droppedAttributesCount] 1)
                        (assoc-in [:resourceLogs 0 :scopeLogs 0 :scope
                                   :droppedAttributesCount] 2)
                        (assoc-in [:resourceLogs 0 :scopeLogs 0 :logRecords 0
                                   :droppedAttributesCount] 3))
            counted-record (first (:records (decode/decode-logs counted)))]
        (is (= 1 (get-in counted-record
                         [:resource :dropped-attributes-count])))
        (is (= 2 (get-in counted-record
                         [:scope :dropped-attributes-count])))
        (is (= 3 (:dropped-attributes-count counted-record)))))))

(deftest sdk-metric-collection-is-the-canonical-receiver-collection
  (let [resource (resource/resource {"resource.boolean" false})
        provider (sdk-metrics/meter-provider
                  {:resource resource
                   :clock (clock/fake-clock {:wall 1000 :mono 0})})
        meter (sdk-metrics/get-meter
               provider
               {:name "roundtrip" :version "1"
                :attributes {"scope.integer" 42 :dropped nil}})
        sum (metrics/counter meter "requests")
        gauge (metrics/gauge meter "temperature")
        histogram (metrics/histogram meter "latency" {:boundaries [5.0]})]
    (metrics/add! sum 42 typed-attributes)
    (metrics/set-value! gauge 1.5 typed-attributes)
    (metrics/record! histogram 3.0 typed-attributes)
    (let [collected (vec (sdk-metrics/collect! provider))
          direct {:resource resource :collected collected}
          result (decode/decode-metrics
                  (encode/metrics-request resource collected))
          relayed (first (:collections result))
          metrics (get-in relayed [:collected 0 :metrics])
          gauge-point (->> metrics (filter #(= :gauge (:type %))) first
                           :data-points first)]
      (is (zero? (:rejected-data-points result)))
      (is (empty? (:errors result)))
      (is (= direct relayed))
      (is (= #{:gauge :sum :histogram} (set (map :type metrics))))
      (is (= typed-attributes (:attributes gauge-point)))
      (is (not (contains? (:attributes gauge-point) "absent")))
      (is (not (contains? gauge-point :start-time-unix-nano)))
      (is (= 1 (get-in relayed [:collected 0 :scope
                                :dropped-attributes-count])))
      (is (not (contains? (:resource relayed)
                          :dropped-attributes-count))))))
