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
        inputs ["text" false 42 any/min-int64 any/max-int64 1.5
                :ready 'phase/ready
                "" nil any/empty-value (any/bytes [0 255]) {} []
                {:event :joined
                 :players [{:id 1 :ready true}
                           {:id 2 :ready false}]
                 :metadata {:rounds [1 2 any/empty-value]}}]
        canonical-bodies ["text" false 42 any/min-int64 any/max-int64 1.5
                          "ready" "phase/ready"
                          "" "" any/empty-value (any/bytes [0 255])
                          (sorted-map) []
                          (sorted-map
                           "event" "joined"
                           "metadata" (sorted-map
                                       "rounds" [1 2 any/empty-value])
                           "players" [(sorted-map "id" 1 "ready" true)
                                      (sorted-map "id" 2 "ready" false)])]
        correlated-context
        (trace/span-context
         {:trace-id "10000000000000000000000000000000"
          :span-id "2000000000000000"
          :trace-flags 0})]
    (doseq [body inputs]
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
      (is (= (conj canonical-bodies "correlated") (mapv :body direct)))
      (is (= (conj canonical-bodies "correlated") (mapv :body relayed)))
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

(deftest sdk-log-record-dropped-attribute-counts-round-trip-and-reject-bad-siblings
  (let [exporter (memory/log-exporter)
        provider (sdk-logs/logger-provider
                  {:resource resource/empty-resource
                   :clock (clock/fake-clock {:wall 1000 :mono 0})
                   :processors [(sdk-logs/simple-processor exporter)]})
        logger (sdk-logs/get-logger provider {:name "dropped-roundtrip"})]
    (logs/emit! logger {:body "loss" :severity :info
                        :attributes {:kept 1 :rejected nil}})
    (logs/emit! logger {:body "complete" :severity :info
                        :attributes {:kept 2}})
    (let [direct (vec (memory/records exporter))
          request (encode/logs-request direct)
          wire-records (get-in request [:resourceLogs 0 :scopeLogs 0 :logRecords])
          decoded (decode/decode-logs request)]
      (is (= 1 (:dropped-attributes-count (first direct))))
      (is (not (contains? (second direct) :dropped-attributes-count)))
      (is (= 1 (:droppedAttributesCount (first wire-records))))
      (is (not (contains? (second wire-records) :droppedAttributesCount)))
      (is (= direct (:records decoded)))
      (is (zero? (:rejected-log-records decoded)))
      (doseq [bad [-1 4294967296 "not-an-integer"]]
        (let [malformed (assoc-in request
                                  [:resourceLogs 0 :scopeLogs 0 :logRecords 0
                                   :droppedAttributesCount]
                                  bad)
              result (decode/decode-logs malformed)]
          (is (= 1 (:rejected-log-records result)))
          (is (= [(second direct)] (:records result)))
          (is (= :invalid-integer (-> result :errors first :reason)))
          (is (= ["resourceLogs" 0 "scopeLogs" 0 "logRecords" 0
                  "droppedAttributesCount"]
                 (-> result :errors first :path))))))))

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
        histogram (metrics/histogram meter "latency" {:boundaries [5.0]})
        point-input-attributes (assoc typed-attributes "rejected" nil)]
    (metrics/add! sum 42 point-input-attributes)
    (metrics/set-value! gauge 1.5 point-input-attributes)
    (metrics/record! histogram 3.0 point-input-attributes)
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
      (is (= 0 (:flags gauge-point)))
      (is (contains? gauge-point :flags))
      (doseq [metric metrics
              point (:data-points metric)]
        (is (= 0 (:flags point)))
        (is (= 1 (:dropped-attributes-count point))))
      (is (not (contains? (:attributes gauge-point) "absent")))
      (is (not (contains? gauge-point :start-time-unix-nano)))
      (is (= 1 (get-in relayed [:collected 0 :scope
                                :dropped-attributes-count])))
      (is (not (contains? (:resource relayed)
                          :dropped-attributes-count))))))

(deftest metric-point-metadata-preserves-presence-and-partially-rejects-malformed-siblings
  (let [points [{:timeUnixNano "1" :asInt "1"}
                {:timeUnixNano "2" :asInt "2" :flags 0
                 :droppedAttributesCount 0}
                {:timeUnixNano "3" :asInt "3" :flags 4294967295
                 :droppedAttributesCount 4294967295}
                {:timeUnixNano "4" :asInt "4" :flags 4294967296}
                {:timeUnixNano "5" :asInt "5"
                 :droppedAttributesCount "not-an-integer"}]
        request {:resourceMetrics
                 [{:scopeMetrics
                   [{:metrics
                     [{:name "points" :gauge {:dataPoints points}}]}]}]}
        result (decode/decode-metrics request)
        decoded (get-in result [:collections 0 :collected 0 :metrics 0
                                :data-points])]
    (is (= 2 (:rejected-data-points result)))
    (is (= [1 2 3] (mapv :value decoded)))
    (is (not (contains? (first decoded) :flags)))
    (is (= 0 (:flags (second decoded))))
    (is (contains? (second decoded) :flags))
    (is (not (contains? (second decoded) :dropped-attributes-count)))
    (is (= 4294967295 (:flags (nth decoded 2))))
    (is (= 4294967295 (:dropped-attributes-count (nth decoded 2))))
    (is (= [["resourceMetrics" 0 "scopeMetrics" 0 "metrics" 0 "gauge"
             "dataPoints" 3 "flags"]
            ["resourceMetrics" 0 "scopeMetrics" 0 "metrics" 0 "gauge"
             "dataPoints" 4 "droppedAttributesCount"]]
           (mapv :path (:errors result))))))
