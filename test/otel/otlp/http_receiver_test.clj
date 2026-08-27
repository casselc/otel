(ns otel.otlp.http-receiver-test
  (:require [clojure.test :refer [deftest is]]
            [hegel.clojure-test :refer [with]]
            [hegel.generator :as g]
            [hegel.stateful :as hs]
            [otel.otlp.http-receiver :as receiver]))

(def valid-wire
  {"resourceSpans"
   [{"scopeSpans"
     [{"spans"
       [{"traceId" "11111111111111111111111111111111"
         "spanId" "2222222222222222"
         "name" "work"
         "startTimeUnixNano" "1"
         "endTimeUnixNano" "2"}]}]}]})

(def valid-logs-wire
  {"resourceLogs"
   [{"resource" {"attributes" [{"key" "service.name"
                                  "value" {"stringValue" "receiver-test"}}]}
     "scopeLogs"
     [{"scope" {"name" "test.logs"}
       "logRecords"
       [{"timeUnixNano" "10" "observedTimeUnixNano" "11"
         "severityNumber" 9 "severityText" "INFO"
         "body" {"stringValue" "hello"}
         "traceId" "11111111111111111111111111111111"
         "spanId" "2222222222222222"}]}]}]})

(def valid-metrics-wire
  {"resourceMetrics"
   [{"resource" {"attributes" [{"key" "service.name"
                                  "value" {"stringValue" "receiver-test"}}]}
     "scopeMetrics"
     [{"scope" {"name" "test.metrics"}
       "metrics"
       [{"name" "temperature"
         "gauge" {"dataPoints" [{"timeUnixNano" "20" "asDouble" 21.5}]}}
        {"name" "requests"
         "sum" {"aggregationTemporality" 2 "isMonotonic" true
                "dataPoints" [{"startTimeUnixNano" "1" "timeUnixNano" "20"
                               "asInt" "4"}]}}
        {"name" "latency"
         "histogram" {"aggregationTemporality" 1
                      "dataPoints" [{"startTimeUnixNano" "1" "timeUnixNano" "20"
                                     "count" "2" "sum" 7.0
                                     "bucketCounts" ["1" "1"]
                                     "explicitBounds" [5.0]}]}}]}]}]})

(defn- parser
  ([request _limit]
   {:value (:body request) :encoded-bytes (or (:encoded-bytes request) 1)}))

(defn- recording-handler
  ([] (recording-handler {}))
  ([opts]
   (let [batches (atom [])]
     {:batches batches
      :handler (receiver/handler
                (merge {:parse-body parser
                        :exporter ::exporter
                        :export-spans! (fn [_ spans]
                                         (swap! batches conj spans)
                                         true)}
                       opts))})))

(defn- request
  ([] (request {}))
  ([m]
   (merge {:request-method :post
           :uri "/v1/traces"
           :headers {"content-type" "application/json"}
           :body valid-wire
           :encoded-bytes 100}
          m)))

(deftest enforces-ring-route-and-representation-policy
  (let [{:keys [handler batches]} (recording-handler)]
    (is (= 404 (:status (handler (request {:uri "/nope"})))))
    (let [r (handler (request {:request-method :get}))]
      (is (= 405 (:status r))))
    (is (= 415 (:status (handler (request {:headers {}})))))
    (is (= 415 (:status (handler (request
                                  {:headers {"Content-Type" "application/json; charset=utf-8"
                                             "Content-Encoding" "gzip"}})))))
    (is (= 200 (:status (handler (request
                                  {:headers {"content-type" "application/json"
                                             "content-encoding" "identity"}})))))
    (is (= 1 (count @batches)))))

(deftest enforces-actual-encoded-body-size
  (let [{:keys [handler]} (recording-handler {:max-body-bytes 10})]
    (is (= 413 (:status (handler (request
                                  {:headers {"content-type" "application/json"
                                             "content-length" "11"}
                                   :encoded-bytes 1})))))
    (is (= 413 (:status (handler (request {:encoded-bytes 11})))))
    (is (= 200 (:status (handler (request {:encoded-bytes 10}))))))
  (let [h (receiver/handler
           {:parse-body (fn [_ limit] (throw (receiver/body-too-large limit 11)))
            :exporter ::exporter
            :export-spans! (fn [_ _] true)
            :max-body-bytes 10})]
    (is (= 413 (:status (h (request)))))))

(deftest returns-standard-partial-success-and-exports-valid-siblings
  (let [{:keys [handler batches]} (recording-handler)
        bad (assoc-in valid-wire
                      ["resourceSpans" 0 "scopeSpans" 0 "spans" 0 "spanId"]
                      "bad")
        response (handler (request {:body bad}))]
    (is (= 200 (:status response)))
    (is (.contains (:body response) "partialSuccess"))
    (is (.contains (:body response) "rejectedSpans\":\"1"))
    (is (empty? @batches)))
  (let [{:keys [handler batches]} (recording-handler)
        response (handler (request))]
    (is (= 200 (:status response)))
    (is (= 1 (count (first @batches))))))

(deftest globally-malformed-otlp-is-a-structured-client-failure
  (let [{:keys [handler batches]} (recording-handler)
        response (handler (request {:body {"resourceSpans" {}}}))]
    (is (= 400 (:status response)))
    (is (.contains (:body response) "invalid OTLP trace request"))
    (is (= :otel.otlp.http-receiver/invalid-otlp-request
           (get-in response [::receiver/failure :type])))
    (is (empty? @batches))))

(deftest maps-parser-exporter-and-timeout-failures
  (let [bad-parser (receiver/handler
                    {:parse-body (fn [_ _] (throw (ex-info "bad json" {})))
                     :exporter ::exporter
                     :export-spans! (fn [_ _] true)})]
    (is (= 400 (:status (bad-parser (request))))))
  (let [{h :handler} (recording-handler
                      {:export-spans! (fn [_ _] (throw (ex-info "down" {})))})]
    (is (= 503 (:status (h (request))))))
  (let [{h :handler} (recording-handler
                      {:export-spans! (fn [_ _] false)})]
    (is (= 503 (:status (h (request))))))
  (let [{h :handler} (recording-handler
                      {:export-timeout-ms 25
                       :call-with-timeout (fn [ms _]
                                            (throw (receiver/export-timeout ms)))})]
    (is (= 504 (:status (h (request)))))))

(deftest limits-concurrent-requests-and-releases-the-slot
  (let [entered (promise)
        release (promise)
        calls (atom 0)
        {h :handler} (recording-handler
                      {:max-concurrency 1
                       :export-spans! (fn [_ _]
                                        (swap! calls inc)
                                        (deliver entered true)
                                        @release
                                        true)})
        first-response (promise)
        t (Thread. #(deliver first-response (h (request))))]
    (.start t)
    (is (= true (deref entered 2000 ::timeout)))
    (is (= 429 (:status (h (request)))))
    (deliver release true)
    (is (= 200 (:status (deref first-response 2000 nil))))
    (is (= 200 (:status (h (request)))))
    (is (= 2 @calls))))

(deftest parser-failure-releases-the-concurrency-slot
  (let [attempts (atom 0)
        h (receiver/handler
           {:parse-body (fn [_ _]
                          (if (= 1 (swap! attempts inc))
                            (throw (ex-info "bad" {}))
                            {:value valid-wire :encoded-bytes 1}))
            :exporter ::exporter
            :export-spans! (fn [_ _] true)
            :max-concurrency 1})]
    (is (= 400 (:status (h (request)))))
    (is (= 200 (:status (h (request)))))))

(deftest feedback-loop-suppression-is-explicit-and-pre-parse
  (let [seen (atom nil)
        h (receiver/handler
           {:parse-body (fn [request _]
                          (reset! seen (receiver/telemetry-suppressed? request))
                          {:value valid-wire :encoded-bytes 1})
            :exporter ::exporter
            :export-spans! (fn [_ _] true)})]
    (is (= 200 (:status (h (request)))))
    (is (true? @seen)))
  (let [seen (atom nil)
        wrapped (receiver/wrap-suppress-receiver-telemetry
                 (fn [request]
                   (reset! seen (receiver/telemetry-suppressed? request))
                   {:status 204}))]
    (wrapped (request))
    (is (true? @seen))))

(deftest routes-and-isolates-all-modeled-signals
  (let [span-batches (atom []) log-batches (atom []) metric-batches (atom [])
        h (receiver/handler
           {:parse-body parser :exporter ::traces :log-exporter ::logs
            :metric-exporter ::metrics
            :export-spans! (fn [exporter spans]
                             (swap! span-batches conj [exporter spans]) true)
            :export-logs! (fn [exporter records]
                            (swap! log-batches conj [exporter records]) true)
            :export-metrics! (fn [exporter resource collected]
                               (swap! metric-batches conj [exporter resource collected]) true)})]
    (is (= 200 (:status (h (request)))))
    (is (= 200 (:status (h (request {:uri receiver/logs-path :body valid-logs-wire})))))
    (is (= 200 (:status (h (request {:uri receiver/metrics-path :body valid-metrics-wire})))))
    (is (= [::traces] (mapv first @span-batches)))
    (is (= [::logs] (mapv first @log-batches)))
    (is (= [::metrics] (mapv first @metric-batches)))
    (is (= ["hello"] (mapv :body (second (first @log-batches)))))
    (let [metrics (-> @metric-batches first (nth 2) first :metrics)]
      (is (= [:gauge :sum :histogram] (mapv :type metrics)))
      (is (= 21.5 (-> metrics first :data-points first :value)))
      (is (= 4 (-> metrics second :data-points first :value)))
      (is (= [5.0] (-> metrics (nth 2) :explicit-bounds)))
      (is (= [1 1] (-> metrics (nth 2) :data-points first :bucket-counts))))
    (is (= 503 (:status ((receiver/handler
                          {:parse-body parser :exporter ::traces})
                         (request {:uri receiver/logs-path :body valid-logs-wire})))))))

(deftest signal-specific-partial-success-fields
  (let [logs (update-in valid-logs-wire ["resourceLogs" 0 "scopeLogs" 0 "logRecords"]
                        conj {"body" {"stringValue" "bad"} "traceId" "bad"})
        metrics (-> valid-metrics-wire
                    (update-in ["resourceMetrics" 0 "scopeMetrics" 0 "metrics" 0
                                "gauge" "dataPoints"]
                               conj {"timeUnixNano" "21"})
                    (update-in ["resourceMetrics" 0 "scopeMetrics" 0 "metrics"]
                               conj {"name" "unsupported"
                                     "summary" {"dataPoints" [{"count" "1"}]}}))
        log-seen (atom []) metric-seen (atom [])
        h (receiver/handler
           {:parse-body parser :log-exporter ::logs :metric-exporter ::metrics
            :export-logs! (fn [_ records] (reset! log-seen records) true)
            :export-metrics! (fn [_ _ collected] (reset! metric-seen collected) true)})
        log-response (h (request {:uri receiver/logs-path :body logs}))
        metric-response (h (request {:uri receiver/metrics-path :body metrics}))]
    (is (= 200 (:status log-response)))
    (is (.contains (:body log-response) "rejectedLogRecords\":\"1"))
    (is (not (.contains (:body log-response) "rejectedSpans")))
    (is (= 1 (count @log-seen)))
    (is (= 200 (:status metric-response)))
    (is (.contains (:body metric-response) "rejectedDataPoints\":\"2"))
    (is (not (.contains (:body metric-response) "rejectedLogRecords")))
    (is (= [:gauge :sum :histogram] (mapv :type (-> @metric-seen first :metrics))))
    (is (= 1 (count (-> @metric-seen first :metrics first :data-points))))))

(deftest admission-and-timeout-are-shared-across-signals
  (let [entered (promise) release (promise) logs (atom 0)
        h (receiver/handler
           {:parse-body parser :exporter ::traces :log-exporter ::logs
            :max-concurrency 1
            :export-spans! (fn [_ _] (deliver entered true) @release true)
            :export-logs! (fn [_ _] (swap! logs inc) true)})
        trace-response (promise)
        thread (Thread. #(deliver trace-response (h (request))))]
    (.start thread)
    (is (= true (deref entered 2000 ::timeout)))
    (is (= 429 (:status (h (request {:uri receiver/logs-path :body valid-logs-wire})))))
    (deliver release true)
    (is (= 200 (:status (deref trace-response 2000 nil))))
    (is (= 200 (:status (h (request {:uri receiver/logs-path :body valid-logs-wire})))))
    (is (= 1 @logs)))
  (let [h (receiver/handler
           {:parse-body parser :log-exporter ::logs :export-timeout-ms 5
            :call-with-timeout (fn [ms _] (throw (receiver/export-timeout ms)))
            :export-logs! (fn [_ _] true)})]
    (is (= 504 (:status (h (request {:uri receiver/logs-path :body valid-logs-wire})))))
    (is (= 504 (:status (h (request {:uri receiver/logs-path :body valid-logs-wire}))))
        "timeout releases the shared admission slot")))

(deftest suppression-covers-every-receiver-path
  (let [seen (atom [])
        wrapped (receiver/wrap-suppress-receiver-telemetry
                 (fn [r] (swap! seen conj [(:uri r) (receiver/telemetry-suppressed? r)])
                   {:status 204}))]
    (doseq [path [receiver/traces-path receiver/logs-path receiver/metrics-path "/other"]]
      (wrapped {:uri path}))
    (is (= [[receiver/traces-path true] [receiver/logs-path true]
            [receiver/metrics-path true] ["/other" false]] @seen))))

(def methods (g/sampled-from [:post :get :put :delete]))
(def content-types
  (g/sampled-from ["application/json" "application/json; charset=utf-8"
                   "text/plain" nil]))
(def signal-paths
  (g/sampled-from [receiver/traces-path receiver/logs-path receiver/metrics-path]))

(deftest request-policy-property
  (with {:test-cases 100 :database "" :verbosity :quiet}
    [method methods
     content-type content-types
     path signal-paths
     size (g/integer 0 32)
     limit (g/integer 1 32)]
    (let [h (receiver/handler
             {:parse-body parser :exporter ::traces :log-exporter ::logs
              :metric-exporter ::metrics :max-body-bytes limit
              :export-spans! (fn [_ _] true) :export-logs! (fn [_ _] true)
              :export-metrics! (fn [_ _ _] true)})
          body (case path receiver/logs-path valid-logs-wire
                     receiver/metrics-path valid-metrics-wire valid-wire)
          response (h (request {:request-method method
                                :uri path :body body
                                :headers (cond-> {}
                                           content-type (assoc "content-type" content-type))
                                :encoded-bytes size}))
          expected (cond
                     (not= method :post) 405
                     (not (#{"application/json" "application/json; charset=utf-8"}
                            content-type)) 415
                     (> size limit) 413
                     :else 200)]
      (is (= expected (:status response))))))

(deftest stateful-swarm-preserves-admission-invariants
  (with {:test-cases 20 :stateful-step-count 20 :database "" :verbosity :quiet}
    []
    (let [{h :handler batches :batches} (recording-handler {:max-body-bytes 8})]
      (hs/run!
       {:initial-state {:accepted 0 :batches batches}
        :rules
        [(hs/rule :valid
                  (fn [state]
                    (let [r (h (request {:encoded-bytes 8}))]
                      (is (= 200 (:status r)))
                      (update state :accepted inc))))
         (hs/rule :wrong-method
                  (fn [state]
                    (is (= 405 (:status (h (request {:request-method :get})))))
                    state))
         (hs/rule :wrong-content
                  (fn [state]
                    (is (= 415 (:status (h (request {:headers {"content-type" "text/plain"}})))))
                    state))
         (hs/rule :oversize
                  (fn [state]
                    (is (= 413 (:status (h (request {:encoded-bytes 9})))))
                    state))]
        :invariants
        [(hs/invariant :only-accepted-requests-export
                       (fn [{:keys [accepted batches]}]
                         (= accepted (count @batches))))]}))))

(deftest stateful-swarm-keeps-signal-exporters-isolated
  (with {:test-cases 20 :stateful-step-count 24 :database "" :verbosity :quiet}
    []
    (let [calls (atom {:traces 0 :logs 0 :metrics 0})
          h (receiver/handler
             {:parse-body parser :exporter ::traces :log-exporter ::logs
              :metric-exporter ::metrics :max-body-bytes 8
              :export-spans! (fn [_ _] (swap! calls update :traces inc) true)
              :export-logs! (fn [_ _] (swap! calls update :logs inc) true)
              :export-metrics! (fn [_ _ _] (swap! calls update :metrics inc) true)})]
      (hs/run!
       {:initial-state {:traces 0 :logs 0 :metrics 0}
        :rules
        [(hs/rule :trace
                  (fn [state]
                    (is (= 200 (:status (h (request {:encoded-bytes 8})))))
                    (update state :traces inc)))
         (hs/rule :log
                  (fn [state]
                    (is (= 200 (:status (h (request {:uri receiver/logs-path
                                                     :body valid-logs-wire
                                                     :encoded-bytes 8})))))
                    (update state :logs inc)))
         (hs/rule :metric
                  (fn [state]
                    (is (= 200 (:status (h (request {:uri receiver/metrics-path
                                                     :body valid-metrics-wire
                                                     :encoded-bytes 8})))))
                    (update state :metrics inc)))
         (hs/rule :rejected-before-export
                  (fn [state]
                    (is (= 413 (:status (h (request {:uri receiver/logs-path
                                                     :body valid-logs-wire
                                                     :encoded-bytes 9})))))
                    state))]
        :invariants [(hs/invariant :exact-signal-delivery #(= % @calls))]}))))
