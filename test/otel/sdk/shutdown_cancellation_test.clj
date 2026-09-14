(ns otel.sdk.shutdown-cancellation-test
  (:require [clojure.test :refer [deftest is testing]]
            [jolt.socket]
            [otel.exporter.memory :as memory]
            [otel.exporter.otlp :as otlp]
            [otel.logs :as log-api]
            [otel.metrics :as metric-api]
            [otel.resource :as resource]
            [otel.sdk.export :as export]
            [otel.sdk.lifecycle :as lifecycle]
            [otel.sdk.logs :as sdk-logs]
            [otel.sdk.metrics :as sdk-metrics]
            [otel.sdk.tracer :as sdk-tracer]
            [otel.trace :as trace]))

(defn- close-quietly [value]
  (when value
    (try (.close value) (catch Throwable _ nil))))

(defn- await! [pred timeout-ms]
  (loop [remaining timeout-ms]
    (cond
      (pred) true
      (pos? remaining) (do (Thread/sleep 5) (recur (- remaining 5)))
      :else false)))

(defn- read-request-head! [input]
  (loop [chars [] tail ""]
    (let [octet (.read input)]
      (when (neg? octet)
        (throw (ex-info "peer closed before request headers completed" {})))
      (let [ch (char octet)
            chars (conj chars ch)
            tail (str tail ch)]
        (if (.endsWith tail "\r\n\r\n")
          (apply str chars)
          (recur chars (if (> (count tail) 4) (subs tail 1) tail)))))))

(defn- read-request! [socket]
  (let [input (.getInputStream socket)
        head (read-request-head! input)
        [_ length] (re-find #"(?i)\r\ncontent-length:\s*([0-9]+)\r\n"
                            (str "\r\n" head))
        length (when length (Long/parseLong length))]
    (when-not length
      (throw (ex-info "OTLP POST did not declare Content-Length" {})))
    (loop [remaining length]
      (when (pos? remaining)
        (let [octet (.read input)]
          (when (neg? octet)
            (throw (ex-info "peer closed before request body completed"
                            {:remaining remaining})))
          (recur (dec remaining)))))
    {:content-length length}))

(defn- start-stalled-collector [path]
  (let [listener (java.net.ServerSocket. 0)
        peers (atom [])
        requests (atom 0)
        first-request (promise)
        acceptor
        (future
          (try
            (loop []
              (let [socket (.accept listener)
                    request-number (swap! requests inc)]
                (swap! peers conj socket)
                (if (= 1 request-number)
                  (deliver first-request (read-request! socket))
                  (read-request! socket))
                ;; Keep every request open without returning response bytes.
                ;; The next accept remains live, making an accidental POST
                ;; replay observable as a second server-side request.
                (recur)))
            (catch Throwable error
              (when-not (realized? first-request)
                (deliver first-request error)))))]
    {:listener listener
     :peers peers
     :requests requests
     :first-request first-request
     :acceptor acceptor
     :url (str "http://127.0.0.1:" (.getLocalPort listener) path)}))

(defn- stop-collector! [{:keys [listener peers acceptor]}]
  (close-quietly listener)
  (doseq [peer @peers] (close-quietly peer))
  (deref acceptor 2000 nil))

(defrecord CountingExporter [delegate shutdown-calls]
  export/SpanExporter
  (export-spans! [_ spans] (export/export-spans! delegate spans))
  (flush-exporter! [_] (export/flush-exporter! delegate))
  (shutdown-exporter! [_]
    (swap! shutdown-calls inc)
    (export/shutdown-exporter! delegate)))

(defrecord CountingLogExporter [delegate shutdown-calls]
  sdk-logs/LogRecordExporter
  (export-logs! [_ records] (sdk-logs/export-logs! delegate records))
  (shutdown-log-exporter! [_]
    (swap! shutdown-calls inc)
    (sdk-logs/shutdown-log-exporter! delegate)))

(defrecord CountingMetricExporter [delegate shutdown-calls]
  export/MetricExporter
  (export-metrics! [_ resource collected]
    (export/export-metrics! delegate resource collected))
  (shutdown-metric-exporter! [_]
    (swap! shutdown-calls inc)
    (export/shutdown-metric-exporter! delegate)))

(defn- named-processor [pipelines destination]
  (some (fn [[name processor]]
          (when (= destination name) processor))
        (:pipelines pipelines)))

(deftest shutdown-cancels-only-the-owned-stalled-otlp-worker
  (let [collector (start-stalled-collector "/v1/traces")
        remote-delegate (otlp/exporter {:traces-url (:url collector)
                                        :timeout-ms 10000
                                        :max-retries 0
                                        :environment? false})
        healthy-delegate (memory/exporter)
        remote-shutdowns (atom 0)
        healthy-shutdowns (atom 0)
        remote (->CountingExporter remote-delegate remote-shutdowns)
        healthy (->CountingExporter healthy-delegate healthy-shutdowns)
        pipelines (export/independent-batch-pipelines
                   (array-map
                    :remote {:exporter remote
                             :config {:schedule-delay-ms 1
                                      :max-export-batch-size 1
                                      :max-queue-size 2}}
                    :healthy {:exporter healthy
                              :config {:schedule-delay-ms 1
                                       :max-export-batch-size 1
                                       :max-queue-size 2}}))
        provider (sdk-tracer/tracer-provider
                  {:resource resource/empty-resource
                   :processors [pipelines]})
        tracer (sdk-tracer/get-tracer provider {:name "shutdown.cancellation"})
        remote-processor (named-processor pipelines :remote)
        healthy-processor (named-processor pipelines :healthy)
        flush-result (promise)
        shutdown-result (promise)
        flush-thread (Thread.
                      #(deliver flush-result
                                (export/force-flush-pipelines! pipelines)))
        shutdown-thread (Thread.
                         #(deliver shutdown-result
                                   (export/shutdown-pipelines! pipelines)))]
    (try
      (trace/with-span [span tracer "blocked-otlp"])
      (let [request (deref (:first-request collector) 3000 ::not-read)]
        (is (map? request)
            (str "collector must consume the complete OTLP POST before "
                 "withholding its response: " request)))
      ;; Two more accepted batches remain behind the in-flight request. Once
      ;; shutdown cancels that owned request they must be accounted failed
      ;; without entering two fresh blocking POSTs.
      (trace/with-span [span tracer "queued-1"])
      (trace/with-span [span tracer "queued-2"])
      (is (await! #(= 3 (count (memory/spans healthy-delegate))) 3000)
          "the independent healthy queue exports while OTLP is stalled")
      (is (= 2 (export/queue-size remote-processor)))
      (is (:worker-export-active? @(:state remote-processor))
          "the selected remote worker must own exporter I/O")

      (testing "force-flush remains a completion barrier, not cancellation"
        (.start flush-thread)
        (is (= ::still-blocked (deref flush-result 350 ::still-blocked)))
        (is (zero? (:worker-interrupt-count @(:state remote-processor)))))

      (testing "shutdown retires admission, then cancels only its stalled worker"
        (let [started (System/currentTimeMillis)]
          (.start shutdown-thread)
          (let [result (deref shutdown-result 3500 ::still-blocked)
                elapsed (- (System/currentTimeMillis) started)]
            (is (= {:remote {:ok? false :failure :returned-false}
                    :healthy {:ok? true}}
                   result))
            (is (< elapsed (+ lifecycle/shutdown-cancel-grace-ms
                              lifecycle/shutdown-join-bound-ms
                              750))
                (str "shutdown exceeded its documented cancellation bound: "
                     elapsed " ms")))))
      (is (= {:remote {:ok? false :failure :returned-false}
              :healthy {:ok? true}}
             (deref flush-result 2000 ::still-blocked)))
      (is (= 1 (:worker-interrupt-count @(:state remote-processor))))
      (is (zero? (:worker-interrupt-count @(:state healthy-processor)))
          "the healthy sibling worker is never interrupted")
      (is (= {:queue-size 0
              :attempted-span-count 3
              :exported-span-count 0
              :failed-span-count 3
              :dropped-count 0}
             (export/batch-processor-stats remote-processor))
          "cancelled queued batches are explicit failures without new POSTs")

      (testing "cleanup and terminal results are exactly once"
        (let [first-result @shutdown-result]
          (is (identical? first-result
                          (export/shutdown-pipelines! pipelines))))
        (is (= 1 @remote-shutdowns))
        (is (= 1 @healthy-shutdowns)))

      ;; The request body crossed the server boundary before interruption. A
      ;; retry would therefore duplicate a non-idempotent POST and be visible
      ;; to the collector's still-live accept loop.
      (Thread/sleep 300)
      (is (= 1 @(:requests collector))
          "interrupted OTLP POST is surfaced as failure, never replayed")
      (finally
        (.interrupt (:worker remote-processor))
        (.interrupt flush-thread)
        (.interrupt shutdown-thread)
        (stop-collector! collector)
        (.join flush-thread 2000)
        (.join shutdown-thread 2000)
        (try (sdk-tracer/shutdown! provider) (catch Throwable _ nil))))))

(deftest stalled-otlp-log-cancellation-remains-a-failed-shutdown
  (let [collector (start-stalled-collector "/v1/logs")
        delegate (otlp/log-exporter {:logs-url (:url collector)
                                     :timeout-ms 10000
                                     :max-retries 0
                                     :environment? false})
        shutdowns (atom 0)
        exporter (->CountingLogExporter delegate shutdowns)
        processor (sdk-logs/batch-processor
                   exporter {:schedule-delay-ms 1
                             :max-export-batch-size 1
                             :max-queue-size 2})
        provider (sdk-logs/logger-provider
                  {:resource resource/empty-resource
                   :processors [processor]})
        logger (sdk-logs/get-logger provider {:name "shutdown.logs"})
        result (promise)
        shutdown-thread (Thread. #(deliver result (export/shutdown! processor)))]
    (try
      (log-api/emit! logger {:body "blocked" :severity :info})
      (is (map? (deref (:first-request collector) 3000 ::not-read)))
      (log-api/emit! logger {:body "queued-1" :severity :info})
      (log-api/emit! logger {:body "queued-2" :severity :info})
      (is (= 2 (count (:queue @(:state processor)))))
      (.start shutdown-thread)
      (is (false? (deref result 3000 ::still-blocked))
          "the interrupted owned log export remains a failed shutdown")
      (is (:worker-export-failed? @(:state processor)))
      (is (= 1 (:worker-interrupt-count @(:state processor))))
      (is (empty? (:queue @(:state processor))))
      (is (not (.isAlive (:worker processor))))
      (is (= 1 @shutdowns))
      (is (false? (export/shutdown! processor)))
      (is (= 1 @shutdowns) "log exporter closes exactly once")
      (Thread/sleep 300)
      (is (= 1 @(:requests collector))
          "queued logs do not start another POST after cancellation")
      (finally
        (.interrupt shutdown-thread)
        (stop-collector! collector)
        (.join shutdown-thread 2000)
        (try (sdk-logs/shutdown! provider) (catch Throwable _ nil))))))

(deftest stalled-otlp-metric-cancellation-remains-a-failed-shutdown
  (let [collector (start-stalled-collector "/v1/metrics")
        delegate (otlp/metric-exporter {:metrics-url (:url collector)
                                        :timeout-ms 10000
                                        :max-retries 0
                                        :environment? false})
        shutdowns (atom 0)
        exporter (->CountingMetricExporter delegate shutdowns)
        provider (sdk-metrics/meter-provider {:resource resource/empty-resource})
        meter (sdk-metrics/get-meter provider {:name "shutdown.metrics"})
        counter (metric-api/counter meter "requests")
        reader (sdk-metrics/periodic-reader provider exporter {:interval-ms 1})
        result (promise)
        shutdown-thread (Thread. #(deliver result (export/shutdown! reader)))]
    (try
      (metric-api/add! counter 1)
      (is (map? (deref (:first-request collector) 3000 ::not-read)))
      (.start shutdown-thread)
      (is (false? (deref result 3000 ::still-blocked))
          "the interrupted owned metric export remains a failed shutdown")
      (is (:worker-export-failed? @(:state reader)))
      (is (= 1 (:worker-interrupt-count @(:state reader))))
      (is (not (.isAlive (:worker reader))))
      (is (= 1 @shutdowns))
      (is (false? (export/shutdown! reader)))
      (is (= 1 @shutdowns) "metric exporter closes exactly once")
      (Thread/sleep 300)
      (is (= 1 @(:requests collector))
          "the interrupted metric POST is not replayed")
      (finally
        (.interrupt shutdown-thread)
        (stop-collector! collector)
        (.join shutdown-thread 2000)
        (try (export/shutdown! reader) (catch Throwable _ nil))
        (try (sdk-metrics/shutdown! provider) (catch Throwable _ nil))))))
