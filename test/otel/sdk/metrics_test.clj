(ns otel.sdk.metrics-test
  (:require [clojure.test :refer [deftest is testing]]
            [otel.instrument.runtime :as runtime]
            [otel.metrics :as api]
            [otel.resource :as res]
            [otel.sdk.clock :as clock]
            [otel.sdk.export :as export]
            [otel.sdk.metrics :as sdk]))

(defn- setup
  ([] (setup {}))
  ([opts]
   (let [provider (sdk/meter-provider (merge {:resource res/empty-resource
                                              :clock (clock/fake-clock {:wall 1000 :mono 0})}
                                             opts))]
     {:provider provider :meter (sdk/get-meter provider {:name "test"})})))

(defn- metric-named [provider nm]
  (->> (sdk/collect! provider) (mapcat :metrics) (filter #(= nm (:name %))) first))

(defn- point-for [metric attrs]
  (first (filter #(= attrs (:attributes %)) (:data-points metric))))

(defrecord BlockingMetricExporter [entered release calls]
  export/MetricExporter
  (export-metrics! [_ _ _]
    ;; Only the scheduled worker's first export blocks. A concurrent flush on
    ;; the old implementation would enter a second export and return early.
    (when (= 1 (swap! calls inc))
      (deliver entered true)
      @release)
    true)
  (shutdown-metric-exporter! [_] true))

;; --- counters ---------------------------------------------------------------

(deftest counter-sums-measurements
  (let [{:keys [provider meter]} (setup)
        c (api/counter meter "requests" {:unit "{request}" :description "served"})]
    (api/add! c 1)
    (api/add! c 4)
    (let [m (metric-named provider "requests")]
      (is (= :sum (:type m)))
      (is (:monotonic? m))
      (is (= "{request}" (:unit m)))
      (is (= 5 (:value (point-for m {})))))))

(deftest counter-separates-attribute-sets
  (let [{:keys [provider meter]} (setup)
        c (api/counter meter "requests")]
    (api/add! c 1 {:route "/a"})
    (api/add! c 2 {:route "/a"})
    (api/add! c 5 {:route "/b"})
    (let [m (metric-named provider "requests")]
      (is (= 2 (count (:data-points m))))
      (is (= 3 (:value (point-for m {"route" "/a"}))))
      (is (= 5 (:value (point-for m {"route" "/b"})))))))

(deftest counter-ignores-a-negative-add
  (testing "a monotonic counter that decreased would be unrepresentable downstream"
    (let [{:keys [provider meter]} (setup)
          c (api/counter meter "requests")]
      (api/add! c 5)
      (api/add! c -3)
      (is (= 5 (:value (point-for (metric-named provider "requests") {})))))))

(deftest up-down-counter-accepts-negatives
  (let [{:keys [provider meter]} (setup)
        c (api/up-down-counter meter "queue.depth")]
    (api/add-delta! c 5)
    (api/add-delta! c -2)
    (let [m (metric-named provider "queue.depth")]
      (is (= :sum (:type m)))
      (is (not (:monotonic? m)))
      (is (= 3 (:value (point-for m {})))))))

;; --- gauges -----------------------------------------------------------------

(deftest gauge-keeps-the-latest-value
  (let [{:keys [provider meter]} (setup)
        g (api/gauge meter "temperature")]
    (api/set-value! g 10)
    (api/set-value! g 20)
    (let [m (metric-named provider "temperature")]
      (is (= :gauge (:type m)))
      (is (= 20 (:value (point-for m {}))))
      (testing "a gauge point describes an instant, so it carries no start time"
        (is (nil? (:start-time-unix-nano (point-for m {}))))))))

;; --- histograms -------------------------------------------------------------

(deftest histogram-aggregates-a-distribution
  (let [{:keys [provider meter]} (setup)
        h (api/histogram meter "latency" {:unit "ms" :boundaries [10.0 100.0]})]
    (doseq [v [5 50 500 50]] (api/record! h v))
    (let [m (metric-named provider "latency")
          p (point-for m {})]
      (is (= :histogram (:type m)))
      (is (= [10.0 100.0] (:explicit-bounds m)))
      (is (= 4 (:count p)))
      (is (= 605.0 (:sum p)))
      (is (= 5.0 (:min p)))
      (is (= 500.0 (:max p)))
      (testing "buckets are (-inf,10], (10,100], (100,+inf)"
        (is (= [1 2 1] (:bucket-counts p)))))))

(deftest histogram-boundary-values-fall-in-the-lower-bucket
  (testing "buckets are closed on the upper bound, per the spec"
    (let [{:keys [provider meter]} (setup)
          h (api/histogram meter "h" {:boundaries [10.0]})]
      (api/record! h 10)
      (is (= [1 0] (:bucket-counts (point-for (metric-named provider "h") {})))))))

(deftest histogram-uses-default-boundaries
  (let [{:keys [provider meter]} (setup)
        h (api/histogram meter "h")]
    (api/record! h 1)
    (is (= sdk/default-boundaries (:explicit-bounds (metric-named provider "h"))))))

;; --- asynchronous instruments -----------------------------------------------

(deftest observable-gauge-reads-on-collection
  (let [{:keys [provider meter]} (setup)
        current (atom 7)]
    (api/observable-gauge meter "heap" (fn [obs] (api/observe! obs @current)))
    (is (= 7 (:value (point-for (metric-named provider "heap") {}))))
    (reset! current 9)
    (testing "the callback runs again on the next collection"
      (is (= 9 (:value (point-for (metric-named provider "heap") {})))))))

(deftest observable-counter-is-a-monotonic-sum
  (let [{:keys [provider meter]} (setup)]
    (api/observable-counter meter "total" (fn [obs] (api/observe! obs 42)))
    (let [m (metric-named provider "total")]
      (is (= :sum (:type m)))
      (is (:monotonic? m))
      (is (= 42 (:value (point-for m {})))))))

(deftest observable-can-report-several-attribute-sets
  (let [{:keys [provider meter]} (setup)]
    (api/observable-gauge meter "by-pool"
                          (fn [obs]
                            (api/observe! obs 1 {:pool "a"})
                            (api/observe! obs 2 {:pool "b"})))
    (let [m (metric-named provider "by-pool")]
      (is (= 1 (:value (point-for m {"pool" "a"}))))
      (is (= 2 (:value (point-for m {"pool" "b"})))))))

(deftest a-throwing-callback-does-not-break-collection
  (testing "one bad instrument must not take out every other metric"
    (let [{:keys [provider meter]} (setup)]
      (api/observable-gauge meter "bad" (fn [_] (throw (ex-info "boom" {}))))
      (api/observable-gauge meter "good" (fn [obs] (api/observe! obs 1)))
      (is (= 1 (:value (point-for (metric-named provider "good") {})))))))

(deftest an-attribute-set-that-stops-being-reported-disappears
  (let [{:keys [provider meter]} (setup)
        report-b? (atom true)]
    (api/observable-gauge meter "g"
                          (fn [obs]
                            (api/observe! obs 1 {:k "a"})
                            (when @report-b? (api/observe! obs 2 {:k "b"}))))
    (is (= 2 (count (:data-points (metric-named provider "g")))))
    (reset! report-b? false)
    (is (= 1 (count (:data-points (metric-named provider "g")))))))

;; --- temporality ------------------------------------------------------------

(deftest cumulative-temporality-keeps-running-totals
  (let [{:keys [provider meter]} (setup)
        c (api/counter meter "n")]
    (api/add! c 1)
    (is (= 1 (:value (point-for (metric-named provider "n") {}))))
    (api/add! c 1)
    (testing "the second collection reports the total, not the increment"
      (is (= 2 (:value (point-for (metric-named provider "n") {})))))))

(deftest delta-temporality-resets-after-collection
  (let [{:keys [provider meter]} (setup {:temporality :delta})
        c (api/counter meter "n")]
    (api/add! c 3)
    (is (= 3 (:value (point-for (metric-named provider "n") {}))))
    (api/add! c 2)
    (testing "the second collection reports only what happened since the first"
      (is (= 2 (:value (point-for (metric-named provider "n") {})))))))

(deftest async-instruments-stay-cumulative-under-delta
  (testing "an absolute reading has no delta to compute, so its temporality is
            cumulative regardless of configuration"
    (let [{:keys [provider meter]} (setup {:temporality :delta})]
      (api/observable-counter meter "total" (fn [obs] (api/observe! obs 42)))
      (is (= :cumulative (:temporality (metric-named provider "total"))))
      (is (= 42 (:value (point-for (metric-named provider "total") {}))))
      (is (= 42 (:value (point-for (metric-named provider "total") {})))))))

;; --- scoping ----------------------------------------------------------------

(deftest metrics-are-grouped-by-scope
  (let [provider (sdk/meter-provider {:resource res/empty-resource})
        m1 (sdk/get-meter provider {:name "lib-a"})
        m2 (sdk/get-meter provider {:name "lib-b"})]
    (api/add! (api/counter m1 "a") 1)
    (api/add! (api/counter m2 "b") 1)
    (let [collected (sdk/collect! provider)]
      (is (= #{"lib-a" "lib-b"} (set (map #(get-in % [:scope :name]) collected)))))))

;; --- periodic reader synchronization ---------------------------------------

(deftest metric-force-flush-waits-for-an-in-flight-periodic-export
  (let [{:keys [provider meter]} (setup)
        counter (api/counter meter "requests")
        entered (promise)
        release (promise)
        calls (atom 0)
        exporter (->BlockingMetricExporter entered release calls)
        reader (sdk/periodic-reader provider exporter {:interval-ms 1})
        flush-started (promise)
        flush-done (promise)
        flush-thread (Thread. (fn []
                                (deliver flush-started true)
                                (deliver flush-done (export/force-flush! reader))))]
    (try
      (api/add! counter 1)
      (is (= true (deref entered 2000 ::timeout))
          "the periodic reader must enter the exporter")
      (.start flush-thread)
      (is (= true (deref flush-started 2000 ::timeout)))
      (is (= ::timeout (deref flush-done 100 ::timeout))
          "force-flush must wait for periodic exporter I/O already in flight")
      (deliver release true)
      (is (= true (deref flush-done 2000 ::timeout)))
      (is (>= @calls 2)
          "the explicit flush collects after the scheduled export completes")
      (finally
        (deliver release true)
        (try (.join flush-thread 2000) (catch :default _ nil))
        (export/shutdown! reader)))))

;; --- no-op API --------------------------------------------------------------

(deftest the-api-works-without-an-sdk
  (testing "instrumentation must be safe to write before anyone configures metrics"
    (let [m api/noop-meter]
      (is (some? (api/add! (api/counter m "c") 1)))
      (is (some? (api/record! (api/histogram m "h") 1.0)))
      (is (some? (api/set-value! (api/gauge m "g") 1)))
      (is (some? (api/observable-gauge m "og" (fn [_] nil)))))))

;; --- Chez runtime instrumentation -------------------------------------------

(deftest runtime-instruments-read-the-chez-collector
  (let [{:keys [provider meter]} (setup)]
    (runtime/register! meter)
    (let [by-name (into {} (map (juxt :name identity) (mapcat :metrics (sdk/collect! provider))))]
      (testing "heap and reserved memory are gauges with live values"
        (is (= :gauge (:type (get by-name "process.runtime.jolt.memory.heap"))))
        (is (pos? (:value (point-for (get by-name "process.runtime.jolt.memory.heap") {}))))
        (is (pos? (:value (point-for (get by-name "process.runtime.jolt.memory.reserved") {})))))
      (testing "collection totals are monotonic counters, so a backend can rate them"
        (let [gc (get by-name "process.runtime.jolt.gc.count")]
          (is (= :sum (:type gc)))
          (is (:monotonic? gc))
          (is (>= (:value (point-for gc {})) 0))))
      (testing "durations are reported in seconds, per the OTel conventions"
        (is (= "s" (:unit (get by-name "process.runtime.jolt.gc.duration"))))
        (is (>= (:value (point-for (get by-name "process.runtime.jolt.cpu.time") {})) 0.0)))
      (testing "cpu count comes from the host"
        (is (pos? (:value (point-for (get by-name "system.cpu.logical.count") {}))))))))

;; Not "allocating makes the number go up": the gauge reports bytes live on the
;; Chez heap, so a collection between two reads can leave it lower than it
;; started no matter what the test retains. What the gauge owes us is that each
;; read reflects the host counter at that moment rather than a cached constant.
(deftest runtime-heap-gauge-tracks-real-allocation
  (let [{:keys [provider meter]} (setup)
        read-gauge #(:value (point-for (metric-named provider "process.runtime.jolt.memory.heap") {}))
        ;; reading the gauge allocates, so allow a slack rather than equality
        tracks? (fn [gauge host] (< (abs (- gauge host)) (max 2000000 (* 0.05 host))))]
    (runtime/register! meter)
    (let [host-1 (jolt.host/bytes-allocated)
          gauge-1 (read-gauge)
          keep (into [] (map #(str "padpadpadpad" %) (range 100000)))
          host-2 (jolt.host/bytes-allocated)
          gauge-2 (read-gauge)]
      (is (= 100000 (count keep)))
      (is (pos? gauge-1))
      (is (tracks? gauge-1 host-1) "the gauge must report the host counter, not a constant")
      (is (tracks? gauge-2 host-2) "and must re-read it on every collection"))))
