(ns otel.sdk.metrics-test
  (:require [clojure.test :refer [deftest is testing]]
            [otel.any-value :as any]
            [otel.attributes :as attributes]
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

(deftest number-data-point-admission-guards-sync-gauges-before-normalization
  (let [{:keys [provider meter]} (setup)
        gauge (api/gauge meter "number-data-point")
        calls (atom 0)
        normalize attributes/normalize-result
        valid [any/min-int64 any/max-int64 9007199254740993 4.9e-324]
        invalid [nil false "not-a-number" {:private "fixture"}
                 ##NaN ##Inf ##-Inf
                 (dec any/min-int64) (inc any/max-int64)]]
    ;; Keep distinct attribute sets so every admitted wire arm is observable.
    (doseq [[i v] (map-indexed vector valid)]
      (is (identical? gauge (api/set-value! gauge v {:series i}))))
    (let [before @(:state gauge)]
      (with-redefs [attributes/normalize-result
                    (fn [& args]
                      (swap! calls inc)
                      (apply normalize args))]
        (doseq [v invalid]
          (is (identical? gauge
                          (api/set-value! gauge v {:series "must-not-normalize"}))))
        (is (zero? @calls)))
      (is (= before @(:state gauge))))
    (let [points (:data-points (metric-named provider "number-data-point"))]
      (is (= (count valid) (count points)))
      (is (= (set valid) (set (map :value points))))
      (is (= 4.9e-324 (:value (point-for {:data-points points} {"series" 3})))))))

(deftest every-sdk-point-normalizes-and-counts-attributes-once
  (let [{:keys [provider meter]} (setup)
        counter (api/counter meter "requests")
        gauge (api/gauge meter "temperature")
        histogram (api/histogram meter "latency" {:boundaries [5.0]})
        calls (atom 0)
        normalize attributes/normalize-result]
    (with-redefs [attributes/normalize-result
                  (fn [& args]
                    (swap! calls inc)
                    (apply normalize args))]
      (api/add! counter 1 {:kept 1 :rejected nil})
      (api/set-value! gauge 2 {:kept 1 :rejected nil})
      (api/record! histogram 3 {:kept 1 :rejected nil})
      (is (= 3 @calls) "one normalization result per measurement")
      (doseq [metric (map #(metric-named provider %)
                          ["requests" "temperature" "latency"])
              :let [point (point-for metric {"kept" 1})]]
        (is (= 0 (:flags point)))
        (is (contains? point :flags))
        (is (= 1 (:dropped-attributes-count point)))))))

(deftest dropped-count-metadata-never-fragments-an-attribute-series
  (let [{:keys [provider meter]} (setup)
        counter (api/counter meter "requests")
        histogram (api/histogram meter "latency" {:boundaries [5.0]})
        observable (api/observable-gauge
                    meter "temperature"
                    (fn [observer]
                      (api/observe! observer 10 {:kept 1 :rejected nil})
                      (api/observe! observer 20
                                    {:kept 1 :rejected-a nil
                                     :rejected-b nil})))]
    (api/add! counter 2 {:kept 1 :rejected nil})
    (api/add! counter 3 {:kept 1 :rejected-a nil :rejected-b nil})
    (api/record! histogram 2 {:kept 1 :rejected nil})
    (api/record! histogram 4 {:kept 1 :rejected-a nil :rejected-b nil})
    (let [sum-points (:data-points (metric-named provider "requests"))
          histogram-points (:data-points (metric-named provider "latency"))
          async-points (:data-points (metric-named provider "temperature"))]
      (doseq [points [sum-points histogram-points async-points]]
        (is (= 1 (count points)))
        (is (= {"kept" 1} (:attributes (first points))))
        (is (= 2 (:dropped-attributes-count (first points)))))
      (is (= 5 (:value (first sum-points))))
      (is (= 2 (:count (first histogram-points))))
      (is (= 6.0 (:sum (first histogram-points))))
      (is (= 20 (:value (first async-points)))
          "duplicate async attributes retain last value without duplicate points"))))

(deftest invalid-synchronous-sum-measurements-do-not-touch-series-or-attributes
  (let [overflow (/ (reduce *' 1N (repeat 40 10000000000N)) 3)]
    (doseq [[constructor record] [[api/counter api/add!]
                                 [api/up-down-counter api/add-delta!]]
            v [##NaN ##Inf ##-Inf nil false "not-a-number" {:private "fixture"} overflow]]
      (let [{:keys [provider meter]} (setup)
            instrument (constructor meter "finite-sum")
            calls (atom 0)
            normalize attributes/normalize-result]
        (record instrument 5 {"series" "kept"})
        (let [before @(:state instrument)]
          (with-redefs [attributes/normalize-result
                        (fn [& args] (swap! calls inc) (apply normalize args))]
            (doseq [attrs [{"series" "kept"} {"series" "fresh"}]]
              (let [result (try {:returned (record instrument v attrs)}
                                (catch Throwable _ {:threw true}))]
                (is (not (:threw result)))
                (is (identical? instrument (:returned result)))))
            (is (zero? @calls)))
          (is (= before @(:state instrument))))
        (record instrument 7 {"series" "kept"})
        (let [m (metric-named provider "finite-sum")]
          (is (= 1 (count (:data-points m))))
          (is (= 12 (:value (point-for m {"series" "kept"})))))))))

(deftest negative-finite-counter-adds-retain-rejection-and-fluent-return
  (doseq [v [-1 -1/2 -1.5M]]
    (let [{:keys [meter]} (setup)
          counter (api/counter meter "negative-counter")
          calls (atom 0)
          normalize attributes/normalize-result]
      (api/add! counter 5 {"series" "kept"})
      (let [before @(:state counter)]
        (with-redefs [attributes/normalize-result
                      (fn [& args] (swap! calls inc) (apply normalize args))]
          (doseq [attrs [{"series" "kept"} {"series" "fresh"}]]
            (is (identical? counter (api/add! counter v attrs))))
          (is (zero? @calls)))
        (is (= before @(:state counter)))))))

(deftest synchronous-sums-retain-existing-finite-number-domains
  (doseq [[constructor record signed?] [[api/counter api/add! false]
                                       [api/up-down-counter api/add-delta! true]]
          v (cond-> [0 9007199254740993 9223372036854775807
                     (reduce *' 1N (repeat 40 10000000000N))
                     1/2 1.5M 0.0 -0.0 4.9e-324 1.7976931348623157e308]
              signed? (conj -2 -1/2 -1.5M))]
    ;; Independent cells avoid finite aggregate overflow. Huge integers retain
    ;; the existing SDK arithmetic domain; their OTLP Int64 admission is separate.
    (let [{:keys [provider meter]} (setup)
          instrument (constructor meter "accepted-sum")]
      (is (identical? instrument (record instrument v)))
      (let [actual (:value (point-for (metric-named provider "accepted-sum") {}))]
        (if (integer? v)
          (do (is (integer? actual)) (is (= v actual)))
          (is (== v actual)))))))

(deftest rejected-synchronous-sum-inputs-preserve-temporality-and-sign-policy
  (doseq [temporality [:cumulative :delta]
          [constructor record delta] [[api/counter api/add! 2]
                                       [api/up-down-counter api/add-delta! -2]]]
    (let [{:keys [provider meter]} (setup {:temporality temporality})
          instrument (constructor meter "temporal-sum")]
      (record instrument 5)
      (doseq [bad [##NaN ##Inf ##-Inf]] (record instrument bad))
      (is (= 5 (:value (point-for (metric-named provider "temporal-sum") {}))))
      (record instrument delta)
      (is (= (if (= temporality :delta) delta (+ 5 delta))
             (:value (point-for (metric-named provider "temporal-sum") {})))))))

;; --- histograms -------------------------------------------------------------

(deftest invalid-histogram-measurements-do-not-touch-series-or-attributes
  (doseq [v [##NaN ##Inf ##-Inf nil false "not-a-number" {:private "fixture"}
             (reduce *' 1N (repeat 40 10000000000N))]]
    (let [{:keys [provider meter]} (setup)
          h (api/histogram meter "finite" {:boundaries [10]})
          calls (atom 0)
          normalize attributes/normalize-result]
      (api/record! h 5 {"series" "kept"})
      (let [before @(:state h)]
        (with-redefs [attributes/normalize-result
                      (fn [& args] (swap! calls inc) (apply normalize args))]
          (doseq [attrs [{"series" "kept"} {"series" "fresh"}]]
            (let [result (try {:returned (api/record! h v attrs)}
                              (catch Throwable _ {:threw true}))]
              (is (not (:threw result)))
              (is (identical? h (:returned result)))))
          (is (zero? @calls)))
        (is (= before @(:state h))))
      (api/record! h 7 {"series" "kept"})
      (let [m (metric-named provider "finite")
            p (point-for m {"series" "kept"})]
        (is (= 1 (count (:data-points m))))
        (is (= [2 0] (:bucket-counts p)))
        (is (= 2 (:count p)))
        (is (== 12.0 (:sum p)))
        (is (== 5.0 (:min p)))
        (is (== 7.0 (:max p)))))))

(deftest finite-histogram-measurements-retain-negative-zero-subnormal-and-endpoint
  (doseq [[v buckets] [[-1.0 [1 0]] [0.0 [1 0]] [-0.0 [1 0]]
                        [1/2 [1 0]] [1.5M [1 0]]
                        [4.9e-324 [1 0]] [10.0 [1 0]] [11.0 [0 1]]
                        [1.7976931348623157e308 [0 1]]]]
    ;; Separate cells keep finite-input sum overflow outside this control.
    (let [{:keys [provider meter]} (setup)
          h (api/histogram meter "accepted" {:boundaries [10]})]
      (is (identical? h (api/record! h v)))
      (let [p (point-for (metric-named provider "accepted") {})]
        (is (= 1 (:count p)))
        (is (= buckets (:bucket-counts p)))
        (is (== (double v) (:sum p) (:min p) (:max p)))))))

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

(deftest histogram-boundaries-canonicalize-before-registration
  (doseq [[supplied expected measurements buckets]
          [[[0 1] [0.0 1.0] [-1 0 0.5 1 2] [2 2 1]]
           [[(/ -3 2) (/ 1 2)] [-1.5 0.5] [-2 -1.5 0 0.5 1] [2 2 1]]
           [[(bigdec "-1.5") (bigdec "0.5")] [-1.5 0.5] [-2 -1.5 0 0.5 1] [2 2 1]]
           [[-2 (/ -1 2) (bigdec "0.5") 2.0] [-2.0 -0.5 0.5 2.0]
            [-3 -2 -1 0 1 2 3] [2 1 1 2 1]]
           [[-2.5 0.0 1.5] [-2.5 0.0 1.5] [-3 -2.5 0 1.5 2] [2 1 1 1]]
           [[1] [1.0] [0 1 2] [2 1]]
           [[] [] [-1 0 1] [3]]
           [nil sdk/default-boundaries [1] nil]]]
    (let [{:keys [provider meter]} (setup)
          h (api/histogram meter "canonical" {:boundaries supplied})]
      (doseq [v measurements] (api/record! h v))
      (let [m (metric-named provider "canonical") p (point-for m {})]
        (is (= expected (:explicit-bounds m)))
        (is (every? float? (:explicit-bounds m)))
        (is (= (inc (count expected)) (count (:bucket-counts p))))
        (is (= (count measurements) (:count p) (reduce + (:bucket-counts p))))
        (when buckets (is (= buckets (:bucket-counts p))))))))

(deftest invalid-histogram-boundaries-never-publish-an-instrument
  (doseq [[supplied reason]
          [[[1 1] :not-increasing] [[2 1] :not-increasing]
           [[0.0 -0.0] :not-increasing]
           [[9007199254740992 9007199254740993] :not-increasing]
           [[(bigdec "9007199254740992") (bigdec "9007199254740993")] :not-increasing]
           [[9007199254740992 (/ 18014398509481985N 2)] :not-increasing]
           [[##NaN] :nonfinite] [[##Inf] :nonfinite] [[##-Inf] :nonfinite]
           [[(reduce *' 1N (repeat 40 10000000000N))] :nonfinite]
           [[(bigdec "1e400")] :nonfinite]
           [[(/ (reduce *' 1N (repeat 40 10000000000N)) 3)] :nonfinite]
           [["secret-bearing-invalid-bound"] :invalid-type]
           [[nil] :invalid-type] [[false] :invalid-type]
           ["secret-bearing-invalid-envelope" :invalid-shape]
           [{} :invalid-shape]]]
    (let [{:keys [provider meter]} (setup)
          prior (api/histogram meter "prior" {:boundaries [1]})
          before @(:instruments meter)
          error (try (api/histogram meter "invalid" {:boundaries supplied}) nil
                     (catch Throwable error error))]
      (is (some? error))
      (is (= {:type :otel.sdk.metrics/invalid-histogram-boundaries :reason reason}
             (ex-data error)))
      (is (= "Invalid histogram boundaries" (ex-message error)))
      (is (nil? (ex-cause error)))
      (is (= (count before) (count @(:instruments meter))))
      (is (every? true? (map identical? before @(:instruments meter))))
      (api/record! prior 1)
      (is (= [1 0] (:bucket-counts (point-for (metric-named provider "prior") {})))))))

(deftest observable-gauge-reads-on-collection
  (let [{:keys [provider meter]} (setup)
        current (atom 7)]
    (api/observable-gauge meter "heap" (fn [obs] (api/observe! obs @current)))
    (is (= 7 (:value (point-for (metric-named provider "heap") {}))))
    (reset! current 9)
    (testing "the callback runs again on the next collection"
      (is (= 9 (:value (point-for (metric-named provider "heap") {})))))))

(deftest async-observer-number-data-point-admission-rejects-invalid-replacements
  (let [{:keys [provider meter]} (setup)
        phase (atom :valid)
        calls (atom 0)
        normalize attributes/normalize-result
        gauge (api/observable-gauge
               meter "async-number-data-point"
               (fn [observer]
                 (case @phase
                   :valid (api/observe! observer 9007199254740993 {:series "kept"})
                   ;; An invalid update after a valid observation in the same
                   ;; callback must not replace it or normalize its attributes.
                   :valid-then-invalid (do
                                         (api/observe! observer 4.9e-324 {:series "kept"})
                                         (api/observe! observer ##NaN {:series "kept"}))
                   ;; A later callback with only an invalid value must replace
                   ;; the old async snapshot with an empty one, not retain it.
                   :invalid-only (api/observe! observer (inc any/max-int64)
                                               {:series "must-not-normalize"}))))]
    (is (= 9007199254740993
           (:value (point-for (metric-named provider "async-number-data-point")
                              {"series" "kept"}))))
    (reset! phase :valid-then-invalid)
    (with-redefs [attributes/normalize-result
                  (fn [& args]
                    (swap! calls inc)
                    (apply normalize args))]
      (is (= 4.9e-324
             (:value (point-for (metric-named provider "async-number-data-point")
                                {"series" "kept"}))))
      (is (= 1 @calls)))
    (reset! phase :invalid-only)
    (reset! calls 0)
    (with-redefs [attributes/normalize-result
                  (fn [& args]
                    (swap! calls inc)
                    (apply normalize args))]
      (is (empty? (:data-points (metric-named provider "async-number-data-point"))))
      (is (zero? @calls)))))

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

(deftest meter-scope-attributes-use-the-shared-contract
  (let [provider (sdk/meter-provider {:resource res/empty-resource})
        meter (sdk/get-meter provider {:name "lib"
                                       :attributes {:mode :fast
                                                    :empty any/empty-value
                                                    :dropped nil}})]
    (api/add! (api/counter meter "requests") 1)
    (let [scope (:scope (first (sdk/collect! provider)))]
      (is (= {"empty" any/empty-value "mode" "fast"} (:attributes scope)))
      (is (= 1 (:dropped-attributes-count scope))))))

(deftest typed-meter-scopes-respect-provider-shutdown
  (let [provider (sdk/meter-provider {:resource res/empty-resource})
        meter (sdk/get-meter provider {:name "lib"
                                       :attributes {:attempts 42
                                                    :enabled false
                                                    :dropped nil}})]
    (api/add! (api/counter meter "requests") 1)
    (let [scope (:scope (first (sdk/collect! provider)))]
      (is (= {"attempts" 42 "enabled" false} (:attributes scope)))
      (is (= 1 (:dropped-attributes-count scope))))
    (is (true? (sdk/shutdown! provider)))
    (is (identical? api/noop-meter
                    (sdk/get-meter provider
                                   {:name "late"
                                    :attributes {:attempts 43}})))
    (is (= [] (sdk/collect! provider)))))

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
