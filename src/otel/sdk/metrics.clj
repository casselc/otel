(ns otel.sdk.metrics
  "The metrics SDK: instrument storage, aggregation, and collection.

  Each instrument keeps one aggregation cell per distinct attribute set. A
  collection walks every instrument, invokes the callbacks of the asynchronous
  ones, and snapshots every cell into the metric-data shape an exporter consumes.

  Temporality is cumulative by default, which is what the spec prescribes and what
  makes collection idempotent: a cell holds a running total since the instrument
  was created, so a dropped export loses resolution but never data — the next
  successful one carries the full total. Delta temporality resets each cell after
  it is read, which suits push-based backends that would rather add up the pieces
  themselves, and is what the spec recommends pairing with a monotonic counter on
  a system that restarts often."
  (:refer-clojure :exclude [count])
  (:require [otel.attributes :as attr]
            [otel.metrics :as api]
            [otel.resource :as res]
            [otel.sdk.clock :as clock]
            [otel.sdk.export :as export]
            [otel.sdk.lifecycle :as lifecycle]))

(def default-boundaries
  "The spec's default explicit bucket boundaries, in the instrument's unit."
  [0.0 5.0 10.0 25.0 50.0 75.0 100.0 250.0 500.0 750.0 1000.0 2500.0 5000.0 7500.0 10000.0])

;; --- aggregation cells ------------------------------------------------------

(defn- bucket-index
  "The bucket `v` falls in, given ascending boundaries. Bucket i counts values in
  (boundaries[i-1], boundaries[i]], and the last bucket is everything above."
  [boundaries v]
  (loop [i 0]
    (cond
      (>= i (clojure.core/count boundaries)) i
      (<= v (nth boundaries i)) i
      :else (recur (inc i)))))

(defn- empty-histogram [boundaries]
  {:count 0
   :sum 0.0
   :min nil
   :max nil
   :bucket-counts (vec (repeat (inc (clojure.core/count boundaries)) 0))})

(defn- record-histogram [cell boundaries v]
  (let [v (double v)
        i (bucket-index boundaries v)]
    (-> (or cell (empty-histogram boundaries))
        (update :count inc)
        (update :sum + v)
        (update :min (fn [m] (if (or (nil? m) (< v m)) v m)))
        (update :max (fn [m] (if (or (nil? m) (> v m)) v m)))
        (update-in [:bucket-counts i] inc))))

;; --- instruments ------------------------------------------------------------

;; Every instrument is the same shape: a descriptor plus an atom of
;; attribute-set -> cell. The kind decides how a measurement folds into a cell
;; and how a cell is later rendered as a metric point.
(defrecord SdkInstrument [kind name description unit boundaries monotonic? callback state clock]
  api/Counter
  (add! [this v] (api/add! this v {}))
  (add! [this v attrs]
    (when (neg? v)
      ;; The spec says a negative add to a monotonic counter is a caller error.
      ;; Ignoring it (loudly) beats corrupting the series or throwing into the
      ;; application, since a counter that goes down is unrepresentable downstream.
      (binding [*out* *err*]
        (println "otel: ignoring negative add to counter" name)))
    (when-not (neg? v)
      (swap! state update (attr/normalize attrs) (fnil + 0) v))
    this)

  api/UpDownCounter
  (add-delta! [this v] (api/add-delta! this v {}))
  (add-delta! [this v attrs]
    (swap! state update (attr/normalize attrs) (fnil + 0) v)
    this)

  api/Histogram
  (record! [this v] (api/record! this v {}))
  (record! [this v attrs]
    (swap! state update (attr/normalize attrs) record-histogram boundaries v)
    this)

  api/Gauge
  (set-value! [this v] (api/set-value! this v {}))
  (set-value! [this v attrs]
    (swap! state assoc (attr/normalize attrs) v)
    this))

(defrecord CollectingObserver [state]
  api/Observer
  (observe! [this v] (api/observe! this v {}))
  (observe! [this v attrs]
    (swap! state assoc (attr/normalize attrs) v)
    this))

(defn- observe-async!
  "Run an asynchronous instrument's callback, replacing its cells with what the
  callback reports. Replacing rather than merging is what makes an attribute set
  that stops being reported disappear from the output."
  [inst]
  (let [fresh (atom {})]
    (try
      ((:callback inst) (->CollectingObserver fresh))
      (catch :default e
        (binding [*out* *err*]
          (println "otel: metric callback for" (:name inst) "threw:" (ex-message e)))))
    (reset! (:state inst) @fresh)))

;; --- collection -------------------------------------------------------------

(defn- point-common [attrs start now]
  {:attributes attrs :start-time-unix-nano start :time-unix-nano now})

(defn- instrument->metric
  [inst start now temporality]
  (let [cells @(:state inst)
        base {:name (:name inst)
              :description (:description inst)
              :unit (:unit inst)}]
    (case (:kind inst)
      (:counter :observable-counter :up-down-counter :observable-up-down-counter)
      (assoc base
             :type :sum
             :monotonic? (:monotonic? inst)
             ;; An asynchronous instrument reports an absolute reading, so its
             ;; temporality is always cumulative regardless of configuration —
             ;; there is no delta to compute from a single observation.
             :temporality (if (:callback inst) :cumulative temporality)
             :data-points (mapv (fn [[attrs v]] (assoc (point-common attrs start now) :value v)) cells))

      (:gauge :observable-gauge)
      (assoc base
             :type :gauge
             :data-points (mapv (fn [[attrs v]]
                                  ;; A gauge point has no start time: it describes
                                  ;; an instant, not an interval.
                                  {:attributes attrs :time-unix-nano now :value v})
                                cells))

      :histogram
      (assoc base
             :type :histogram
             :temporality temporality
             :explicit-bounds (:boundaries inst)
             :data-points (mapv (fn [[attrs c]]
                                  (assoc (point-common attrs start now)
                                         :count (:count c)
                                         :sum (:sum c)
                                         :min (:min c)
                                         :max (:max c)
                                         :bucket-counts (:bucket-counts c)))
                                cells)))))

(defn- reset-for-delta! [inst]
  ;; Delta temporality reports what happened since the last collection, so the
  ;; cells start again from empty. Asynchronous instruments are left alone: they
  ;; are rebuilt from their callback on every collection anyway.
  (when-not (:callback inst)
    (reset! (:state inst) {})))

;; --- meter and provider -----------------------------------------------------

(defn- register!
  [meter kind nm {:keys [description unit boundaries]} callback]
  (let [inst (->SdkInstrument kind nm description unit
                              (or boundaries default-boundaries)
                              (contains? #{:counter :observable-counter} kind)
                              callback
                              (atom {})
                              (:clock meter))]
    (swap! (:instruments meter) conj inst)
    inst))

(defrecord SdkMeter [scope instruments clock]
  api/Meter
  (counter [m nm] (api/counter m nm {}))
  (counter [m nm opts] (register! m :counter nm opts nil))
  (up-down-counter [m nm] (api/up-down-counter m nm {}))
  (up-down-counter [m nm opts] (register! m :up-down-counter nm opts nil))
  (histogram [m nm] (api/histogram m nm {}))
  (histogram [m nm opts] (register! m :histogram nm opts nil))
  (gauge [m nm] (api/gauge m nm {}))
  (gauge [m nm opts] (register! m :gauge nm opts nil))
  (observable-counter [m nm cb] (api/observable-counter m nm cb {}))
  (observable-counter [m nm cb opts] (register! m :observable-counter nm opts cb))
  (observable-up-down-counter [m nm cb] (api/observable-up-down-counter m nm cb {}))
  (observable-up-down-counter [m nm cb opts] (register! m :observable-up-down-counter nm opts cb))
  (observable-gauge [m nm cb] (api/observable-gauge m nm cb {}))
  (observable-gauge [m nm cb opts] (register! m :observable-gauge nm opts cb)))

(defrecord SdkMeterProvider [resource meters clock start-time temporality state terminal])

(defn meter-provider
  "Build a meter provider.

  Options: :resource, :clock, and :temporality (:cumulative, the default, or
  :delta)."
  [{:keys [resource clock temporality]}]
  (let [c (clock/anchored (or clock clock/system))]
    (->SdkMeterProvider (or resource (res/default-resource))
                        (atom [])
                        c
                        (clock/wall-nanos c)
                        (or temporality :cumulative)
                        (atom {:shutdown? false})
                        (lifecycle/terminal-action))))

(defn get-meter
  "A meter for one instrumentation scope."
  [provider {:keys [name version schema-url]}]
  (locking (:state provider)
    (if (:shutdown? @(:state provider))
      api/noop-meter
      (let [m (->SdkMeter {:name name :version version :schema-url schema-url}
                          (atom [])
                          (:clock provider))]
        (swap! (:meters provider) conj m)
        m))))

(defn collect!
  "Snapshot every instrument in the provider as metric data.

  Returns a sequence of {:scope … :metrics […]}. Asynchronous instruments are
  read here — that is the whole point of them, and it means a callback runs on
  the collecting thread, at the reader's cadence, not on the application's hot
  path."
  [provider]
  (locking (:state provider)
    (if (:shutdown? @(:state provider))
      []
      (let [now (clock/wall-nanos (:clock provider))
            start (:start-time provider)
            temporality (:temporality provider)]
        (doall
          (for [meter @(:meters provider)
                :let [insts @(:instruments meter)]
                :when (seq insts)]
            {:scope (:scope meter)
             :metrics (doall
                        (for [inst insts]
                          (do
                            (when (:callback inst) (observe-async! inst))
                            (let [m (instrument->metric inst start now temporality)]
                              (when (= :delta temporality) (reset-for-delta! inst))
                              m))))}))))))

(defn shutdown!
  "Stop the provider. Collection after this returns nothing."
  [provider]
  (lifecycle/run-terminal!
    (:terminal provider)
    #(locking (:state provider)
       (swap! (:state provider) assoc :shutdown? true)
       (reset! (:meters provider) [])
       true)))

;; --- periodic reader --------------------------------------------------------

(def default-reader-config
  {:interval-ms 60000})

(defn- collect-and-export!
  [provider exporter]
  (try
    (let [collected (collect! provider)]
      ;; An empty collection is skipped: the OTLP spec allows an empty envelope
      ;; but there is nothing to learn from one, and a collector counts it as a
      ;; request either way.
      (when (seq (mapcat :metrics collected))
        (export/export-metrics! exporter (:resource provider) collected)))
    (catch :default e
      (binding [*out* *err*]
        (println "otel: metric collection failed:" (ex-message e)))
      false)))

(defrecord PeriodicReader [provider exporter config state worker terminal]
  export/SpanProcessor
  ;; A reader is not a span processor; these arms exist only so a reader can be
  ;; shut down and flushed through the same calls a provider already makes.
  (on-start [_ _ _] nil)
  (on-end [_ _] nil)
  (force-flush! [_]
    ;; Interval collection and force-flush both mutate delta aggregation state.
    ;; Serialize the entire collect/export operation so a flush also waits for
    ;; exporter I/O already started by the worker.
    (locking state
      (if (:shutdown? @state)
        false
        (boolean (collect-and-export! provider exporter)))))
  (shutdown! [this]
    (lifecycle/run-terminal!
      terminal
      (fn []
        ;; Serialize the acceptance boundary with both scheduled and explicit
        ;; collections. The final collection covers everything recorded before
        ;; shutdown; later force-flush calls are rejected.
        (locking state
          (swap! state assoc :shutdown? true)
          (collect-and-export! provider exporter))
        (when worker (try (.join worker 5000) (catch :default _ nil)))
        (export/shutdown-metric-exporter! exporter)))))

(defn periodic-reader
  "Collect every instrument on an interval and hand the result to `exporter`.

  Options: :interval-ms (default 60000). Metric collection is a poll, not a
  stream, so this interval is the resolution of every series the provider
  produces — including the runtime instruments, whose callbacks only run here."
  ([provider exporter] (periodic-reader provider exporter {}))
  ([provider exporter opts]
   (let [config (merge default-reader-config opts)
         state (atom {:shutdown? false})
         worker (Thread.
                  (fn []
                    (loop []
                      ;; Sliced so shutdown is prompt: a 60s interval slept in one
                      ;; call would make every process exit take up to a minute.
                      (loop [remaining (:interval-ms config)]
                        (when (and (pos? remaining) (not (:shutdown? @state)))
                          (let [slice (min 50 remaining)]
                            (Thread/sleep slice)
                            (recur (- remaining slice)))))
                      (when-not (:shutdown? @state)
                        (locking state
                          (when-not (:shutdown? @state)
                            (collect-and-export! provider exporter)))
                        (recur)))))]
     (.setDaemon worker true)
     (.start worker)
     (->PeriodicReader provider exporter config state worker
                       (lifecycle/terminal-action)))))
