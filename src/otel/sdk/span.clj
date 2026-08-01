(ns otel.sdk.span
  "The recording span: the SDK's implementation of the Span protocol, and the
  immutable snapshot an exporter receives.

  A span is mutable for exactly as long as it is running — attributes, events and
  status accumulate — and becomes an immutable value the moment it ends. That
  transition is the important one: `end!` is what hands the span to the
  processors, and everything after it is ignored rather than being a race or an
  error. Instrumentation that keeps a reference to a finished span and sets one
  more attribute is common, and must be harmless.

  Mutable state lives in a single atom, so every update is atomic and the
  snapshot taken at `end!` is internally consistent."
  (:require [otel.attributes :as attr]
            [otel.sdk.clock :as clock]
            [otel.sdk.export :as export]
            [otel.trace :as trace]))

(def default-limits
  "Per-span limits. The spec's defaults."
  {:attribute-count-limit 128
   :attribute-value-length-limit nil
   :event-count-limit 128
   :link-count-limit 128
   :attribute-per-event-count-limit 128
   :attribute-per-link-count-limit 128})

(defn span-limits
  "Fill in the default span limits around `opts`."
  [opts]
  (merge default-limits opts))

(defn- nested-attr-limits
  "Attribute limits for an event's or link's own attribute map."
  [lim count-key]
  (attr/limits {:count-limit (get lim count-key)
                :value-length-limit (:attribute-value-length-limit lim)}))

(defn- add-attributes
  "Merge `new-attrs` into the span state, honouring the attribute count limit and
  counting what the limit rejected. Overwriting an attribute that is already
  present never counts as a drop and never needs a free slot."
  [state new-attrs lim]
  (let [;; Normalized with no count limit first: the limit has to be applied
        ;; against what is ALREADY on the span, which attr/normalize cannot see.
        normalized (attr/normalize new-attrs
                                   (attr/limits {:count-limit Integer/MAX_VALUE
                                                 :value-length-limit (:attribute-value-length-limit lim)}))
        limit (:attribute-count-limit lim)]
    (reduce (fn [s [k v]]
              (if (or (contains? (:attributes s) k)
                      (< (count (:attributes s)) limit))
                (assoc-in s [:attributes k] v)
                (update s :dropped-attributes-count inc)))
            state
            normalized)))

(defn- next-status
  "Apply a status transition under the spec's precedence rules: :ok is final, and
  :unset never overrides an already-set status. A description is kept only for
  :error, the only status where a failure reason is meaningful."
  [current code description]
  (cond
    (= :ok (:code current)) current
    (= :unset code) current
    (= :ok code) {:code :ok :description nil}
    :else {:code code :description description}))

(defn- exception-attributes
  "The semantic-convention attributes describing a thrown value."
  [t]
  (let [msg (try (ex-message t) (catch :default _ nil))
        data (try (ex-data t) (catch :default _ nil))
        type-name (try (.getName (class t)) (catch :default _ (str t)))]
    (cond-> {:exception.type type-name}
      msg (assoc :exception.message msg)
      ;; ex-data is Clojure's structured error payload and usually the most useful
      ;; part of a failure. It is rendered rather than expanded: its shape is
      ;; arbitrary and attribute values have to be scalars.
      data (assoc :exception.data (pr-str data)))))

(defn span-data
  "The immutable snapshot of `span`'s state, as handed to processors and
  exporters. A plain map, not a record: exporters only read it, and this keeps
  the encoding layer independent of this namespace."
  [span]
  (let [s @(:state span)]
    {:name (:name s)
     :span-context (:span-context span)
     :parent-span-id (:parent-span-id span)
     :kind (:kind span)
     :scope (:scope span)
     :resource (:resource span)
     :start-time-unix-nano (:start-time-unix-nano span)
     :end-time-unix-nano (:end-time-unix-nano s)
     :attributes (:attributes s)
     :events (:events s)
     :links (:links s)
     :status (:status s)
     :dropped-attributes-count (:dropped-attributes-count s)
     :dropped-events-count (:dropped-events-count s)
     :dropped-links-count (:dropped-links-count s)
     :ended? (:ended? s)}))

(defrecord SdkSpan [span-context parent-span-id kind scope resource
                    start-time-unix-nano clock limits processor state]
  trace/Span
  (span-context-of [_] span-context)

  (recording? [_] (not (:ended? @state)))

  (set-attribute! [this k v] (trace/set-attributes! this {k v}))

  (set-attributes! [this attrs]
    (swap! state (fn [s] (if (:ended? s) s (add-attributes s attrs limits))))
    this)

  (add-event! [this nm] (trace/add-event! this nm {} nil))
  (add-event! [this nm attrs] (trace/add-event! this nm attrs nil))
  (add-event! [this nm attrs timestamp-nanos]
    (let [ts (or timestamp-nanos (clock/wall-nanos clock))
          evt {:name nm
               :timestamp-unix-nano ts
               :attributes (attr/normalize attrs (nested-attr-limits limits :attribute-per-event-count-limit))}]
      (swap! state
             (fn [s]
               (cond
                 (:ended? s) s
                 (>= (count (:events s)) (:event-count-limit limits))
                 (update s :dropped-events-count inc)
                 :else (update s :events conj evt)))))
    this)

  (add-link! [this linked] (trace/add-link! this linked {}))
  (add-link! [this linked attrs]
    (let [lnk {:span-context linked
               :attributes (attr/normalize attrs (nested-attr-limits limits :attribute-per-link-count-limit))}]
      (swap! state
             (fn [s]
               (cond
                 (:ended? s) s
                 (>= (count (:links s)) (:link-count-limit limits))
                 (update s :dropped-links-count inc)
                 :else (update s :links conj lnk)))))
    this)

  (set-status! [this code] (trace/set-status! this code nil))
  (set-status! [this code description]
    (swap! state (fn [s] (if (:ended? s) s (update s :status next-status code description))))
    this)

  (update-name! [this nm]
    (swap! state (fn [s] (if (:ended? s) s (assoc s :name nm))))
    this)

  (record-exception! [this t] (trace/record-exception! this t {}))
  (record-exception! [this t attrs]
    (trace/add-event! this "exception" (merge (exception-attributes t) attrs) nil))

  (end! [this] (trace/end! this nil))
  (end! [this timestamp-nanos]
    ;; The ended? flag flips inside the swap!, so two threads racing to end the
    ;; same span still produce exactly one on-end call.
    (let [ts (or timestamp-nanos (clock/wall-nanos clock))
          [old _] (swap-vals! state
                              (fn [s]
                                (if (:ended? s)
                                  s
                                  (assoc s :ended? true :end-time-unix-nano ts))))]
      (when-not (:ended? old)
        (export/on-end processor (span-data this))))
    nil))

(defn new-span
  "Build a started recording span. Called by the tracer; not part of the public
  API — instrumentation goes through `otel.trace/with-span`."
  [{:keys [span-context parent-span-id name kind scope resource start-time-unix-nano
           clock limits processor attributes links]}]
  (let [lim (span-limits limits)
        link-limit (:link-count-limit lim)
        kept-links (vec (take link-limit
                              (map (fn [l]
                                     {:span-context (:span-context l)
                                      :attributes (attr/normalize
                                                    (:attributes l)
                                                    (nested-attr-limits lim :attribute-per-link-count-limit))})
                                   links)))
        state (atom (-> {:name name
                         :attributes {}
                         :events []
                         :links kept-links
                         :status {:code :unset :description nil}
                         :end-time-unix-nano nil
                         :ended? false
                         :dropped-attributes-count 0
                         :dropped-events-count 0
                         :dropped-links-count (max 0 (- (count links) link-limit))}
                        (add-attributes attributes lim)))]
    (->SdkSpan span-context parent-span-id kind scope resource
               start-time-unix-nano clock lim processor state)))
