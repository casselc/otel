(ns otel.otlp.signal-decode
  "Strict OTLP/JSON decoders for the SDK's canonical log and metric models."
  (:require [clojure.string :as str]
            [otel.resource :as resource]))

(def ^:private absent (Object.))
(defn- field [m k]
  (let [s (name k)]
    (cond (and (map? m) (contains? m s)) (get m s)
          (and (map? m) (contains? m k)) (get m k)
          :else absent)))
(defn- present? [v] (not (identical? absent v)))
(defn- invalid! [signal path reason expected actual]
  (throw (ex-info (str "invalid OTLP " (name signal) " value at " (pr-str path))
                  {:type ::invalid-value :signal signal :path path :reason reason
                   :expected expected :actual actual})))
(defn- object! [signal v path]
  (if (map? v) v (invalid! signal path :wrong-type "object" v)))
(defn- array! [signal v path]
  (if (vector? v) v (invalid! signal path :wrong-type "array" v)))
(defn- string! [signal v path]
  (if (string? v) v (invalid! signal path :wrong-type "string" v)))
(defn- optional-string! [signal m k path default]
  (let [v (field m k)]
    (if (present? v) (string! signal v (conj path (name k))) default)))
(defn- decimal! [signal v path minimum maximum]
  (let [n (cond
            (integer? v) v
            (and (string? v) (re-matches #"-?[0-9]+" v))
            (let [negative? (str/starts-with? v "-")
                  digits (if negative? (subs v 1) v)
                  magnitude (reduce (fn [n c] (+ (* n 10) (- (int c) (int \0))))
                                    0 digits)]
              (if negative? (- magnitude) magnitude))
            :else nil)]
    (if (and (integer? n) (<= minimum n maximum))
      n
      (invalid! signal path :invalid-integer (str minimum ".." maximum) v))))
(defn- uint64! [s v p] (decimal! s v p 0 18446744073709551615))
(defn- uint32! [s v p] (decimal! s v p 0 4294967295))
(defn- finite-number! [signal v path]
  (let [d (when (or (integer? v) (float? v)) (double v))]
    (if (and d (= d d) (not= d ##Inf) (not= d ##-Inf)) d
        (invalid! signal path :wrong-type "finite JSON number" v))))
(defn- hex-id! [signal v width path]
  (let [text (string! signal v path)]
    (if (or (= "" text)
            (and (= width (count text)) (re-matches #"[0-9a-f]+" text)
                 (not (every? #(= % \0) text))))
      (when-not (= "" text) text)
      (invalid! signal path :invalid-id
                (str "empty or " width " lowercase hexadecimal characters") text))))

(declare any-value!)
(defn- any-value! [signal v path]
  (let [m (object! signal v path)
        arms (filter #(present? (field m %))
                     [:stringValue :boolValue :intValue :doubleValue
                      :arrayValue :kvlistValue :bytesValue])]
    (when-not (= 1 (count arms))
      (invalid! signal path :invalid-any-value "exactly one supported AnyValue field" m))
    (let [arm (first arms) x (field m arm) p (conj path (name arm))]
      (case arm
        :stringValue (string! signal x p)
        :boolValue (if (or (true? x) (false? x)) x
                       (invalid! signal p :wrong-type "boolean" x))
        :intValue (decimal! signal x p -9223372036854775808 9223372036854775807)
        :doubleValue (finite-number! signal x p)
        :arrayValue
        (let [a (object! signal x p)
              vv (field a :values)
              xs (if (present? vv) (array! signal vv (conj p "values")) [])
              values (mapv #(any-value! signal %1 (conj p "values" %2))
                           xs (range (count xs)))
              kinds (set (map #(cond (string? %) :string (boolean? %) :bool
                                     (integer? %) :int (float? %) :double :else :other)
                              values))]
          (if (<= (count kinds) 1) values
              (invalid! signal p :heterogeneous-array "one scalar type" values)))
        :kvlistValue (invalid! signal p :unsupported-any-value "scalar or scalar array" x)
        :bytesValue (invalid! signal p :unsupported-any-value "scalar or scalar array" x)))))

(defn- attributes! [signal v path]
  (let [xs (if (present? v) (array! signal v path) [])]
    (reduce (fn [out i]
              (let [p (conj path i) kv (object! signal (nth xs i) p)
                    k (field kv :key) av (field kv :value)
                    key (if (present? k) (string! signal k (conj p "key"))
                            (invalid! signal (conj p "key") :missing-field "string" nil))]
                (when (contains? out key)
                  (invalid! signal (conj p "key") :duplicate-attribute-key "unique key" key))
                (when-not (present? av)
                  (invalid! signal (conj p "value") :missing-field "AnyValue" nil))
                (assoc out key (any-value! signal av (conj p "value")))))
            {} (range (count xs)))))
(defn- count-field! [signal m k path]
  (let [v (field m k)]
    (if (present? v) (uint32! signal v (conj path (name k))) 0)))
(defn- resource! [signal v schema-url path]
  (let [m (if (present? v) (object! signal v path) {})]
    (assoc (resource/resource (attributes! signal (field m :attributes)
                                           (conj path "attributes"))
                              {:schema-url schema-url})
           :dropped-attributes-count (count-field! signal m :droppedAttributesCount path))))
(defn- scope! [signal v schema-url path]
  (let [m (if (present? v) (object! signal v path) {})]
    {:name (optional-string! signal m :name path "")
     :version (optional-string! signal m :version path nil)
     :schema-url schema-url
     :attributes (attributes! signal (field m :attributes) (conj path "attributes"))
     :dropped-attributes-count (count-field! signal m :droppedAttributesCount path)}))
(defn- error-map [signal e]
  (assoc (or (ex-data e) {:type ::invalid-value :signal signal :path []
                           :reason :decode-failure})
         :message (or (ex-message e) (str e))))

;; --- logs -------------------------------------------------------------------

(defn- log-record! [v path r scope]
  (let [m (object! :logs v path)
        body-v (field m :body)
        time-v (field m :timeUnixNano)
        observed-v (field m :observedTimeUnixNano)
        trace-v (field m :traceId) span-v (field m :spanId) flags-v (field m :flags)]
    {:body (if (present? body-v) (any-value! :logs body-v (conj path "body")) "")
     :severity-number (let [v (field m :severityNumber)]
                        (if (present? v) (uint32! :logs v (conj path "severityNumber")) 0))
     :severity-text (optional-string! :logs m :severityText path nil)
     :timestamp-unix-nano (when (present? time-v)
                            (uint64! :logs time-v (conj path "timeUnixNano")))
     :observed-time-unix-nano (if (present? observed-v)
                                (uint64! :logs observed-v (conj path "observedTimeUnixNano")) 0)
     :attributes (attributes! :logs (field m :attributes) (conj path "attributes"))
     :dropped-attributes-count (count-field! :logs m :droppedAttributesCount path)
     :trace-id (when (present? trace-v) (hex-id! :logs trace-v 32 (conj path "traceId")))
     :span-id (when (present? span-v) (hex-id! :logs span-v 16 (conj path "spanId")))
     :trace-flags (if (present? flags-v) (uint32! :logs flags-v (conj path "flags")) 0)
     :event-name (optional-string! :logs m :eventName path nil)
     :resource r :scope scope}))

(defn- raw-record-count [scopes]
  (if (vector? scopes)
    (reduce (fn [n sl] (let [xs (field sl :logRecords)]
                         (+ n (if (vector? xs) (count xs) 0)))) 0 scopes) 0))

(defn decode-logs [request]
  (try
    (let [m (object! :logs request []) rv (field m :resourceLogs)
          resources (if (present? rv) (array! :logs rv ["resourceLogs"]) [])]
      (reduce
       (fn [acc ri]
         (let [path ["resourceLogs" ri] raw (nth resources ri)]
           (try
             (let [rm (object! :logs raw path)
                   schema (optional-string! :logs rm :schemaUrl path nil)
                   r (resource! :logs (field rm :resource) schema (conj path "resource"))
                   sv (field rm :scopeLogs)
                   scopes (if (present? sv) (array! :logs sv (conj path "scopeLogs")) [])]
               (reduce
                (fn [a si]
                  (let [sp (conj path "scopeLogs" si) raw-scope (nth scopes si)]
                    (try
                      (let [sm (object! :logs raw-scope sp)
                            ss (optional-string! :logs sm :schemaUrl sp nil)
                            scope (scope! :logs (field sm :scope) ss (conj sp "scope"))
                            lv (field sm :logRecords)
                            records (if (present? lv) (array! :logs lv (conj sp "logRecords")) [])]
                        (reduce (fn [x li]
                                  (try (update x :records conj
                                               (log-record! (nth records li)
                                                            (conj sp "logRecords" li) r scope))
                                       (catch :default e (-> x (update :rejected-log-records inc)
                                                               (update :errors conj (error-map :logs e))))))
                                a (range (count records))))
                      (catch :default e
                        (let [records (field raw-scope :logRecords)]
                          (-> a (update :rejected-log-records +
                                        (if (vector? records) (count records) 0))
                              (update :errors conj (error-map :logs e))))))))
                acc (range (count scopes))))
             (catch :default e
               (let [scopes (field raw :scopeLogs)]
                 (-> acc (update :rejected-log-records + (raw-record-count scopes))
                     (update :errors conj (error-map :logs e))))))))
       {:records [] :rejected-log-records 0 :errors []} (range (count resources))))
    (catch :default e {:records [] :rejected-log-records 0
                       :errors [(error-map :logs e)]})))

;; --- metrics ----------------------------------------------------------------

(defn- reject-unmodeled-point-fields! [m path]
  (let [flags (field m :flags)
        dropped (field m :droppedAttributesCount)
        exemplars (field m :exemplars)]
    (when (and (present? flags) (not (zero? (uint32! :metrics flags (conj path "flags")))))
      (invalid! :metrics (conj path "flags") :unsupported-point-field
                "zero flags (the SDK model has no point flags)" flags))
    (when (and (present? dropped)
               (pos? (uint32! :metrics dropped (conj path "droppedAttributesCount"))))
      (invalid! :metrics (conj path "droppedAttributesCount") :unsupported-point-field
                "zero dropped attributes" dropped))
    (when (present? exemplars)
      (let [xs (array! :metrics exemplars (conj path "exemplars"))]
        (when (seq xs)
          (invalid! :metrics (conj path "exemplars") :unsupported-point-field
                    "no exemplars (not modeled by this SDK)" xs))))))

(defn- number-point! [v path]
  (let [m (object! :metrics v path) iv (field m :asInt) dv (field m :asDouble)
        arms (count (filter present? [iv dv]))]
    (reject-unmodeled-point-fields! m path)
    (when-not (= 1 arms)
      (invalid! :metrics path :invalid-number-point "exactly one of asInt/asDouble" m))
    {:attributes (attributes! :metrics (field m :attributes) (conj path "attributes"))
     :start-time-unix-nano (let [x (field m :startTimeUnixNano)]
                             (when (present? x) (uint64! :metrics x (conj path "startTimeUnixNano"))))
     :time-unix-nano (let [x (field m :timeUnixNano)]
                       (if (present? x) (uint64! :metrics x (conj path "timeUnixNano")) 0))
     :value (if (present? iv)
              (decimal! :metrics iv (conj path "asInt") -9223372036854775808 9223372036854775807)
              (finite-number! :metrics dv (conj path "asDouble")))}))
(defn- number-array! [v path]
  (let [xs (array! :metrics v path)]
    (mapv #(finite-number! :metrics %1 (conj path %2)) xs (range (count xs)))))
(defn- uint-array! [v path]
  (let [xs (array! :metrics v path)]
    (mapv #(uint64! :metrics %1 (conj path %2)) xs (range (count xs)))))
(defn- histogram-point! [v path]
  (let [m (object! :metrics v path)
        bounds-v (field m :explicitBounds) buckets-v (field m :bucketCounts)
        bounds (if (present? bounds-v) (number-array! bounds-v (conj path "explicitBounds")) [])
        buckets (if (present? buckets-v) (uint-array! buckets-v (conj path "bucketCounts")) [])
        count-v (field m :count) sum-v (field m :sum)]
    (reject-unmodeled-point-fields! m path)
    (when-not (= (inc (count bounds)) (count buckets))
      (invalid! :metrics path :invalid-histogram-buckets
                "bucketCounts width = explicitBounds width + 1" buckets))
    (when-not (or (empty? bounds) (apply < bounds))
      (invalid! :metrics (conj path "explicitBounds") :invalid-histogram-bounds
                "strictly increasing bounds" bounds))
    (when-not (present? sum-v)
      (invalid! :metrics (conj path "sum") :unsupported-histogram-shape
                "histogram sum required by the SDK model" nil))
    (let [count (if (present? count-v)
                  (uint64! :metrics count-v (conj path "count")) 0)]
      (when-not (= count (reduce + 0 buckets))
        (invalid! :metrics path :invalid-histogram-count
                  "count equal to the sum of bucketCounts" count)))
    {:attributes (attributes! :metrics (field m :attributes) (conj path "attributes"))
     :start-time-unix-nano (let [x (field m :startTimeUnixNano)]
                             (if (present? x) (uint64! :metrics x (conj path "startTimeUnixNano")) 0))
     :time-unix-nano (let [x (field m :timeUnixNano)]
                       (if (present? x) (uint64! :metrics x (conj path "timeUnixNano")) 0))
     :count (if (present? count-v) (uint64! :metrics count-v (conj path "count")) 0)
     :sum (finite-number! :metrics sum-v (conj path "sum"))
     :min (let [x (field m :min)] (when (present? x) (finite-number! :metrics x (conj path "min"))))
     :max (let [x (field m :max)] (when (present? x) (finite-number! :metrics x (conj path "max"))))
     :bucket-counts buckets :explicit-bounds bounds}))
(def ^:private temporalities {1 :delta 2 :cumulative
                               "AGGREGATION_TEMPORALITY_DELTA" :delta
                               "AGGREGATION_TEMPORALITY_CUMULATIVE" :cumulative})
(defn- metric-point-count [m]
  (reduce + 0 (for [k [:gauge :sum :histogram :exponentialHistogram :summary]
                    :let [kind (field m k)] :when (map? kind)
                    :let [points (field kind :dataPoints)]]
                (if (vector? points) (count points) 0))))
(defn- raw-metric-count [scopes]
  (if (vector? scopes)
    (reduce (fn [n scope]
              (let [metrics (field scope :metrics)]
                (+ n (if (vector? metrics)
                       (reduce + 0 (map metric-point-count metrics)) 0))))
            0 scopes)
    0))
(defn- metric! [v path]
  (let [m (object! :metrics v path) kinds (filter #(present? (field m %))
                                                   [:gauge :sum :histogram
                                                    :exponentialHistogram :summary])]
    (when-not (= 1 (count kinds))
      (invalid! :metrics path :invalid-metric-kind "exactly one metric data kind" m))
    (let [kind (first kinds)]
      (when (contains? #{:exponentialHistogram :summary} kind)
        (invalid! :metrics (conj path (name kind)) :unsupported-metric-kind
                  "gauge, sum, or explicit histogram" kind))
      (let [km (object! :metrics (field m kind) (conj path (name kind)))
            pv (field km :dataPoints)
            points (if (present? pv) (array! :metrics pv (conj path (name kind) "dataPoints")) [])
            temporality (when (contains? #{:sum :histogram} kind)
                          (let [wire (field km :aggregationTemporality)]
                            (or (get temporalities wire)
                                (invalid! :metrics (conj path (name kind) "aggregationTemporality")
                                          :invalid-enum "delta or cumulative" wire))))
            decode-point (if (= :histogram kind) histogram-point! number-point!)
            decoded (reduce (fn [acc i]
                              (try (update acc :points conj
                                           (decode-point (nth points i)
                                                         (conj path (name kind) "dataPoints" i)))
                                   (catch :default e (-> acc (update :rejected inc)
                                                           (update :errors conj (error-map :metrics e))))))
                            {:points [] :rejected 0 :errors []} (range (count points)))
            histogram-bounds (when (= :histogram kind)
                               (set (map :explicit-bounds (:points decoded))))]
        (when (and histogram-bounds (> (count histogram-bounds) 1))
          (invalid! :metrics (conj path "histogram" "dataPoints")
                    :unsupported-histogram-shape
                    "one shared explicit-bound vector per SDK metric" histogram-bounds))
        {:metric (cond-> {:name (optional-string! :metrics m :name path "")
                          :description (optional-string! :metrics m :description path nil)
                          :unit (optional-string! :metrics m :unit path nil)
                          :type kind
                          :data-points (if (= :histogram kind)
                                         (mapv #(dissoc % :explicit-bounds) (:points decoded))
                                         (:points decoded))}
                   temporality (assoc :temporality temporality)
                   (= kind :sum) (assoc :monotonic? (let [x (field km :isMonotonic)]
                                                      (if (present? x)
                                                        (if (boolean? x) x
                                                            (invalid! :metrics (conj path "sum" "isMonotonic")
                                                                      :wrong-type "boolean" x)) false)))
                   (= kind :histogram) (assoc :explicit-bounds
                                              (or (:explicit-bounds (first (:points decoded))) [])))
         :rejected (:rejected decoded) :errors (:errors decoded)}))))

(defn decode-metrics [request]
  (try
    (let [m (object! :metrics request []) rv (field m :resourceMetrics)
          resources (if (present? rv) (array! :metrics rv ["resourceMetrics"]) [])]
      (reduce
       (fn [acc ri]
         (let [path ["resourceMetrics" ri] raw (nth resources ri)]
           (try
             (let [rm (object! :metrics raw path)
                   schema (optional-string! :metrics rm :schemaUrl path nil)
                   r (resource! :metrics (field rm :resource) schema (conj path "resource"))
                   sv (field rm :scopeMetrics)
                   scopes (if (present? sv) (array! :metrics sv (conj path "scopeMetrics")) [])
                   decoded
                   (reduce
                    (fn [out si]
                      (let [sp (conj path "scopeMetrics" si) raw-scope (nth scopes si)]
                        (try
                          (let [sm (object! :metrics raw-scope sp)
                                ss (optional-string! :metrics sm :schemaUrl sp nil)
                                scope (scope! :metrics (field sm :scope) ss (conj sp "scope"))
                                mv (field sm :metrics)
                                metrics (if (present? mv) (array! :metrics mv (conj sp "metrics")) [])
                                one (reduce
                                     (fn [x mi]
                                       (let [mp (conj sp "metrics" mi) raw-metric (nth metrics mi)]
                                         (try
                                           (let [{:keys [metric rejected errors]} (metric! raw-metric mp)]
                                             (cond-> (-> x (update :rejected + rejected)
                                                        (update :errors into errors))
                                               (seq (:data-points metric))
                                               (update :metrics conj metric)))
                                           (catch :default e
                                             (-> x (update :rejected + (metric-point-count raw-metric))
                                                 (update :errors conj (error-map :metrics e)))))))
                                     {:metrics [] :rejected 0 :errors []} (range (count metrics)))]
                            (-> out
                                (update :collected conj {:scope scope :metrics (:metrics one)})
                                (update :rejected + (:rejected one))
                                (update :errors into (:errors one))))
                          (catch :default e
                            (let [metrics (field raw-scope :metrics)]
                              (-> out
                                  (update :rejected + (if (vector? metrics)
                                                        (reduce + (map metric-point-count metrics)) 0))
                                  (update :errors conj (error-map :metrics e))))))))
                    {:collected [] :rejected 0 :errors []} (range (count scopes)))]
               (-> acc
                   (update :collections conj {:resource r :collected (:collected decoded)})
                   (update :rejected-data-points + (:rejected decoded))
                   (update :errors into (:errors decoded))))
             (catch :default e
               (let [scopes (field raw :scopeMetrics)]
                 (-> acc
                     (update :rejected-data-points + (raw-metric-count scopes))
                     (update :errors conj (error-map :metrics e))))))))
       {:collections [] :rejected-data-points 0 :errors []} (range (count resources))))
    (catch :default e {:collections [] :rejected-data-points 0
                       :errors [(error-map :metrics e)]})))
