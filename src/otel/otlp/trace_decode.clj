(ns otel.otlp.trace-decode
  "Strict, transport-neutral decoding of OTLP/JSON trace request values.

  `decode-request` accepts the JSON-compatible map produced by a server's JSON
  parser. It deliberately does not parse bytes or strings: HTTP body limits,
  content type, authentication and JSON parser selection belong to the receiver
  layer. Accepted spans use the same immutable ended-span map consumed by this
  SDK's exporters."
  (:require [clojure.string :as str]
            [otel.resource :as resource]))

(def ^:private absent (Object.))

(defn- field [m k]
  (let [s (name k)]
    (cond
      (and (map? m) (contains? m s)) (get m s)
      (and (map? m) (contains? m k)) (get m k)
      :else absent)))

(defn- present? [v] (not (identical? absent v)))

(defn- invalid! [path reason expected actual]
  (throw (ex-info (str "invalid OTLP trace value at " (pr-str path))
                  {:type :otel.otlp.trace-decode/invalid-value
                   :path path
                   :reason reason
                   :expected expected
                   :actual actual})))

(defn- object! [v path]
  (if (map? v) v (invalid! path :wrong-type "object" v)))

(defn- array! [v path]
  (if (vector? v) v (invalid! path :wrong-type "array" v)))

(defn- string! [v path]
  (if (string? v) v (invalid! path :wrong-type "string" v)))

(defn- optional-string! [m k path default]
  (let [v (field m k)]
    (if (present? v) (string! v (conj path (name k))) default)))

(defn- decimal-integer! [v path minimum maximum]
  (let [n (cond
            (integer? v) v
            (and (string? v) (re-matches #"-?[0-9]+" v))
            (let [negative? (str/starts-with? v "-")
                  digits (if negative? (subs v 1) v)
                  magnitude (reduce (fn [n c]
                                      (+ (* n 10) (- (int c) (int \0))))
                                    0 digits)]
              (if negative? (- magnitude) magnitude))
            :else (invalid! path :invalid-integer "decimal integer string or integer" v))]
    (if (<= minimum n maximum)
      n
      (invalid! path :out-of-range (str minimum ".." maximum) v))))

(defn- uint64! [v path]
  (decimal-integer! v path 0 18446744073709551615))

(defn- uint32! [v path]
  (decimal-integer! v path 0 4294967295))

(defn- int64! [v path]
  (decimal-integer! v path -9223372036854775808 9223372036854775807))

(defn- hex-id! [v n path allow-empty?]
  (let [s (string! v path)]
    (cond
      (and allow-empty? (empty? s)) nil
      (and (= n (count s)) (re-matches #"[0-9a-f]+" s)
           (not (every? #(= % \0) s))) s
      :else (invalid! path :invalid-id
                      (str n " lowercase hexadecimal characters, not all zero") s))))

(defn- value-kind [v]
  (cond
    (string? v) :string
    (or (true? v) (false? v)) :bool
    (integer? v) :int
    (float? v) :double))

(declare any-value!)

(defn- array-value! [m path]
  (let [values-v (field (object! m path) :values)
        values (if (present? values-v) (array! values-v (conj path "values")) [])
        decoded (mapv (fn [i v] (any-value! v (conj path "values" i)))
                      (range (count values)) values)
        kinds (set (map value-kind decoded))]
    (if (<= (count kinds) 1)
      decoded
      (invalid! path :heterogeneous-array "an array of one scalar type" decoded))))

(defn- any-value! [v path]
  (let [m (object! v path)
        arms (filter (fn [k] (present? (field m k)))
                     [:stringValue :boolValue :intValue :doubleValue
                      :arrayValue :kvlistValue :bytesValue])]
    (when-not (= 1 (count arms))
      (invalid! path :invalid-any-value "exactly one AnyValue field" m))
    (let [arm (first arms)
          x (field m arm)
          p (conj path (name arm))]
      (case arm
        :stringValue (string! x p)
        :boolValue (if (or (true? x) (false? x)) x
                       (invalid! p :wrong-type "boolean" x))
        :intValue (int64! x p)
        ;; A protobuf double accepts any JSON number token, including `1`.
        ;; Most parsers represent that spelling as an integer even though the
        ;; declared wire field is a double.
        :doubleValue (let [d (when (or (integer? x) (float? x)) (double x))]
                       (if (and d (= d d) (not= d ##Inf) (not= d ##-Inf))
                         d
                         (invalid! p :wrong-type "finite JSON number" x)))
        :arrayValue (array-value! x p)
        ;; The SDK's canonical attribute model intentionally has no nested maps
        ;; or byte strings, so accepting these would silently lose information.
        :kvlistValue (invalid! p :unsupported-any-value
                               "scalar or homogeneous scalar array" x)
        :bytesValue (invalid! p :unsupported-any-value
                              "scalar or homogeneous scalar array" x)))))

(defn- attributes! [v path]
  (let [xs (if (present? v) (array! v path) [])]
    (reduce (fn [attrs i]
              (let [p (conj path i)
                    kv (object! (nth xs i) p)
                    key-v (field kv :key)
                    value-v (field kv :value)
                    key (if (present? key-v)
                          (string! key-v (conj p "key"))
                          (invalid! (conj p "key") :missing-field "string" nil))]
                (when (contains? attrs key)
                  (invalid! (conj p "key") :duplicate-attribute-key "unique key" key))
                (when-not (present? value-v)
                  (invalid! (conj p "value") :missing-field "AnyValue" nil))
                (assoc attrs key (any-value! value-v (conj p "value")))))
            {}
            (range (count xs)))))

(defn- count! [m k path]
  (let [v (field m k)]
    (if (present? v) (uint32! v (conj path (name k))) 0)))

(defn- trace-state! [v path]
  (if (or (not (present? v)) (= "" v))
    []
    (let [s (string! v path)
          entries (str/split s #"," -1)]
      (mapv (fn [i entry]
              (let [at (str/index-of entry "=")]
                (if (and at (pos? at) (< at (dec (count entry))))
                  [(subs entry 0 at) (subs entry (inc at))]
                  (invalid! (conj path i) :invalid-trace-state
                            "non-empty key=value member" entry))))
            (range (count entries)) entries))))

(def ^:private kinds
  {0 :internal 1 :internal 2 :server 3 :client 4 :producer 5 :consumer
   "SPAN_KIND_UNSPECIFIED" :internal "SPAN_KIND_INTERNAL" :internal
   "SPAN_KIND_SERVER" :server "SPAN_KIND_CLIENT" :client
   "SPAN_KIND_PRODUCER" :producer "SPAN_KIND_CONSUMER" :consumer})

(def ^:private statuses
  {0 :unset 1 :ok 2 :error
   "STATUS_CODE_UNSET" :unset "STATUS_CODE_OK" :ok "STATUS_CODE_ERROR" :error})

(defn- enum! [v path values default]
  (let [v (if (present? v) v default)]
    (if-let [decoded (get values v)]
      decoded
      (invalid! path :invalid-enum (vec (keys values)) v))))

(defn- event! [v path]
  (let [m (object! v path)
        time-v (field m :timeUnixNano)
        name-v (field m :name)]
    {:name (if (present? name-v) (string! name-v (conj path "name")) "")
     :timestamp-unix-nano (if (present? time-v)
                            (uint64! time-v (conj path "timeUnixNano")) 0)
     :attributes (attributes! (field m :attributes) (conj path "attributes"))
     :dropped-attributes-count (count! m :droppedAttributesCount path)}))

(defn- link! [v path]
  (let [m (object! v path)
        trace-v (field m :traceId)
        span-v (field m :spanId)
        flags-v (field m :flags)]
    (when-not (present? trace-v)
      (invalid! (conj path "traceId") :missing-field "trace id" nil))
    (when-not (present? span-v)
      (invalid! (conj path "spanId") :missing-field "span id" nil))
    {:span-context
     {:trace-id (hex-id! trace-v 32 (conj path "traceId") false)
      :span-id (hex-id! span-v 16 (conj path "spanId") false)
      :trace-flags (if (present? flags-v) (uint32! flags-v (conj path "flags")) 0)
      :trace-state (trace-state! (field m :traceState) (conj path "traceState"))
      :remote? false}
     :attributes (attributes! (field m :attributes) (conj path "attributes"))
     :dropped-attributes-count (count! m :droppedAttributesCount path)}))

(defn- status! [v path]
  (if-not (present? v)
    {:code :unset :description nil}
    (let [m (object! v path)
          code (enum! (field m :code) (conj path "code") statuses 0)
          message (optional-string! m :message path nil)]
      {:code code :description (when (= :error code) message)})))

(defn- span! [v path resource scope]
  (let [m (object! v path)
        trace-v (field m :traceId)
        span-v (field m :spanId)
        start-v (field m :startTimeUnixNano)
        end-v (field m :endTimeUnixNano)
        parent-v (field m :parentSpanId)
        flags-v (field m :flags)]
    (doseq [[k x expected] [["traceId" trace-v "trace id"]
                            ["spanId" span-v "span id"]
                            ["startTimeUnixNano" start-v "uint64 timestamp"]
                            ["endTimeUnixNano" end-v "uint64 timestamp"]]]
      (when-not (present? x)
        (invalid! (conj path k) :missing-field expected nil)))
    {:name (optional-string! m :name path "")
     :span-context
     {:trace-id (hex-id! trace-v 32 (conj path "traceId") false)
      :span-id (hex-id! span-v 16 (conj path "spanId") false)
      :trace-flags (if (present? flags-v) (uint32! flags-v (conj path "flags")) 0)
      :trace-state (trace-state! (field m :traceState) (conj path "traceState"))
      :remote? false}
     :parent-span-id (if (present? parent-v)
                       (hex-id! parent-v 16 (conj path "parentSpanId") true) nil)
     :kind (enum! (field m :kind) (conj path "kind") kinds 0)
     :scope scope
     :resource resource
     :start-time-unix-nano (uint64! start-v (conj path "startTimeUnixNano"))
     :end-time-unix-nano (uint64! end-v (conj path "endTimeUnixNano"))
     :attributes (attributes! (field m :attributes) (conj path "attributes"))
     :events (let [xs-v (field m :events)
                   xs (if (present? xs-v) (array! xs-v (conj path "events")) [])]
               (mapv (fn [i x] (event! x (conj path "events" i)))
                     (range (count xs)) xs))
     :links (let [xs-v (field m :links)
                  xs (if (present? xs-v) (array! xs-v (conj path "links")) [])]
              (mapv (fn [i x] (link! x (conj path "links" i)))
                    (range (count xs)) xs))
     :status (status! (field m :status) (conj path "status"))
     :dropped-attributes-count (count! m :droppedAttributesCount path)
     :dropped-events-count (count! m :droppedEventsCount path)
     :dropped-links-count (count! m :droppedLinksCount path)
     :ended? true}))

(defn- resource! [v schema-url path]
  (let [m (if (present? v) (object! v path) {})
        r (resource/resource
           (attributes! (field m :attributes) (conj path "attributes"))
           {:schema-url schema-url})]
    (assoc r :dropped-attributes-count (count! m :droppedAttributesCount path))))

(defn- scope! [v schema-url path]
  (let [m (if (present? v) (object! v path) {})]
    {:name (optional-string! m :name path "")
     :version (optional-string! m :version path nil)
     :schema-url schema-url
     :attributes (attributes! (field m :attributes) (conj path "attributes"))
     :dropped-attributes-count (count! m :droppedAttributesCount path)}))

(defn- error-map [e]
  (assoc (or (ex-data e)
             {:type :otel.otlp.trace-decode/invalid-value
              :path [] :reason :decode-failure})
         :message (or (ex-message e) (str e))))

(defn- raw-span-count [scope-spans]
  (if (vector? scope-spans)
    (reduce (fn [n ss]
              (let [spans (field ss :spans)]
                (+ n (if (vector? spans) (count spans) 0))))
            0 scope-spans)
    0))

(defn- decode-scope [acc ss path resource]
  (try
    (let [m (object! ss path)
          schema-url (optional-string! m :schemaUrl path nil)
          scope (scope! (field m :scope) schema-url (conj path "scope"))
          spans-v (field m :spans)
          spans (if (present? spans-v) (array! spans-v (conj path "spans")) [])]
      (reduce (fn [a i]
                (try
                  (update a :spans conj
                          (span! (nth spans i) (conj path "spans" i) resource scope))
                  (catch :default e
                    (-> a
                        (update :rejected-spans inc)
                        (update :errors conj (error-map e))))))
              acc (range (count spans))))
    (catch :default e
      (let [spans (field ss :spans)
            rejected (if (vector? spans) (count spans) 0)]
        (-> acc
            (update :rejected-spans + rejected)
            (update :errors conj (error-map e)))))))

(defn- decode-resource [acc rs path]
  (try
    (let [m (object! rs path)
          schema-url (optional-string! m :schemaUrl path nil)
          r (resource! (field m :resource) schema-url (conj path "resource"))
          scopes-v (field m :scopeSpans)
          scopes (if (present? scopes-v)
                   (array! scopes-v (conj path "scopeSpans")) [])]
      (reduce (fn [a i]
                (decode-scope a (nth scopes i) (conj path "scopeSpans" i) r))
              acc (range (count scopes))))
    (catch :default e
      (let [scopes (field rs :scopeSpans)]
        (-> acc
            (update :rejected-spans + (raw-span-count scopes))
            (update :errors conj (error-map e)))))))

(defn decode-request
  "Decode an already parsed OTLP ExportTraceServiceRequest.

  Returns `{:spans [...] :rejected-spans n :errors [...]}`. A malformed span is
  rejected independently, while valid siblings remain available for export.
  Every error carries `:path`, `:reason`, `:expected` and `:actual` where known,
  making it suitable for an OTLP partial-success response and local diagnostics.

  This function does not parse JSON strings. A receiver must first enforce its
  byte/content/auth limits and use a JSON parser that preserves integer strings."
  [request]
  (try
    (let [m (object! request [])
          resources-v (field m :resourceSpans)
          resources (if (present? resources-v)
                      (array! resources-v ["resourceSpans"]) [])]
      (reduce (fn [acc i]
                (decode-resource acc (nth resources i) ["resourceSpans" i]))
              {:spans [] :rejected-spans 0 :errors []}
              (range (count resources))))
    (catch :default e
      {:spans [] :rejected-spans 0 :errors [(error-map e)]})))
