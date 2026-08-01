(ns otel.attributes
  "Attribute normalization against the OpenTelemetry common data model.

  An OTLP attribute is a string key paired with an AnyValue, and AnyValue has
  exactly these shapes: string, bool, int, double, and homogeneous arrays of
  those. Clojure code naturally hands us keyword keys, keyword values, nils,
  nested maps and mixed vectors, none of which OTLP can carry. Normalizing at the
  boundary — rather than at export time — means an attribute that cannot be
  represented is dropped at the call that set it, and everything downstream
  (samplers, processors, exporters) can assume a clean map.

  Dropping rather than throwing is deliberate and matches the other SDKs:
  telemetry is not the workload, and a bad attribute value must never take down
  the code being instrumented."
  (:refer-clojure :exclude [merge]))

(def default-count-limit
  "Attributes kept per span/event/link. The spec's default."
  128)

(defn limits
  "Attribute limits. :count-limit caps how many attributes are kept (default 128);
  :value-length-limit truncates string values and strings inside arrays (default
  unlimited)."
  [{:keys [count-limit value-length-limit]}]
  {:count-limit (or count-limit default-count-limit)
   :value-length-limit value-length-limit})

(def default-limits (limits {}))

(defn- attr-key
  "The OTLP string form of an attribute key. A namespaced keyword keeps its
  namespace (:db/system -> \"db/system\") because that distinction is meaningful
  to the caller; `name` alone would silently collapse :db/system and :http/system."
  [k]
  (cond
    (string? k) k
    (keyword? k) (subs (str k) 1)
    (symbol? k) (str k)
    :else (str k)))

(defn- scalar-value
  "The OTLP scalar for `v`, or ::invalid if it has no representation. Keywords
  render as their name so idiomatic Clojure enum-ish values survive."
  [v]
  (cond
    (string? v) v
    ;; boolean? before integer?: on the JVM these are disjoint, but checking
    ;; explicitly documents that true/false must land on the bool arm.
    (or (true? v) (false? v)) v
    (integer? v) v
    (float? v) v
    (keyword? v) (subs (str v) 1)
    (symbol? v) (str v)
    :else ::invalid))

(defn- scalar-kind
  "The OTLP type tag of an already-validated scalar, for array homogeneity checks."
  [v]
  (cond
    (string? v) :string
    (or (true? v) (false? v)) :bool
    (integer? v) :int
    (float? v) :double))

(defn- truncate
  [limit v]
  (if (and limit (string? v) (> (count v) limit))
    (subs v 0 limit)
    v))

(defn- normalize-value
  "The OTLP value for `v` under `value-limit`, or ::invalid."
  [v value-limit]
  (if (and (sequential? v) (not (string? v)))
    ;; An array: every element must be a valid scalar AND they must all share one
    ;; type, since OTLP arrays are typed. An empty array is valid and stays empty.
    (let [elems (map #(scalar-value %) v)]
      (cond
        (some #(= ::invalid %) elems) ::invalid
        (empty? elems) []
        (apply = (map scalar-kind elems)) (mapv #(truncate value-limit %) elems)
        :else ::invalid))
    (let [s (scalar-value v)]
      (if (= ::invalid s) ::invalid (truncate value-limit s)))))

(defn normalize
  "Normalize `attrs` to an OTLP-representable map: string keys, scalar or
  homogeneous-array values. Entries whose value has no OTLP representation —
  including nil, maps, sets and mixed arrays — are dropped, and at most
  :count-limit entries are kept."
  ([attrs] (normalize attrs default-limits))
  ([attrs {:keys [count-limit value-length-limit]}]
   (if (empty? attrs)
     {}
     ;; reduced over the input so the count limit applies to KEPT entries: a map
     ;; whose first entries are all nil-valued must not exhaust the budget.
     (reduce (fn [acc [k v]]
               (if (>= (count acc) count-limit)
                 (reduced acc)
                 (let [nv (normalize-value v value-length-limit)]
                   (if (= ::invalid nv)
                     acc
                     (assoc acc (attr-key k) nv)))))
             {}
             attrs))))

(defn merge-attrs
  "Normalize and merge attribute maps left to right; later values win."
  ([] {})
  ([a] (normalize a))
  ([a b] (clojure.core/merge (normalize a) (normalize b)))
  ([a b & more] (reduce merge-attrs (merge-attrs a b) more)))
