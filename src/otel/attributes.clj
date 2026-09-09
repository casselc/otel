(ns otel.attributes
  "Attribute collection normalization against the OpenTelemetry data model.

  Values are normalized once, at the SDK boundary, into the portable algebra in
  otel.any-value. Invalid entries are dropped rather than thrown into observed
  application code. normalize-result retains bounded reasons for callers that
  can expose SDK diagnostics; normalize preserves the original map-only API."
  (:refer-clojure :exclude [merge])
  (:require [otel.any-value :as any]))

(def default-count-limit 128)
(def ^:private max-reported-errors 16)

(defn limits
  "Attribute limits. :count-limit caps top-level entries. The value length,
  depth, node and byte limits apply recursively to each AnyValue; max bytes and
  nodes are also enforced over the complete attribute collection."
  [{:keys [count-limit value-length-limit max-depth max-nodes max-bytes]}]
  (clojure.core/merge
   any/default-limits
   {:count-limit (or count-limit default-count-limit)}
   (cond-> {}
     (some? value-length-limit) (assoc :value-length-limit value-length-limit)
     (some? max-depth) (assoc :max-depth max-depth)
     (some? max-nodes) (assoc :max-nodes max-nodes)
     (some? max-bytes) (assoc :max-bytes max-bytes))))

(def default-limits (limits {}))

(defn- report-error [result error]
  (-> result
      (update :dropped-count inc)
      (update :errors #(if (< (count %) max-reported-errors)
                         (conj % error) %))))

(defn- reported-key [key]
  (when key (subs key 0 (min 128 (count key)))))

(defn- source-entries [attrs]
  (->> attrs
       (mapv (fn [[key value]] [(any/key-string key) key value]))
       (sort-by (fn [[canonical _ _]] (or canonical "")))))

(defn normalize-result
  "Normalize an attribute collection without throwing for bad application data.

  The result contains :attributes, :dropped-count, :truncated-count and at most
  sixteen structured :errors. Canonical-key collisions drop every conflicting
  entry, so map traversal order can never select an arbitrary winner."
  ([attrs] (normalize-result attrs default-limits))
  ([attrs options]
   (let [options (limits options)]
     (if (or (nil? attrs) (and (map? attrs) (empty? attrs)))
       {:attributes {} :dropped-count 0 :truncated-count 0 :errors []}
       (if-not (map? attrs)
         {:attributes {} :dropped-count 1 :truncated-count 0
          :errors [{:reason :invalid-attribute-collection}]}
         (let [entries (source-entries attrs)
               frequencies (frequencies (keep first entries))
               initial {:attributes (sorted-map)
                        :dropped-count 0 :truncated-count 0 :errors []
                        :nodes 0 :bytes 0}]
           (->
            (reduce
             (fn [result [key _source-key value]]
               (cond
                 (nil? key)
                 (report-error result {:reason :invalid-key})

                 (empty? key)
                 (report-error result {:key "" :reason :empty-key})

                 (> (get frequencies key 0) 1)
                 (report-error result {:key (reported-key key)
                                       :reason :duplicate-key})

                 (>= (count (:attributes result)) (:count-limit options))
                 (report-error result {:key (reported-key key)
                                       :reason :count-limit})

                 :else
                 (let [key-bytes (* 4 (count key))
                       remaining-nodes (- (:max-nodes options) (:nodes result))
                       remaining-bytes (- (:max-bytes options)
                                          (:bytes result) key-bytes)
                       normalized
                       (if (neg? remaining-nodes)
                         {:error :node-limit}
                         (if (neg? remaining-bytes)
                           {:error :byte-limit}
                           (any/canonicalize
                            value
                            (assoc options
                                   :max-nodes remaining-nodes
                                   :max-bytes remaining-bytes))))]
                   (if-let [reason (:error normalized)]
                     (report-error result {:key (reported-key key) :reason reason})
                     (-> result
                         (assoc-in [:attributes key] (:value normalized))
                         (update :nodes + (:nodes normalized))
                         (update :bytes + key-bytes (:bytes normalized))
                         (update :truncated-count +
                                 (if (:truncated? normalized) 1 0)))))))
             initial entries)
            (dissoc :nodes :bytes))))))))

(defn normalize
  "Return only the canonical attribute map, preserving the original public API.
  Use normalize-result when dropped/truncated diagnostics are needed."
  ([attrs] (:attributes (normalize-result attrs default-limits)))
  ([attrs options] (:attributes (normalize-result attrs options))))

(defn normalize-scope
  "Normalize one instrumentation scope and account for attributes rejected at
  registration. Keeping this at the shared SDK boundary prevents a bad scope
  value from reaching strict OTLP encoding and dropping an otherwise valid
  batch."
  [scope]
  (let [{:keys [attributes dropped-count]}
        (normalize-result (:attributes scope) default-limits)
        dropped-count (+ (or (:dropped-attributes-count scope) 0)
                         dropped-count)]
    (cond-> (assoc scope :attributes attributes)
      (pos? dropped-count) (assoc :dropped-attributes-count dropped-count))))

(defn merge-attrs
  "Normalize and merge attribute maps left to right; later values win."
  ([] {})
  ([a] (normalize a))
  ([a b] (clojure.core/merge (normalize a) (normalize b)))
  ([a b & more] (reduce merge-attrs (merge-attrs a b) more)))
