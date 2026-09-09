(ns otel.any-value
  "Portable, immutable values for OpenTelemetry AnyValue.

  Clojure scalars, vectors and maps already have the value semantics AnyValue
  needs. Empty and byte-string values need explicit representations so they do
  not collapse into an absent map entry or a mutable host byte array."
  (:refer-clojure :exclude [bytes bytes?]))

(def min-int64 -9223372036854775808)
(def max-int64 9223372036854775807)

(defrecord EmptyValue [])
(defrecord Bytes [octets])

(def empty-value
  "The canonical present-but-empty AnyValue. This is distinct from an absent
  attribute key, an empty string and an empty array."
  (->EmptyValue))

(defn empty-value? [value] (instance? EmptyValue value))
(defn bytes? [value] (instance? Bytes value))
(defn byte-values [value] (:octets value))

(defn bytes
  "Copy a finite collection of unsigned octets into an immutable AnyValue byte
  string. Invalid octets are rejected at this explicit construction boundary."
  [octets]
  (if (bytes? octets)
    octets
    (let [value (when (sequential? octets) (vec octets))]
      (when-not (and value
                     (every? #(and (integer? %) (<= 0 % 255)) value))
        (throw (ex-info "OpenTelemetry bytes require unsigned octets"
                        {:otel.any-value/error true
                         :reason :invalid-bytes})))
      (->Bytes value))))

(def default-limits
  "Per attribute-value safety limits. String bytes are conservatively charged
  as four bytes per Clojure character, which is never smaller than UTF-8."
  {:max-depth 16
   :max-nodes 1024
   :max-bytes (* 1024 1024)
   :value-length-limit nil})

(defn limits [options]
  (let [{:keys [max-depth max-nodes max-bytes value-length-limit]} options]
    (merge default-limits
           (cond-> {}
             (some? max-depth) (assoc :max-depth max-depth)
             (some? max-nodes) (assoc :max-nodes max-nodes)
             (some? max-bytes) (assoc :max-bytes max-bytes)
             (some? value-length-limit)
             (assoc :value-length-limit value-length-limit)))))

(defn- fail! [reason data]
  (throw (ex-info "invalid OpenTelemetry AnyValue"
                  (assoc data :otel.any-value/error true :reason reason))))

(defn- consume [state options nodes bytes]
  (let [state (-> state (update :nodes + nodes) (update :bytes + bytes))]
    (when (> (:nodes state) (:max-nodes options))
      (fail! :node-limit {:maximum (:max-nodes options)}))
    (when (> (:bytes state) (:max-bytes options))
      (fail! :byte-limit {:maximum (:max-bytes options)}))
    state))

(defn- string-bytes [value]
  ;; Jolt strings are codepoint indexed while JVM strings are UTF-16 indexed.
  ;; Four bytes per observed character is a conservative bound on both hosts.
  (* 4 (count value)))

(defn- truncate-string [value limit]
  (if (and limit (> (count value) limit))
    [(subs value 0 limit) true]
    [value false]))

(defn key-string
  "Return the canonical string form of a supported attribute/map key, or nil."
  [key]
  (cond
    (string? key) key
    (keyword? key) (subs (str key) 1)
    (symbol? key) (str key)
    :else nil))

(declare canonicalize*)

(defn- canonical-map [value options depth state]
  (let [entries (->> value
                     (mapv (fn [[key item]]
                             (let [canonical (key-string key)]
                               (when-not canonical
                                 (fail! :invalid-map-key {}))
                               (when (empty? canonical)
                                 (fail! :empty-map-key {}))
                               [canonical item])))
                     (sort-by first))
        keys (mapv first entries)
        key-frequencies (frequencies keys)
        duplicate (first (filter #(> (get key-frequencies %) 1) keys))]
    (when duplicate
      (fail! :duplicate-map-key
             {:key (subs duplicate 0 (min 128 (count duplicate)))}))
    (loop [remaining entries
           result (sorted-map)
           state (consume state options 1 0)]
      (if-let [[key item] (first remaining)]
        (let [state (consume state options 0 (string-bytes key))
              [item state] (canonicalize* item options (inc depth) state)]
          (recur (rest remaining) (assoc result key item) state))
        [result state]))))

(defn- canonical-array [value options depth state]
  (loop [remaining (seq value)
         result []
         state (consume state options 1 0)]
    (if (seq remaining)
      (let [[item state]
            (canonicalize* (first remaining) options (inc depth) state)]
        (recur (next remaining) (conj result item) state))
      [result state])))

(defn- canonicalize* [value options depth state]
  (when (> depth (:max-depth options))
    (fail! :depth-limit {:maximum (:max-depth options)}))
  (cond
    (nil? value) (fail! :nil-value {})
    (empty-value? value) [empty-value (consume state options 1 0)]

    (bytes? value)
    (let [octets (:octets value)]
      (when-not (and (vector? octets)
                     (every? #(and (integer? %) (<= 0 % 255)) octets))
        (fail! :invalid-bytes {}))
      (let [limit (:value-length-limit options)
            truncated? (and limit (> (count octets) limit))
            octets (if truncated? (subvec octets 0 limit) octets)
            state (cond-> (consume state options 1 (count octets))
                    truncated? (assoc :truncated? true))]
        [(->Bytes octets) state]))

    (string? value)
    (let [[value truncated?]
          (truncate-string value (:value-length-limit options))
          state (cond-> (consume state options 1 (string-bytes value))
                  truncated? (assoc :truncated? true))]
      [value state])

    (or (true? value) (false? value))
    [value (consume state options 1 1)]

    (integer? value)
    (if (<= min-int64 value max-int64)
      [value (consume state options 1 8)]
      (fail! :int64-range {:minimum min-int64 :maximum max-int64}))

    (float? value)
    [value (consume state options 1 8)]

    (keyword? value)
    (canonicalize* (subs (str value) 1) options depth state)

    (symbol? value)
    (canonicalize* (str value) options depth state)

    (map? value) (canonical-map value options depth state)
    (and (sequential? value) (not (string? value)))
    (canonical-array value options depth state)

    :else (fail! :unsupported-type {})))

(defn canonicalize
  "Return a non-throwing normalization result for one value.

  A successful result contains :value, :nodes, :bytes and :truncated?. An
  invalid or over-budget value contains :error and bounded explanatory data.
  Callers that instrument application code can therefore drop/report a bad
  value without changing the application's return or exception behavior."
  ([value] (canonicalize value default-limits))
  ([value options]
   (let [options (limits options)]
     (try
       (let [[value state]
             (canonicalize* value options 0
                            {:nodes 0 :bytes 0 :truncated? false})]
         (assoc state :value value))
       (catch :default error
         (if (:otel.any-value/error (ex-data error))
           {:error (:reason (ex-data error))
            :details (dissoc (ex-data error) :otel.any-value/error :reason)}
           (throw error)))))))
