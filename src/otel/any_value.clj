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

;; Immutable original defaults are already a complete normalized option map.
;; Keep identity guards so redefined defaults and caller maps use normal merging.
(def ^:private scalar-default-limits default-limits)

(defn limits [options]
  (if (and (identical? default-limits scalar-default-limits)
           (identical? options scalar-default-limits))
    scalar-default-limits
    (let [{:keys [max-depth max-nodes max-bytes value-length-limit]} options]
      (merge default-limits
             (cond-> {}
               (some? max-depth) (assoc :max-depth max-depth)
               (some? max-nodes) (assoc :max-nodes max-nodes)
               (some? max-bytes) (assoc :max-bytes max-bytes)
               (some? value-length-limit)
               (assoc :value-length-limit value-length-limit))))))

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

(def ^:private original-string-bytes string-bytes)

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

;; Identity guards the shortcut's fixed budget assumptions. Replacing defaults
;; (even with an equal map) conservatively restores the generic options path.

(defn- default-scalar-result [value]
  (cond
    (string? value)
    (let [size (string-bytes value)]
      (when (<= size (:max-bytes scalar-default-limits))
        {:value value :nodes 1 :bytes size :truncated? false}))

    (boolean? value)
    {:value value :nodes 1 :bytes 1 :truncated? false}

    (and (integer? value) (<= min-int64 value max-int64))
    {:value value :nodes 1 :bytes 8 :truncated? false}))

(defn canonicalize
  "Return a non-throwing normalization result for one value.

  A successful result contains :value, :nodes, :bytes and :truncated?. An
  invalid or over-budget value contains :error and bounded explanatory data.
  Callers that instrument application code can therefore drop/report a bad
  value without changing the application's return or exception behavior."
  ([value]
   (or (when (identical? default-limits scalar-default-limits)
         (default-scalar-result value))
       (canonicalize value default-limits)))
  ([value options]
   (let [options (limits options)]
     (try
       (or (when (and (identical? default-limits scalar-default-limits)
                      (identical? options scalar-default-limits))
             (default-scalar-result value))
           (let [[value state]
                 (canonicalize* value options 0
                                {:nodes 0 :bytes 0 :truncated? false})]
             (assoc state :value value)))
       (catch Throwable error
         (if (:otel.any-value/error (ex-data error))
           {:error (:reason (ex-data error))
            :details (dissoc (ex-data error) :otel.any-value/error :reason)}
           (throw error)))))))

(def ^:private scalar-text-canonicalizer canonicalize)

(defn try-scalar-string
  "Return canonical scalar text without a normalization-result map, or nil
  when the caller must use normal canonicalization. Only unchanged bounded
  strings, Booleans and admitted Int64 integers qualify. Decline is not an
  invalid-value verdict: structured/special/floating values and changed limits
  or canonicalizer roots must retain the normal path. Does not cache values,
  truncate, stringify unsupported objects, or change canonicalize's API."
  [value]
  (when (and (identical? default-limits scalar-default-limits)
             (identical? canonicalize scalar-text-canonicalizer))
    (cond
      (string? value)
      (when (<= (string-bytes value) (:max-bytes scalar-default-limits)) value)

      (boolean? value) (str value)

      (and (integer? value) (<= min-int64 value max-int64)) (str value))))

(def ^:private scalar-text-helper try-scalar-string)

(defn default-scalar-string?
  "True only while scalar text uses the SDK's original helper, canonicalizer
  and limits. Lets callers qualify a side-effect-free admission scan without
  treating an arbitrary helper installed before their own startup as stock.
  Does not invoke the helper or admit a value; try-scalar-string still owns
  per-value bounds. Private SDK implementation roots are trusted, not sealed
  against deliberate mutation; this is not a cross-thread redefinition lock."
  []
  (and (identical? try-scalar-string scalar-text-helper)
       (identical? canonicalize scalar-text-canonicalizer)
       (identical? default-limits scalar-default-limits)))

(defn default-string-value-max-chars
  "Return the character bound for the unchanged scalar string policy, or nil.

  Pure builtin-string admission scans can check this bound while walking their
  input, instead of calling the scalar formatter for every value. Count using
  the same host character semantics as this SDK; this is not a UTF-8 byte bound.
  Changed helpers, canonicalizers, limits or byte counters decline without
  invoking them. This is not a cross-thread redefinition lock or permission to
  skip generic normalization for other value types."
  []
  (when (and (identical? try-scalar-string scalar-text-helper)
             (identical? canonicalize scalar-text-canonicalizer)
             (identical? default-limits scalar-default-limits)
             (identical? string-bytes original-string-bytes))
    (quot (:max-bytes scalar-default-limits) 4)))

(def ^:private scalar-map-min-int64 min-int64)
(def ^:private scalar-map-max-int64 max-int64)

(defn- load-scalar-string-map-converter []
  (try
    ;; Optional host-specific implementation. Ordinary JVM/BB loading never
    ;; requires jolt.scheme; unsupported runtimes simply retain scalar calls.
    (require 'otel.any-value-native)
    ((ns-resolve 'otel.any-value-native 'load-string-map-converter!)
     [[#'try-scalar-string scalar-text-helper]
      [#'canonicalize scalar-text-canonicalizer]
      [#'default-limits scalar-default-limits]
      [#'string-bytes original-string-bytes]
      [#'min-int64 scalar-map-min-int64]
      [#'max-int64 scalar-map-max-int64]]
     (quot (:max-bytes scalar-default-limits) 4))
    (catch Throwable _ (constantly nil))))

(def ^:private scalar-string-map-converter
  (delay (load-scalar-string-map-converter)))

(defn try-scalar-string-map
  "Return a bounded builtin map's scalar text values, or nil to decline.

  This optional host optimization accepts string-key maps containing bounded
  strings, Booleans and native fixnums only while the SDK's original scalar
  helper, canonicalizer, limits, byte counter and Int64 bounds remain installed.
  It preserves map kind/order, drops metadata like ordinary fresh conversion,
  and never mutates input nodes or calls replacement helpers. Unsupported maps,
  hosts, values and policies must use the caller's ordinary scalar conversion;
  nil is not an invalid-value verdict. No canonicalization or capture API changes.
  Private implementation roots and ordinary core behavior remain trusted; this
  is not an atomic cross-thread redefinition lock."
  [value]
  ;; The native kernel owns the live SDK-root checks supplied by this defining
  ;; namespace; repeating them here would add a second policy boundary.
  (or (@scalar-string-map-converter value) nil))

;; Captured immediately after definition for the payload cache constructor's
;; policy-root bundle; never sampled from caller input.
(def ^:private original-try-scalar-string-map try-scalar-string-map)

;; Constructed in this defining namespace under the same trusted ordinary-core
;; assumption as the existing native helper; not sampled from caller input.
(def ^:private scalar-sorted-map-prototype
  (sorted-map "$scalar-probe-a" 7 "$scalar-probe-b" "probe-text" "$scalar-probe-c" false))

(defn- load-sorted-scalar-string-map-converter []
  (try
    (require 'otel.any-value-native)
    ((ns-resolve 'otel.any-value-native 'load-sorted-string-map-converter!)
     [[#'try-scalar-string scalar-text-helper]
      [#'canonicalize scalar-text-canonicalizer]
      [#'default-limits scalar-default-limits]
      [#'string-bytes original-string-bytes]
      [#'min-int64 scalar-map-min-int64]
      [#'max-int64 scalar-map-max-int64]]
     (quot (:max-bytes scalar-default-limits) 4)
     scalar-sorted-map-prototype)
    (catch Throwable _ (constantly nil))))

(def ^:private sorted-scalar-string-map-converter
  (delay (load-sorted-scalar-string-map-converter)))

(defn try-sorted-scalar-string-map
  "Return a fresh plain scalar-text map from an admitted sorted map, or nil.

  Unlike try-scalar-string-map, this deliberately normalizes to the same plain
  map as (into {} (map scalar-conversion) input); it does not preserve sorted
  map kind. Only stock operations, string keys, bounded strings/Booleans/native
  fixnums and at most 128 entries qualify under the unchanged SDK policy.
  Unknown operations/layouts, malformed node shapes/counts, changed policies and unsupported
  hosts decline without invoking callbacks. No comparator is called. Metadata
  is dropped, input is untouched, and nil requires the caller's normal path.
  Ordinary core behavior and private implementation roots remain trusted;
  this is not a seal against their mutation or a cross-thread redefinition lock."
  [value]
  (or (@sorted-scalar-string-map-converter value) nil))

;; Captured immediately after definition; see original-try-scalar-string-map.
(def ^:private original-try-sorted-scalar-string-map try-sorted-scalar-string-map)

(defn- scalar-string-map-payload-bindings
  "The eight original policy anchors a payload cache must stay consistent
  with: the six scalar-text anchors plus both map-converter identities."
  []
  [[#'try-scalar-string scalar-text-helper]
   [#'canonicalize scalar-text-canonicalizer]
   [#'default-limits scalar-default-limits]
   [#'string-bytes original-string-bytes]
   [#'min-int64 scalar-map-min-int64]
   [#'max-int64 scalar-map-max-int64]
   [#'try-scalar-string-map original-try-scalar-string-map]
   [#'try-sorted-scalar-string-map original-try-sorted-scalar-string-map]])

(defn- load-scalar-string-map-payload-cache [bindings max-insertions max-chars folder]
  (try
    (require 'otel.any-value-native)
    ((ns-resolve 'otel.any-value-native 'load-scalar-string-map-payload-cache!)
     bindings max-insertions max-chars folder)
    (catch Throwable _ nil)))

(defn- load-scalar-text-planner [factory bindings max-shapes max-key-chars]
  (try
    (require 'otel.any-value-native)
    ((ns-resolve 'otel.any-value-native 'load-scalar-text-planner!)
     factory bindings (quot (:max-bytes scalar-default-limits) 4)
     scalar-sorted-map-prototype max-shapes max-key-chars)
    (catch Throwable _ nil)))

(defn scalar-string-map-payload-converter*
  "Internal: parameterized constructor for focused bound tests. See
  scalar-string-map-payload-converter for the public contract."
  ([max-insertions max-chars]
   (scalar-string-map-payload-converter* max-insertions max-chars nil))
  ([max-insertions max-chars folder]
   (let [bindings (scalar-string-map-payload-bindings)]
    (when-let [{:keys [current? lookup store!]}
               (load-scalar-string-map-payload-cache
                bindings max-insertions max-chars folder)]
      (fn [value]
        (when (current?)
          (or (lookup value)
              (when-let [result (or (original-try-scalar-string-map value)
                                     (original-try-sorted-scalar-string-map
                                      value))]
                ;; Store rechecks all eight live anchors before/after acceptance.
                ;; It charges admitted output without temporary map entries.
                (store! value result))))))))
  ([max-insertions max-chars folder options]
   (if (nil? options)
     (scalar-string-map-payload-converter* max-insertions max-chars folder)
     (let [{:keys [shape-plan-factory max-shapes max-shape-key-chars]
            :or {max-shapes 8 max-shape-key-chars 8192}} options]
       (when-not (and (map? options)
                      (every? #{:shape-plan-factory :max-shapes :max-shape-key-chars} (keys options))
                      (ifn? shape-plan-factory)
                      (integer? max-shapes) (<= 0 max-shapes 8)
                      (integer? max-shape-key-chars) (<= 0 max-shape-key-chars 8192))
         (throw (ex-info "Invalid scalar-text shape-plan options" {:type ::invalid-shape-plans})))
       (let [bindings (scalar-string-map-payload-bindings)]
         (when-let [{:keys [current? lookup store!]}
                    (load-scalar-string-map-payload-cache bindings max-insertions max-chars folder)]
           (when-let [{shape-lookup :lookup learn! :learn!}
                      (load-scalar-text-planner shape-plan-factory bindings max-shapes max-shape-key-chars)]
             (fn [value]
               (when (current?)
                 (or (lookup value)
                     (when-let [result
                                (or (original-try-scalar-string-map value)
                                    (shape-lookup value)
                                    (when-let [converted (original-try-sorted-scalar-string-map value)]
                                      ;; Learn only after original SDK admission.
                                      ;; Library independently validates; no new
                                      ;; input-value cache or consumer policy roots.
                                      (learn! value)
                                      converted))]
                       (store! value result))))))))))))

(defn scalar-string-map-payload-converter
  "Return a fresh, caller-owned closure that converts the same bounded
  scalar-text map shapes try-scalar-string-map / try-sorted-scalar-string-map
  admit, caching successful conversions by immutable source-root identity
  plus map kind/count/stock-operation identity so a repeated
  payload map need not be re-walked; or nil when the host/loader cannot
  supply the optional native cache (JVM/BB, or any native loader failure).

  Each call returns a fresh plain pmap wrapper with nil metadata. A cache hit
  shares the previously converted root behind a NEW wrapper; it is never the
  same wrapper object across calls, and formatted Strings may be shared with
  no identity promise. There is no global or cross-closure cache: the budget
  (default 64 insertions, 65536 retained key-plus-output String characters)
  is scoped to this one closure's lifetime and is monotonic, so it may
  overestimate retained memory after garbage collection. A budget decline
  still returns a correct, merely uncached, fresh conversion.

  Declines (nil) whenever any of the SDK's eight original policy roots -
  the scalar-text helper, canonicalizer, limits, byte counter, Int64 bounds,
  and both map-converter identities - no longer match what this namespace
  captured at definition time; the live replacement is never invoked, and
  the caller must fall back to its normal conversion path. This check runs
  before and after every admission/cache-match/conversion, not just once.

  Input must remain stable during each call. The original converter bodies
  and ordinary core/private runtime internals are trusted callback-free;
  root identity alone does not certify a body changed by compiler weaving.
  Such woven converter bodies require separate qualification before use.
  This closure is not safe for concurrent calls from multiple threads and
  promises no cross-thread atomic-redefinition lock. It does not change
  canonicalize's API, is never invoked by the original helpers, and is a
  PROTOTYPE: constructing it is not authorization for product adoption.

  Optional folder supplies native traversal without installing global helpers.
  It is a trusted synchronous (map visitor initial) full fold; visitor receives
  (key value accumulator). It must visit every entry once without mutation or
  retaining a loan. This is not validation of arbitrary third-party callbacks.
  Unsupported hosts/loaders still decline; existing no-argument use is unchanged.

  The optional second argument enables bounded inferred key-shape plans through
  {:shape-plan-factory trusted-native-factory}. The SDK supplies its original
  policy anchors, limits and trusted prototype; applications never provide those
  private roots. Learn only after original sorted-map admission. Existing source
  root hits run first, plain maps retain their original path, and every result
  still goes through SDK store admission/policy checks. Unknown shapes/values
  retain the original fallback. No compiler/global JSON hook is installed.
  Additional per-closure retention is at most :max-shapes (default/max 8) key
  schemas / :max-shape-key-chars (default/max 8192) String characters, at most128
  keys per schema plus bounded template overhead. No input wrappers/trees or
  telemetry values are retained by plans. These bounds are not a hard RSS cap.
  A full schema budget leaves conversion correct but uses the old path for new
  shapes. Factory bodies/ordinary core and immutable String keys are trusted,
  like the injected folder. This is not validation of arbitrary callbacks.
  Invalid options throw; unavailable host/factory returns nil at construction.
  No-options arities do not load or invoke any shape factory."
  ([] (scalar-string-map-payload-converter* 64 65536))
  ([folder] (scalar-string-map-payload-converter* 64 65536 folder))
  ([folder options] (scalar-string-map-payload-converter* 64 65536 folder options)))

(defn scalar-text-payload-projector
  "Experimental encoder-only handoff, NOT a map-returning converter.

  Returns a fresh serial-payload closure or nil on unsupported host/capability.
  The closure returns a fresh plain map OR the supplied trusted codec's opaque
  scalar-text view, or nil to require the caller's existing normalization path.
  Use its result only with that codec, never as an ordinary Clojure map or a
  data.json value. The existing scalar-string-map-payload-converter is unchanged.

  SDK owns all eight ORIGINAL policy anchors and scalar limits. Source-root
  cache hits run first, then the original plain-map helper, learned sorted-map
  view, and original sorted-map fallback. Learn only after original admission.
  Views do not enter the ordinary plain-map root cache. Successful fallback
  maps retain its existing fresh-wrapper/admission/retention behavior.

  Retention bounds are fixed per closure:64 root-cache insertions/65536 String
  characters,8 schemas/8192 key characters/128 keys per schema, plus bounded
  native template overhead. Views may own ephemeral value slots but plans must
  retain no source maps/trees/telemetry values. These are not hard RSS bounds.

  Requires {:shape-plan-factory trusted-native-factory}; the factory and optional
  folder have the same trusted callback-free native contracts as the existing
  map converter. No callback/global registration, permission to skip policy,
  cross-thread redefinition lock or concurrent-use promise is introduced.
  Input Strings/collections must remain immutable during use. Not adopted by
  default SDK/exporter use; a whole consumer needs separate qualification."
  [folder options]
  (when-not (and (map? options) (= #{:shape-plan-factory} (set (keys options)))
                 (ifn? (:shape-plan-factory options)))
    (throw (ex-info "Invalid scalar-text projector options" {:type ::invalid-scalar-text-projector})))
  (let [bindings (scalar-string-map-payload-bindings)]
    (when-let [{:keys [current? lookup store!]}
               (load-scalar-string-map-payload-cache bindings 64 65536 folder)]
      (when-let [{shape-lookup :lookup learn! :learn!}
                 (load-scalar-text-planner (:shape-plan-factory options) bindings 8 8192)]
        (fn [value]
          (when (current?)
            (or (lookup value)
                (when-let [converted (original-try-scalar-string-map value)]
                  (store! value converted))
                (when-let [view (shape-lookup value)]
                  ;; Do not send an opaque view through the plain-map cache.
                  (when (current?) view))
                (when-let [converted (original-try-sorted-scalar-string-map value)]
                  (learn! value)
                  (store! value converted)))))))))
