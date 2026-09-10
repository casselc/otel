(ns otel.attribute-schema
  "Deterministic, storage-neutral hints inferred from OpenTelemetry source forms.

  Source is read as data and never evaluated. Unknown and conflicting evidence
  stays visible so a storage consumer cannot mistake a hint for a guarantee."
  (:require [clojure.string :as str]
            [otel.any-value :as any]
            [otel.attribute-schema.reader]))

(def schema-id "otel.attribute-schema/v1")

(def ^:private reader-namespace 'otel.attribute-schema.reader)

(def ^:private call-kinds
  {'otel.trace/with-span :with-span
   'otel.trace/start-span :start-span
   'otel.trace/set-attribute! :set-attribute
   'otel.trace/set-attributes! :set-attributes
   'otel.metrics/add! :metric
   'otel.metrics/add-delta! :metric
   'otel.metrics/record! :metric
   'otel.metrics/set-value! :metric
   'otel.metrics/observe! :metric
   'otel.logs/emit! :log
   'otel.resource/resource :resource
   'otel.sdk.tracer/get-tracer :tracer-scope
   'otel.sdk.metrics/get-meter :meter-scope
   'otel.sdk.logs/get-logger :logger-scope})

(def ^:private cast-types
  {'long :int64 'clojure.core/long :int64
   'int :int64 'clojure.core/int :int64
   'double :double 'clojure.core/double :double
   'float :double 'clojure.core/float :double
   'boolean :boolean 'clojure.core/boolean :boolean
   'str :string 'clojure.core/str :string
   'name :string 'clojure.core/name :string})

(defn- problem! [message data]
  (throw (ex-info message (assoc data :type ::invalid-schema-source))))

(defn canonical-source
  "Validate and return one normalized project-relative source path."
  [source]
  (let [source (str source)
        parts (str/split source #"/")]
    (when (or (empty? source)
              (str/starts-with? source "/")
              (str/includes? source "\\")
              (re-matches #"[A-Za-z]:.*" source)
              (some #(or (= "" %) (= "." %) (= ".." %)) parts))
      (problem! "attribute schema source must be a normalized relative path"
                {:source source}))
    source))

(defn read-forms
  "Read every form in text without evaluation."
  [source text]
  (let [source (canonical-source source)]
    (try
      {:source source
       :forms (binding [*ns* (the-ns reader-namespace)
                        *read-eval* false
                        *data-readers* {}
                        *default-data-reader-fn* nil]
                (read-string (str "[" text "\n]")))}
      (catch :default _error
        (problem! "could not read attribute schema source"
                  {:source source
                   :reason :unsupported-source-syntax})))))

(defn- require-env [forms]
  (let [ns-form (first (filter #(and (seq? %) (= 'ns (first %))) forms))
        clauses (drop 2 ns-form)
        specs (mapcat rest (filter #(and (seq? %) (= :require (first %))) clauses))]
    (reduce
     (fn [env spec]
       (if-not (vector? spec)
         env
         (let [target (first spec)
               ;; Libspecs may contain standalone reader flags in addition to
               ;; key/value options. Scan only the options this analyzer uses;
               ;; this is portable to Jolt, whose transient maps do not accept
               ;; the list pairs produced by partition.
               options (loop [remaining (rest spec) result {}]
                         (if-let [option (first remaining)]
                           (if (and (contains? #{:as :refer} option)
                                    (next remaining))
                             (recur (nnext remaining)
                                    (assoc result option (second remaining)))
                             (recur (next remaining) result))
                           result))
               alias (:as options)
               referred (:refer options)]
           (cond-> env
             alias (assoc-in [:aliases alias] target)
             (vector? referred)
             (update :refers into
                     (map (fn [name]
                            [name (symbol (str target) (str name))])
                          referred))))))
     {:aliases {} :refers {}} specs)))

(defn- resolve-symbol [env value]
  (if-not (symbol? value)
    value
    (if-let [ns-part (namespace value)]
      (if-let [target (get-in env [:aliases (symbol ns-part)])]
        (symbol (str target) (name value))
        value)
      (if (contains? (:locals env) value)
        ::local
        (get-in env [:refers value] value)))))

(declare infer-value-form*)

(defn- type-sort [types]
  (->> types distinct (sort-by pr-str) vec))

(defn- descriptor-contains? [descriptor target]
  (cond
    (= target descriptor) true
    (map? descriptor) (some #(descriptor-contains? % target) (vals descriptor))
    (sequential? descriptor) (some #(descriptor-contains? % target) descriptor)
    :else false))

(defn- literal-key [form quoted?]
  (cond
    (string? form) form
    ;; read-string cannot reconstruct a source namespace before it has read the
    ;; ns form. An auto-resolved ::key therefore lands in this analyzer's
    ;; namespace; treat it as dynamic rather than publish a false key.
    (and (keyword? form) (= (str reader-namespace) (namespace form))) nil
    (keyword? form) (subs (str form) 1)
    (and quoted? (symbol? form)) (str form)
    (and (seq? form) (= 'quote (first form)) (= 2 (count form)))
    (literal-key (second form) true)
    :else nil))

(defn- infer-map [env form quoted?]
  (let [entries (mapv (fn [[key value]]
                        [(literal-key key quoted?)
                         (:type (infer-value-form* env value quoted?))])
                      form)
        keys (mapv first entries)]
    (cond
      (some nil? keys)
      {:type {:kvlist :unknown} :kind :dynamic}
      (not= (count keys) (count (set keys)))
      {:type {:kvlist :invalid} :kind :literal}
      :else
      (let [type {:kvlist (into (sorted-map) entries)}]
        {:type type
         :kind (if (descriptor-contains? type :unknown)
                 :dynamic :literal)}))))

(defn- infer-array [env form quoted?]
  (let [values (mapv #(:type (infer-value-form* env % quoted?)) form)]
    {:type {:array (type-sort values)}
     :kind (if (some #(descriptor-contains? % :unknown) values)
             :dynamic :literal)}))

(defn- infer-value-form* [env form quoted?]
  (cond
    (nil? form) {:type :invalid :kind :literal}
    (string? form) {:type :string :kind :literal}
    (or (true? form) (false? form)) {:type :boolean :kind :literal}
    (integer? form) {:type (if (<= any/min-int64 form any/max-int64)
                             :int64 :invalid)
                     :kind :literal}
    (float? form) {:type :double :kind :literal}
    (keyword? form) {:type :string :kind :literal}

    (symbol? form)
    (if quoted?
      {:type :string :kind :literal}
      (if (= 'otel.any-value/empty-value (resolve-symbol env form))
        {:type :empty :kind :constructor}
        {:type :unknown :kind :dynamic}))

    (vector? form) (infer-array env form quoted?)
    (map? form) (infer-map env form quoted?)

    (and quoted? (sequential? form)) (infer-array env form true)

    (seq? form)
    (let [op (resolve-symbol env (first form))]
      (cond
        (and (= 'quote op) (= 2 (count form)))
        (infer-value-form* env (second form) true)
        (= 'otel.any-value/bytes op) {:type :bytes :kind :constructor}
        (contains? cast-types op) {:type (get cast-types op) :kind :cast}
        :else {:type :unknown :kind :dynamic}))

    :else {:type :invalid :kind :literal}))

(defn infer-value-form
  "Infer one quoted value form. Unresolved expressions return :unknown."
  ([form] (infer-value-form form {}))
  ([form aliases]
   (infer-value-form* {:aliases aliases :refers {}} form false)))

(defn- evidence [source form kind]
  (cond-> {:source source
           :kind kind
           :authority (case kind
                        :literal :literal
                        (:constructor :cast) :expression
                        :unknown)}
    (:line (meta form)) (assoc :line (:line (meta form)))))

(defn- entry
  ([source signal key value-form env form]
   (entry source signal :attributes key value-form env form))
  ([source signal location key value-form env form]
   (let [{:keys [type kind]} (infer-value-form* env value-form false)]
     {:signal signal :location location :key key :types [type]
      :unknown? (boolean (descriptor-contains? type :unknown))
      :invalid? (boolean (descriptor-contains? type :invalid))
      :evidence [(evidence source form kind)]})))

(defn- inferred-attributes
  ([source signal form env call-form]
   (inferred-attributes source signal :attributes form env call-form))
  ([source signal location form env call-form]
   (if-not (map? form)
     {:entries []
      :dynamic-keys [{:signal signal :location location
                      :evidence [(evidence source call-form :dynamic)]}]}
     (reduce
      (fn [result [key value]]
        (if-let [key (literal-key key false)]
          (update result :entries conj
                  (entry source signal location key value env call-form))
          (update result :dynamic-keys conj
                  {:signal signal :location location
                   :evidence [(evidence source call-form :dynamic)]})))
      {:entries [] :dynamic-keys []} form))))

(defn- opts-attributes [source signal form env call-form]
  (if (and (map? form) (contains? form :attributes))
    (inferred-attributes source signal (:attributes form) env call-form)
    {:entries [] :dynamic-keys []}))

(defn- empty-found [] {:entries [] :dynamic-keys []})

(defn- merge-found [left right]
  (-> left
      (update :entries into (:entries right))
      (update :dynamic-keys into (:dynamic-keys right))))

(defn- binding-symbols [form]
  (cond
    (symbol? form)
    (if (= '& form) #{} #{form})

    (vector? form)
    (reduce into #{} (map binding-symbols form))

    (map? form)
    (reduce
     (fn [result [binding lookup]]
       (let [directive (when (keyword? binding) (name binding))]
         (cond
           (= "as" directive) (into result (binding-symbols lookup))
           (= "or" directive) result
           (contains? #{"keys" "syms" "strs"} directive)
           (into result
                 (map (fn [item] (symbol (name item))) lookup))
           :else (into result (binding-symbols binding)))))
     #{} form)

    :else #{}))

(defn- with-locals [env bindings]
  (update env :locals into (binding-symbols bindings)))

(declare walk-evidence)

(defn- walk-forms [source forms env]
  (reduce (fn [found form]
            (merge-found found (walk-evidence source form env)))
          (empty-found) forms))

(defn- walk-binding-vector [source bindings env]
  (loop [remaining (seq bindings)
         current-env env
         found (empty-found)]
    (if (and remaining (next remaining))
      (let [binding (first remaining)
            init (second remaining)]
        (recur (nnext remaining)
               (with-locals current-env binding)
               (merge-found found (walk-evidence source init current-env))))
      [found current-env])))

(defn- walk-fn-tail [source tail env]
  (let [named? (symbol? (first tail))
        env (if named? (with-locals env (first tail)) env)
        arities (if named? (rest tail) tail)]
    (if (vector? (first arities))
      (walk-forms source (rest arities)
                  (with-locals env (first arities)))
      (reduce
       (fn [found arity]
         (if (and (seq? arity) (vector? (first arity)))
           (merge-found found
                        (walk-forms source (rest arity)
                                    (with-locals env (first arity))))
           found))
       (empty-found) arities))))

(defn- core-form? [env form names]
  (contains? names (resolve-symbol env (first form))))

(def ^:private sequential-binding-forms
  '#{let let* loop loop* with-open when-let when-some when-first dotimes
     clojure.core/let clojure.core/let* clojure.core/loop
     clojure.core/loop* clojure.core/with-open clojure.core/when-let
     clojure.core/when-some
     clojure.core/when-first clojure.core/dotimes})

(def ^:private conditional-binding-forms
  '#{if-let if-some clojure.core/if-let clojure.core/if-some})

(def ^:private comprehension-forms
  '#{for doseq clojure.core/for clojure.core/doseq})

(def ^:private fn-forms '#{fn fn* clojure.core/fn clojure.core/fn*})
(def ^:private letfn-forms '#{letfn letfn* clojure.core/letfn
                              clojure.core/letfn*})
(def ^:private defn-forms '#{defn defn- clojure.core/defn
                             clojure.core/defn-})

(defn- scope-options-attributes [source signal form env call-form]
  (let [unknown {:signal signal :location :scope-attributes
                 :evidence [(evidence source call-form :dynamic)]}]
    (if-not (map? form)
      {:entries [] :dynamic-keys [unknown]}
      (let [found (if (contains? form :attributes)
                    (inferred-attributes source signal :scope-attributes
                                         (:attributes form) env call-form)
                    (empty-found))]
        ;; A computed option key can evaluate to :attributes, including beside
        ;; a literal key. Retain any known evidence, but do not claim it is the
        ;; complete scope schema when the analyzer cannot know that.
        (if (some #(nil? (literal-key % false)) (keys form))
          (update found :dynamic-keys conj unknown)
          found)))))

(defn- call-evidence [source form env]
  (case (get call-kinds (resolve-symbol env (first form)))
    :with-span
    (let [binding (second form)]
      (if (and (vector? binding) (= 4 (count binding)))
        (opts-attributes source :span (nth binding 3) env form)
        (empty-found)))

    :start-span
    (if (>= (count form) 4)
      (opts-attributes source :span (nth form 3) env form)
      (empty-found))

    :set-attribute
    (if (>= (count form) 4)
      (if-let [key (literal-key (nth form 2) false)]
        {:entries [(entry source :span key (nth form 3) env form)]
         :dynamic-keys []}
        {:entries []
         :dynamic-keys [{:signal :span :location :attributes
                         :evidence [(evidence source form :dynamic)]}]})
      (empty-found))

    :set-attributes
    (if (>= (count form) 3)
      (inferred-attributes source :span (nth form 2) env form)
      (empty-found))

    :metric
    (if (>= (count form) 4)
      (inferred-attributes source :metric (nth form 3) env form)
      (empty-found))

    :log
    (if (and (>= (count form) 3) (map? (nth form 2))
             (contains? (nth form 2) :attributes))
      (inferred-attributes source :log (:attributes (nth form 2)) env form)
      (empty-found))

    :resource
    (if (>= (count form) 2)
      (inferred-attributes source :resource (nth form 1) env form)
      (empty-found))

    :tracer-scope
    (if (= 3 (count form))
      (scope-options-attributes source :span (nth form 2) env form)
      (empty-found))

    :meter-scope
    (if (= 3 (count form))
      (scope-options-attributes source :metric (nth form 2) env form)
      (empty-found))

    :logger-scope
    (if (= 3 (count form))
      (scope-options-attributes source :log (nth form 2) env form)
      (empty-found))

    (empty-found)))

(defn- walk-evidence [source form env]
  (cond
    (not (coll? form))
    (empty-found)

    (map? form)
    (walk-forms source (mapcat identity form) env)

    (vector? form)
    (walk-forms source form env)

    (not (seq? form))
    (walk-forms source form env)

    (= 'quote (resolve-symbol env (first form)))
    (empty-found)

    (= :with-span (get call-kinds (resolve-symbol env (first form))))
    (let [binding (second form)
          own (call-evidence source form env)]
      (if (and (vector? binding) (<= 3 (count binding) 4))
        (-> own
            (merge-found (walk-forms source (rest binding) env))
            (merge-found
             (walk-forms source (drop 2 form)
                         (with-locals env (first binding)))))
        (merge-found own (walk-forms source (rest form) env))))

    (core-form? env form sequential-binding-forms)
    (let [bindings (second form)]
      (if (vector? bindings)
        (let [[inits body-env] (walk-binding-vector source bindings env)]
          (merge-found inits (walk-forms source (drop 2 form) body-env)))
        (walk-forms source (rest form) env)))

    (core-form? env form conditional-binding-forms)
    (let [bindings (second form)]
      (if (vector? bindings)
        (let [[inits then-env] (walk-binding-vector source bindings env)]
          (-> inits
              (merge-found (walk-evidence source (nth form 2 nil) then-env))
              (merge-found (walk-evidence source (nth form 3 nil) env))))
        (walk-forms source (rest form) env)))

    (core-form? env form comprehension-forms)
    (let [bindings (second form)]
      (if (vector? bindings)
        (loop [remaining (seq bindings)
               body-env env
               found (empty-found)]
          (if (and remaining (next remaining))
            (let [binding (first remaining)
                  expression (second remaining)]
              (if (keyword? binding)
                (if (= :let binding)
                  (let [[nested nested-env]
                        (if (vector? expression)
                          (walk-binding-vector source expression body-env)
                          [(walk-evidence source expression body-env) body-env])]
                    (recur (nnext remaining) nested-env
                           (merge-found found nested)))
                  (recur (nnext remaining) body-env
                         (merge-found found
                                      (walk-evidence source expression body-env))))
                (recur (nnext remaining)
                       (with-locals body-env binding)
                       (merge-found found
                                    (walk-evidence source expression body-env)))))
            (merge-found found
                         (walk-forms source (drop 2 form) body-env))))
        (walk-forms source (rest form) env)))

    (core-form? env form fn-forms)
    (walk-fn-tail source (rest form) env)

    (core-form? env form letfn-forms)
    (let [specs (second form)]
      (if (vector? specs)
        (let [names (keep #(when (seq? %) (first %)) specs)
              body-env (with-locals env (vec names))
              definitions
              (reduce (fn [found spec]
                        (if (seq? spec)
                          (merge-found
                           found
                           (walk-fn-tail source (rest spec) body-env))
                          found))
                      (empty-found) specs)]
          (merge-found definitions
                       (walk-forms source (drop 2 form) body-env)))
        (walk-forms source (rest form) env)))

    (core-form? env form defn-forms)
    (let [tail (drop-while #(or (string? %) (map? %)) (drop 2 form))]
      (walk-fn-tail source (cons (second form) tail) env))

    (and (contains? '#{catch clojure.core/catch}
                    (resolve-symbol env (first form)))
         (>= (count form) 3))
    (walk-forms source (drop 3 form)
                (with-locals env (nth form 2)))

    :else
    (merge-found (call-evidence source form env)
                 (walk-forms source (rest form) env))))

(defn- evidence-sort-key [item]
  [(:source item) (or (:line item) 0) (str (:kind item))])

(defn- merge-entry-group [entries]
  (let [types (type-sort (mapcat :types entries))
        concrete (remove #{:unknown :invalid} types)]
    {:signal (:signal (first entries))
     :location (:location (first entries))
     :key (:key (first entries))
     :types types
     :unknown? (boolean (some :unknown? entries))
     :invalid? (boolean (some :invalid? entries))
     :conflict? (> (count concrete) 1)
     :evidence (->> entries (mapcat :evidence) distinct
                    (sort-by evidence-sort-key) vec)}))

(defn- canonical-fragment [sources entries dynamic-keys]
  (let [entries (->> entries
                     (group-by (juxt :signal :location :key))
                     vals (map merge-entry-group)
                     (sort-by (juxt (comp str :signal)
                                    (comp str :location) :key)) vec)
        dynamic-keys (->> dynamic-keys distinct
                          (sort-by (fn [item]
                                     [(str (:signal item))
                                      (str (:location item))
                                      (evidence-sort-key
                                       (first (:evidence item)))])) vec)]
    {:schema schema-id
     :sources (vec (sort (distinct sources)))
     :entries entries
     :dynamic-keys? (boolean (seq dynamic-keys))
     :dynamic-keys dynamic-keys}))

(defn analyze-form
  "Analyze one already-read form under an explicit source namespace environment."
  [{:keys [source aliases refers] :or {aliases {} refers {}}} form]
  (let [source (canonical-source source)
        found (walk-evidence source form
                             {:aliases aliases :refers refers :locals #{}})]
    (canonical-fragment [source] (:entries found) (:dynamic-keys found))))

(defn analyze-source
  "Analyze every OTel API call in one value returned by read-forms."
  [{:keys [source forms]}]
  (let [initial-env (assoc (require-env forms) :locals #{})
        result
        (reduce
         (fn [{:keys [found env]} form]
           (let [item (walk-evidence source form env)
                 op (when (seq? form) (resolve-symbol env (first form)))
                 defined (when (and (contains? '#{def defn defn- defmacro
                                                  clojure.core/def
                                                  clojure.core/defn
                                                  clojure.core/defn-
                                                  clojure.core/defmacro} op)
                                          (symbol? (second form)))
                                   (second form))]
             {:found (merge-found found item)
              :env (if defined (with-locals env defined) env)}))
         {:found (empty-found) :env initial-env} forms)
        found (:found result)]
    (canonical-fragment [source] (:entries found) (:dynamic-keys found))))

(def ^:private scalar-types
  #{:string :boolean :int64 :double :bytes :empty :unknown :invalid})

(defn- type-descriptor? [descriptor depth]
  (and (<= depth 16)
       (or (contains? scalar-types descriptor)
           (and (map? descriptor)
                (= 1 (count descriptor))
                (cond
                  (contains? descriptor :array)
                  (and (vector? (:array descriptor))
                       (every? #(type-descriptor? % (inc depth))
                               (:array descriptor)))

                  (contains? descriptor :kvlist)
                  (let [entries (:kvlist descriptor)]
                    (or (contains? #{:unknown :invalid} entries)
                        (and (map? entries)
                             (every? string? (keys entries))
                             (every? #(type-descriptor? % (inc depth))
                                     (vals entries)))))

                  :else false)))))

(defn- evidence? [sources item]
  (and (map? item)
       (contains? #{#{:source :kind :authority}
                    #{:source :line :kind :authority}}
                  (set (keys item)))
       (contains? sources (:source item))
       (contains? #{:literal :constructor :cast :dynamic} (:kind item))
       (contains? #{:literal :expression :unknown} (:authority item))
       (= (:authority item)
          (case (:kind item)
            :literal :literal
            (:constructor :cast) :expression
            :dynamic :unknown))
       (or (not (contains? item :line))
           (and (integer? (:line item)) (pos? (:line item))))))

(defn- bool? [value]
  (or (true? value) (false? value)))

(defn- expected-entry-flags [types]
  (let [concrete (remove #{:unknown :invalid} types)]
    {:unknown? (boolean (some #(descriptor-contains? % :unknown) types))
     :invalid? (boolean (some #(descriptor-contains? % :invalid) types))
     :conflict? (> (count concrete) 1)}))

(defn- entry? [sources item]
  (and (map? item)
       (= #{:signal :location :key :types :unknown? :invalid? :conflict?
            :evidence}
          (set (keys item)))
       (contains? #{:span :metric :log :resource} (:signal item))
       (or (= :attributes (:location item))
           (and (= :scope-attributes (:location item))
                (contains? #{:span :metric :log} (:signal item))))
       (string? (:key item))
       (not (empty? (:key item)))
       (vector? (:types item))
       (not (empty? (:types item)))
       (every? #(type-descriptor? % 0) (:types item))
       (every? bool? [(:unknown? item) (:invalid? item) (:conflict? item)])
       (= (select-keys item [:unknown? :invalid? :conflict?])
          (expected-entry-flags (:types item)))
       (vector? (:evidence item))
       (not (empty? (:evidence item)))
       (<= (count (:evidence item)) 4096)
       (every? #(evidence? sources %) (:evidence item))))

(defn- dynamic-key? [sources item]
  (and (map? item)
       (= #{:signal :location :evidence} (set (keys item)))
       (contains? #{:span :metric :log :resource} (:signal item))
       (or (= :attributes (:location item))
           (and (= :scope-attributes (:location item))
                (contains? #{:span :metric :log} (:signal item))))
       (vector? (:evidence item))
       (not (empty? (:evidence item)))
       (<= (count (:evidence item)) 4096)
       (every? #(evidence? sources %) (:evidence item))))

(defn validate
  "Validate the closed v1 envelope and return it."
  [fragment]
  (let [sources (set (:sources fragment))]
    (when-not (and (map? fragment)
                 (= #{:schema :sources :entries :dynamic-keys? :dynamic-keys}
                    (set (keys fragment)))
                 (= schema-id (:schema fragment))
                 (vector? (:sources fragment))
                 (<= (count (:sources fragment)) 4096)
                 (every? #(= % (canonical-source %)) (:sources fragment))
                 (vector? (:entries fragment))
                 (<= (count (:entries fragment)) 65536)
                 (every? #(entry? sources %) (:entries fragment))
                 (vector? (:dynamic-keys fragment))
                 (<= (count (:dynamic-keys fragment)) 65536)
                 (every? #(dynamic-key? sources %) (:dynamic-keys fragment))
                 (= (boolean (seq (:dynamic-keys fragment)))
                    (:dynamic-keys? fragment))
                 (= fragment
                    (canonical-fragment (:sources fragment)
                                        (:entries fragment)
                                        (:dynamic-keys fragment))))
      (problem! "invalid attribute schema fragment" {:fragment fragment})))
  fragment)

(defn merge-fragments
  "Merge fragments without hiding unknown or conflicting evidence."
  [& fragments]
  (let [fragments (mapv validate fragments)]
    (canonical-fragment (mapcat :sources fragments)
                        (mapcat :entries fragments)
                        (mapcat :dynamic-keys fragments))))

(defn analyze-sources [sources]
  (apply merge-fragments (map analyze-source sources)))

(defn render
  "Canonical EDN suitable for a generated classpath resource."
  [fragment]
  (str (pr-str (validate fragment)) "\n"))
