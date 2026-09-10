(ns otel.attribute-schema-test
  (:require [clojure.test :refer [deftest is testing]]
            [otel.any-value :as any]
            [otel.attribute-schema :as schema]
            [otel.attribute-schema.main :as main]
            [otel.sdk.logs :as sdk-logs]
            [otel.sdk.metrics :as sdk-metrics]
            [otel.sdk.tracer :as sdk-tracer]))

(def aliases {'trace 'otel.trace 'metrics 'otel.metrics 'logs 'otel.logs
              'res 'otel.resource 'any 'otel.any-value})

(defn- entry-for [fragment signal key]
  (first (filter #(and (= signal (:signal %)) (= key (:key %)))
                 (:entries fragment))))

(defn- evidence-locations [fragment]
  (set (concat (map (juxt :signal :location :key) (:entries fragment))
               (map (juxt :signal :location (constantly :dynamic))
                    (:dynamic-keys fragment)))))

(deftest value-forms-use-the-canonical-any-value-algebra
  (is (= :string (:type (schema/infer-value-form "x"))))
  (is (= :string (:type (schema/infer-value-form :ready))))
  (is (= :boolean (:type (schema/infer-value-form false))))
  (is (= :int64 (:type (schema/infer-value-form any/min-int64))))
  (is (= :int64 (:type (schema/infer-value-form any/max-int64))))
  (is (= :invalid (:type (schema/infer-value-form (inc any/max-int64)))))
  (is (= :double (:type (schema/infer-value-form 1.5))))
  (is (= :invalid (:type (schema/infer-value-form nil))))
  (is (= :empty (:type (schema/infer-value-form 'any/empty-value aliases))))
  (is (= :bytes (:type (schema/infer-value-form '(any/bytes payload) aliases))))
  (is (= :int64 (:type (schema/infer-value-form '(long dynamic) aliases))))
  (is (= :double (:type (schema/infer-value-form '(double dynamic) aliases))))
  (is (= :string (:type (schema/infer-value-form '(str dynamic) aliases))))
  (is (= :unknown (:type (schema/infer-value-form 'dynamic aliases))))
  (is (= {:array [:int64 :string]}
         (:type (schema/infer-value-form '[1 "x"] aliases))))
  (is (= {:array [:int64 :string]}
         (:type (schema/infer-value-form '(quote (1 "x")) aliases))))
  (is (= {:kvlist {"nested" :boolean}}
         (:type (schema/infer-value-form '{:nested true} aliases))))
  (is (= {:kvlist :invalid}
         (:type (schema/infer-value-form '{:a 1 "a" 2} aliases)))))

(def representative-source
  "(ns app.telemetry
     (:require [otel.any-value :as any]
               [otel.logs :as logs]
               [otel.metrics :as metrics]
               [otel.resource :as res]
               [otel.trace :as trace]))
   (trace/with-span [span tracer \"request\"
                     {:attributes {:http.request.method \"GET\"
                                   :request.id request-id
                                   :payload (any/bytes payload)}}]
     (trace/set-attribute! span :http.response.status_code (long status))
     (trace/set-attribute! span dynamic-key \"hidden\"))
   (metrics/record! latency 1.0 {:http.request.method \"GET\"})
   (logs/emit! logger {:body \"done\" :attributes {:attempt 2}})
   (res/resource {:service.name \"checkout\"})
   (other/set-attribute! span :false.positive 1)")

(deftest source-analysis-finds-only-exact-otel-api-calls
  (let [fragment (schema/analyze-source
                  (schema/read-forms "src/app/telemetry.clj"
                                     representative-source))]
    (is (= #{[:span "http.request.method"] [:span "request.id"]
             [:span "payload"] [:span "http.response.status_code"]
             [:metric "http.request.method"] [:log "attempt"]
             [:resource "service.name"]}
           (set (map (juxt :signal :key) (:entries fragment)))))
    (is (= [:unknown] (:types (entry-for fragment :span "request.id"))))
    (is (= [:bytes] (:types (entry-for fragment :span "payload"))))
    (is (= [:int64]
           (:types (entry-for fragment :span "http.response.status_code"))))
    (is (:dynamic-keys? fragment))
    (is (= ["src/app/telemetry.clj"] (:sources fragment)))
    (is (not-any? #(= "false.positive" (:key %)) (:entries fragment)))))

(deftest every-v1-api-boundary-uses-its-attribute-position
  (let [source
        "(ns app.boundaries
           (:require [otel.logs :as logs]
                     [otel.metrics :as metrics]
                     [otel.resource :as resource]
                     [otel.trace :as trace]))
         (trace/start-span tracer \"op\" {:attributes {:start true}})
         (trace/set-attributes! span {:many 1})
         (metrics/add! counter 1 {:counter \"add\"})
         (metrics/add-delta! up-down -1 {:up-down \"delta\"})
         (metrics/record! histogram 2 {:histogram \"record\"})
         (metrics/set-value! gauge 3 {:gauge \"set\"})
         (metrics/observe! observer 4 {:observer \"observe\"})
         (logs/emit! logger {:attributes {:log \"emit\"}})
         (resource/resource {:service.name \"api\"})"
        fragment (schema/analyze-source
                  (schema/read-forms "src/app/boundaries.clj" source))]
    (is (= #{[:span "start"] [:span "many"]
             [:metric "counter"] [:metric "up-down"]
             [:metric "histogram"] [:metric "gauge"]
             [:metric "observer"] [:log "log"]
             [:resource "service.name"]}
           (set (map (juxt :signal :key) (:entries fragment)))))))

(deftest scope-acquisition-resolves-exact-calls-arities-and-aliases
  (let [source
        "(ns app.scopes
           (:require [otel.sdk.logs :as sdk-logs]
                     [otel.sdk.metrics :refer [get-meter]]
                     [otel.sdk.tracer :as sdk-tracer]))
         (sdk-tracer/get-tracer tracer-provider
           {:name \"trace-lib\" :attributes {:scope.trace true}})
         (get-meter meter-provider
           {:name \"metric-lib\" :attributes {:scope.metric 7}})
         (sdk-logs/get-logger logger-provider
           {:name \"log-lib\" :attributes {:scope.log \"on\"}})
         (other/get-tracer tracer-provider
           {:attributes {:false.positive :other-namespace}})
         (sdk-tracer/get-tracer tracer-provider
           {:attributes {:false.arity :extra}} ignored)"
        fragment (schema/analyze-source
                  (schema/read-forms "src/app/scopes.clj" source))]
    (is (= #{[:span :scope-attributes "scope.trace"]
             [:metric :scope-attributes "scope.metric"]
             [:log :scope-attributes "scope.log"]}
           (evidence-locations fragment)))
    (is (= [:boolean] (:types (entry-for fragment :span "scope.trace"))))
    (is (= [:int64] (:types (entry-for fragment :metric "scope.metric"))))
    (is (= [:string] (:types (entry-for fragment :log "scope.log"))))
    (is (= fragment (schema/validate fragment)))))

(deftest dynamic-scope-options-remain-explicit-unknown-evidence
  (let [source
        "(ns app.dynamic-scopes
           (:require [otel.sdk.logs :refer [get-logger]]
                     [otel.sdk.metrics :as metrics]
                     [otel.sdk.tracer :as tracer]))
         (tracer/get-tracer tracer-provider dynamic-options)
         (metrics/get-meter meter-provider {:name \"m\" :attributes attrs})
         (get-logger logger-provider {option-key attrs})
         (get-logger logger-provider
           {:attributes {:known true} option-key attrs})"
        fragment (schema/analyze-source
                  (schema/read-forms "src/app/dynamic_scopes.clj" source))]
    (is (= [:boolean] (:types (entry-for fragment :log "known"))))
    (is (= #{[:span :scope-attributes :dynamic]
             [:metric :scope-attributes :dynamic]
             [:log :scope-attributes :dynamic]
             [:log :scope-attributes "known"]}
           (evidence-locations fragment)))
    (is (:dynamic-keys? fragment))))

(deftest lexical-bindings-shadow-referred-api-vars
  (let [source
        "(ns app.shadowed
           (:require [otel.sdk.logs :refer [get-logger]]
                     [otel.sdk.metrics :as sdk-metrics :refer [get-meter]]
                     [otel.sdk.tracer :refer [get-tracer]]
                     [otel.trace :refer [set-attribute! with-span]]))
         (let [get-tracer
               (get-tracer tp {:attributes {:refer.visible-in-init true}})]
           (get-tracer tp {:attributes {:shadow.let true}}))
         ((fn [{:keys [get-meter]}]
            (get-meter mp {:attributes {:shadow.fn true}})) values)
         (loop [[get-logger] loggers]
           (get-logger lp {:attributes {:shadow.loop true}}))
         (letfn [(get-tracer [provider options] local-tracer)]
           (get-tracer tp {:attributes {:shadow.letfn true}}))
         (defn local-wrapper [{:keys [get-tracer]}]
           (get-tracer tp {:attributes {:shadow.defn true}}))
         (let [set-attribute! local-set]
           (set-attribute! span :shadow.existing-api true))
         (if-let [get-meter maybe-meter]
           (get-meter mp {:attributes {:shadow.if-then true}})
           (get-meter mp {:attributes {:refer.visible-in-else true}}))
         (doseq [{:keys [get-logger]} loggers]
           (get-logger lp {:attributes {:shadow.doseq true}}))
         (with-span [get-meter tracer \"scope\"
                     {:attributes {:span.binding-boundary true}}]
           (get-meter mp {:attributes {:shadow.with-span true}}))
         (let [sdk-metrics local-value]
           (sdk-metrics/get-meter mp
             {:attributes {:alias.still-qualified true}}))"
        fragment (schema/analyze-source
                  (schema/read-forms "src/app/shadowed.clj" source))]
    (is (= #{[:metric :scope-attributes "alias.still-qualified"]
             [:metric :scope-attributes "refer.visible-in-else"]
             [:span :scope-attributes "refer.visible-in-init"]
             [:span :attributes "span.binding-boundary"]}
           (evidence-locations fragment)))))

(deftest top-level-definitions-shadow-referred-api-vars-after-definition
  (let [source
        "(ns app.top-level-shadow
           (:require [otel.sdk.tracer :refer [get-tracer]]))
         (get-tracer tp {:attributes {:before.definition true}})
         (def get-tracer local-tracer)
         (get-tracer tp {:attributes {:after.definition false}})"
        fragment (schema/analyze-source
                  (schema/read-forms "src/app/top_level_shadow.clj" source))]
    (is (= #{[:span :scope-attributes "before.definition"]}
           (evidence-locations fragment)))))

(defn- runtime-scalar-type [value]
  (cond
    (string? value) :string
    (integer? value) :int64
    (or (true? value) (false? value)) :boolean))

(deftest inferred-scope-schema-agrees-with-runtime-normalization
  (let [attributes {:scope.string "literal" :scope.int 7 :scope.boolean true}
        source
        "(ns app.runtime-agreement
           (:require [otel.sdk.logs :as logs]
                     [otel.sdk.metrics :as metrics]
                     [otel.sdk.tracer :as tracer]))
         (tracer/get-tracer tp {:name \"trace\"
                                :attributes {:scope.string \"literal\"
                                             :scope.int 7
                                             :scope.boolean true}})
         (metrics/get-meter mp {:name \"metric\"
                                :attributes {:scope.string \"literal\"
                                             :scope.int 7
                                             :scope.boolean true}})
         (logs/get-logger lp {:name \"log\"
                              :attributes {:scope.string \"literal\"
                                           :scope.int 7
                                           :scope.boolean true}})"
        fragment (schema/analyze-source
                  (schema/read-forms "src/app/runtime_agreement.clj" source))
        scopes [(:scope (sdk-tracer/get-tracer
                         (sdk-tracer/tracer-provider {})
                         {:name "trace" :attributes attributes}))
                (:scope (sdk-metrics/get-meter
                         (sdk-metrics/meter-provider {})
                         {:name "metric" :attributes attributes}))
                (:scope (sdk-logs/get-logger
                         (sdk-logs/logger-provider {})
                         {:name "log" :attributes attributes}))]
        runtime-manifest
        (into {}
              (mapcat (fn [[signal scope]]
                        (map (fn [[key value]]
                               [[signal :scope-attributes key]
                                (runtime-scalar-type value)])
                             (:attributes scope)))
                      (map vector [:span :metric :log] scopes)))
        inferred-manifest
        (into {} (map (fn [{:keys [signal location key types]}]
                        [[signal location key] (first types)])
                      (:entries fragment)))]
    (is (= runtime-manifest inferred-manifest))))

(deftest aliases-and-referred-api-vars-resolve-without-evaluation
  (let [source "(ns app.core (:require [otel.trace :refer [set-attribute!]]))
                (set-attribute! span :safe (throw (ex-info \"must not run\" {})))"
        fragment (schema/analyze-source
                  (schema/read-forms "src/app/core.clj" source))]
    (is (= [:unknown] (:types (entry-for fragment :span "safe"))))))

(deftest source-reader-disables-evaluating-reader-forms
  (is (thrown? Exception
               (schema/read-forms
                "src/app/unsafe.clj"
                "#=(throw (ex-info \"reader must not run\" {}))"))))

(deftest conflicts-and-unknowns-remain-visible
  (let [one (schema/analyze-form {:source "src/a.clj" :aliases aliases}
                                 '(trace/set-attribute! span :shared "x"))
        two (schema/analyze-form {:source "src/b.clj" :aliases aliases}
                                 '(trace/set-attribute! span :shared 1))
        three (schema/analyze-form {:source "src/c.clj" :aliases aliases}
                                   '(trace/set-attribute! span :shared dynamic))
        entry (entry-for (schema/merge-fragments one two three) :span "shared")]
    (is (= [:int64 :string :unknown] (:types entry)))
    (is (:conflict? entry))
    (is (:unknown? entry))))

(deftest generation-is-order-independent-and-byte-deterministic
  (let [a (schema/read-forms "src/a.clj"
                             "(otel.trace/set-attribute! span :z 1)")
        b (schema/read-forms "src/b.clj"
                             "(otel.trace/set-attribute! span :a \"x\")")
        left (schema/analyze-sources [a b])
        right (schema/analyze-sources [b a])]
    (is (= left right))
    (is (= (schema/render left) (schema/render right)))
    (is (not (.contains (schema/render left) (System/getProperty "user.dir"))))))

(deftest generator-reads-only-explicit-relative-sources
  (let [contents {"root/src/a.clj" "(otel.trace/set-attribute! span :a 1)"
                  "root/src/b.clj" "(otel.trace/set-attribute! span :b true)"}]
    (with-redefs [slurp #(get contents %)]
      (let [left (main/generate "root" ["src/a.clj" "src/b.clj"])
            right (main/generate "root" ["src/b.clj" "src/a.clj"])]
        (is (= (schema/render left) (schema/render right)))))))

(deftest generator-rejects-unsafe-paths-before-filesystem-access
  (let [reads (atom [])]
    (with-redefs [slurp (fn [path]
                          (swap! reads conj path)
                          "(otel.trace/set-attribute! span :wrong true)")]
      (doseq [path ["/tmp/a.clj" "../a.clj" "src/../a.clj" "src\\a.clj"]]
        (is (thrown? Exception (main/generate "root" [path]))))
      (is (empty? @reads)))))

(deftest absolute-and-parent-relative-provenance-is-rejected
  (doseq [path ["/tmp/a.clj" "../a.clj" "src/../a.clj" "src\\a.clj"]]
    (is (thrown? Exception (schema/read-forms path "1")))))

(deftest auto-resolved-keys-are-unknown-not-misattributed
  (let [fragment (schema/analyze-source
                  (schema/read-forms
                   "src/app/core.clj"
                   "(ns app.core (:require [otel.trace :as trace]))
                    (trace/set-attribute! span ::local 1)"))]
    (is (empty? (:entries fragment)))
    (is (:dynamic-keys? fragment))))

(deftest alias-qualified-auto-keywords-fail-closed-without-alias-leaks
  (let [reader-ns (the-ns 'otel.attribute-schema.reader)]
    (is (empty? (ns-aliases reader-ns)))
    (doseq [alias ["str" "any" "unknown"]]
      (let [secret (str "secret-" alias)
            aliased-require (case alias
                              "str" "[clojure.string :as str]"
                              "any" "[otel.any-value :as any]"
                              "")
            source (str "(ns app.core (:require [otel.trace :as trace] "
                        aliased-require ")) "
                        "(trace/set-attribute! span ::" alias "/" secret " 1)")
            error (try
                    (schema/read-forms "src/app/core.clj" source)
                    nil
                    (catch Exception error error))]
        (is (some? error))
        (is (= :unsupported-source-syntax (:reason (ex-data error))))
        (is (not (.contains (pr-str (ex-data error)) secret)))
        (is (not (.contains (ex-message error) secret)))))
    (is (empty? (ns-aliases reader-ns)))))

(deftest validator-rejects-noncanonical-or-open-ended-resources
  (let [valid (schema/analyze-form
               {:source "src/a.clj" :aliases aliases}
               '(trace/set-attribute! span :a 1))]
    (is (= valid (schema/validate valid)))
    (is (thrown? Exception (schema/validate (assoc valid :extra true))))
    (is (thrown? Exception
                 (schema/validate
                  (assoc-in valid [:entries 0 :unknown?] true))))
    (is (thrown? Exception
                 (schema/validate
                  (assoc-in valid [:entries 0 :evidence 0 :authority]
                            :unknown))))
    (is (thrown? Exception
                 (schema/validate
                  (assoc-in valid [:entries 0 :types] [:made-up]))))))

(deftest validator-keeps-scope-attributes-on-instrumentation-signals
  (let [valid (schema/analyze-form
               {:source "src/a.clj"
                :aliases {'tracer 'otel.sdk.tracer}}
               '(tracer/get-tracer provider
                  {:attributes {:scope.attribute true}}))]
    (is (= valid (schema/validate valid)))
    (is (thrown? Exception
                 (schema/validate
                  (assoc-in valid [:entries 0 :signal] :resource))))))
