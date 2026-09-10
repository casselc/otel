(ns otel.attribute-schema-test
  (:require [clojure.test :refer [deftest is testing]]
            [otel.any-value :as any]
            [otel.attribute-schema :as schema]
            [otel.attribute-schema.main :as main]))

(def aliases {'trace 'otel.trace 'metrics 'otel.metrics 'logs 'otel.logs
              'res 'otel.resource 'any 'otel.any-value})

(defn- entry-for [fragment signal key]
  (first (filter #(and (= signal (:signal %)) (= key (:key %)))
                 (:entries fragment))))

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
