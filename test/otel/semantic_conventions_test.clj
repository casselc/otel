(ns otel.semantic-conventions-test
  (:require [clojure.test :refer [deftest is]]
            [otel.attribute-schema :as schema]
            [otel.semantic-conventions :as semconv]))

(def aliases {'res 'otel.resource 'logs 'otel.logs 'trace 'otel.trace})

(defn- analyze [source form]
  (schema/analyze-form {:source source :aliases aliases} form))

(def expected-entries
  #{["exception.message" [[:log :attributes] [:span :attributes]]
     :string :stable]
    ["exception.type" [[:log :attributes] [:span :attributes]]
     :string :stable]
    ["host.arch" [[:resource :attributes]] :string :development]
    ["os.type" [[:resource :attributes]] :string :development]
    ["process.pid" [[:resource :attributes]] :int64 :release-candidate]
    ["process.runtime.description" [[:resource :attributes]]
     :string :release-candidate]
    ["process.runtime.name" [[:resource :attributes]]
     :string :release-candidate]
    ["process.runtime.version" [[:resource :attributes]]
     :string :release-candidate]
    ["service.name" [[:resource :attributes]] :string :stable]
    ["telemetry.sdk.language" [[:resource :attributes]] :string :stable]
    ["telemetry.sdk.name" [[:resource :attributes]] :string :stable]
    ["telemetry.sdk.version" [[:resource :attributes]] :string :stable]})

(deftest pinned-registry-loads-with-canonical-provenance
  (let [registry (semconv/load-registry)]
    (is (= semconv/schema-id (:schema registry)))
    (is (= "v1.44.0" (get-in registry [:provenance :release])))
    (is (= "e10a930844c6951757a43b849d364f7d056ac32b"
           (get-in registry [:provenance :commit])))
    (is (= 6 (count (get-in registry [:provenance :files]))))
    (is (= expected-entries
           (set (map (juxt :key :locations :type :stability)
                     (:entries registry)))))
    (is (= (mapv :key (:entries registry))
           (vec (sort (map :key (:entries registry))))))
    (is (= :int64 (:type (semconv/lookup "process.pid"))))
    (is (= :stable (:stability (semconv/lookup "exception.type"))))
    (is (= "exception.type"
           (:key (semconv/lookup "exception.type" [:log :attributes]))))
    (is (nil? (semconv/lookup "exception.type" [:resource :attributes])))
    (is (nil? (semconv/lookup "exception.data")))))

(deftest checker-accepts-standard-types-and-preserves-the-fragment
  (let [fragment
        (schema/analyze-source
         (schema/read-forms
          "src/app/telemetry.clj"
          "(ns app.telemetry
             (:require [otel.logs :as logs] [otel.resource :as res]))
           (res/resource {:service.name \"checkout\"
                          :telemetry.sdk.name \"opentelemetry\"
                          :telemetry.sdk.language \"jolt\"
                          :telemetry.sdk.version \"0.1.0\"
                          :process.pid (long pid)
                          :process.runtime.name \"Chez Scheme\"
                          :process.runtime.version (str runtime-version)
                          :process.runtime.description description
                          :host.arch \"arm64\"
                          :os.type \"darwin\"})
           (logs/emit! logger {:attributes {:exception.type \"Error\"
                                            :exception.message (str message)
                                            :exception.data data}})"))]
    (is (identical? fragment (semconv/check fragment)))))

(deftest concrete-literals-and-casts-conflict-deterministically
  (doseq [[form expected actual]
          [['(res/resource {:service.name 42}) :string :int64]
           ['(res/resource {:process.pid (double pid)}) :int64 :double]
           ['(logs/emit! logger {:attributes {:exception.type false}})
            :string :boolean]]]
    (let [fragment (analyze "src/app/private.clj" form)
          error (try (semconv/check fragment) nil
                     (catch Exception error error))]
      (is (= :mismatch (:otel.semantic-conventions/error (ex-data error))))
      (is (= :type-mismatch (:reason (ex-data error))))
      (is (= expected (:expected (ex-data error))))
      (is (= actual (:actual (ex-data error))))
      (is (not (.contains (pr-str (ex-data error)) "private.clj"))))))

(deftest no-int64-to-double-widening
  (let [fragment
        (schema/merge-fragments
         (analyze "src/a.clj" '(res/resource {:process.pid 7}))
         (analyze "src/b.clj" '(res/resource {:process.pid 7.0})))
        error (try (semconv/check fragment) nil
                   (catch Exception error error))]
    (is (= [:double :int64]
           (:types (first (:entries fragment)))))
    (is (= :double (:actual (ex-data error))))
    (is (= :int64 (:expected (ex-data error))))))

(deftest unknown-and-unregistered-evidence-retains-fallback-behavior
  (let [dynamic (analyze "src/a.clj"
                         '(res/resource {:service.name service-name}))
        custom-conflict
        (schema/merge-fragments
         (analyze "src/a.clj" '(res/resource {:custom.value 1}))
         (analyze "src/b.clj" '(res/resource {:custom.value "one"})))
        other-location (analyze "src/c.clj"
                                '(trace/set-attribute! span :service.name 1))]
    (is (= dynamic (semconv/check dynamic)))
    (is (:unknown? (first (:entries dynamic))))
    (is (= custom-conflict (semconv/check custom-conflict)))
    (is (:conflict? (first (:entries custom-conflict))))
    (is (= other-location (semconv/check other-location)))))

(deftest registry-errors-never-echo-input
  (let [secret "private-host-and-path-hint"
        error (try
                (semconv/read-registry
                 (str "{:schema \"" secret "\", :observed-value \"" secret "\"}"))
                nil
                (catch Exception error error))]
    (is (= {:otel.semantic-conventions/error :invalid-registry
            :reason :invalid-registry}
           (ex-data error)))
    (is (not (.contains (ex-message error) secret)))
    (is (not (.contains (pr-str (ex-data error)) secret)))))

(deftest registry-rejects-one-source-path-with-conflicting-digests
  (let [registry (semconv/load-registry)
        source (first (get-in registry [:provenance :files]))
        conflicting (assoc source :sha256 (apply str (repeat 64 "0")))
        invalid (update-in registry [:provenance :files]
                           #(vec (sort-by :path (conj % conflicting))))
        error (try (semconv/validate invalid) nil
                   (catch Exception error error))]
    (is (= {:otel.semantic-conventions/error :invalid-registry
            :reason :invalid-registry}
           (ex-data error)))))

(deftest mismatch-errors-do-not-echo-compound-map-keys
  (let [secret "private-observed-map-key"
        fragment (analyze "src/app.clj"
                          (list 'res/resource
                                {:service.name {(keyword secret) "x"}}))
        error (try (semconv/check fragment) nil
                   (catch Exception error error))]
    (is (= :kvlist (:actual (ex-data error))))
    (is (not (.contains (pr-str (ex-data error)) secret)))))

(deftest invalid-fragments-are-reported-without-source-data
  (let [secret "src/private-host-hint.clj"
        fragment (assoc (analyze secret '(res/resource {:service.name "ok"}))
                        :unexpected true)
        error (try (semconv/check fragment) nil
                   (catch Exception error error))]
    (is (= {:otel.semantic-conventions/error :invalid-fragment
            :reason :invalid-fragment}
           (ex-data error)))
    (is (not (.contains (pr-str (ex-data error)) secret)))))
