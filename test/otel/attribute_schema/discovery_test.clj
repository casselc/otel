(ns otel.attribute-schema.discovery-test
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [otel.attribute-schema :as schema]
            [otel.attribute-schema.discovery :as discovery]))

(def fixture-root "test/META-INF/otel/")

(def index-resource-paths
  ["META-INF/otel/attribute-schema-index/ordinary-library.edn"
   "META-INF/otel/attribute-schema-index/checkout-advice.edn"])

(defn- fixture [name]
  (slurp (str fixture-root name)))

(defn- fixture-input []
  {:indexes [(fixture "attribute-schema-index/ordinary-library.edn")
             (fixture "attribute-schema-index/checkout-advice.edn")]
   :resources
   {"META-INF/otel/attribute-schema/ordinary-library.edn"
    (fixture "attribute-schema/ordinary-library.edn")
    "META-INF/otel/attribute-schema/checkout-advice.edn"
    (fixture "attribute-schema/checkout-advice.edn")}})

(defn- discover
  ([indexes resources]
   (discover indexes resources {}))
  ([indexes resources options]
   (discovery/discover indexes #(get resources %) options)))

(defn- failure [f]
  (try (f) nil (catch Exception error error)))

(defn- parsed-index [text]
  (edn/read-string text))

(defn- index-text [index]
  (pr-str index))

(defn- reverse-map-order [value]
  (cond
    (map? value)
    (into (array-map)
          (reverse
           (map (fn [[key item]] [key (reverse-map-order item)]) value)))

    (vector? value) (mapv reverse-map-order value)
    :else value))

(defn- fragment-text [source form]
  (schema/render
   (schema/analyze-form
    {:source source :aliases {'res 'otel.resource}}
    form)))

(deftest checked-library-and-local-advice-pack-remain-distinct
  (let [{:keys [indexes resources]} (fixture-input)
        bundle (discover indexes resources)
        fragments (:fragments bundle)
        merged (:attribute-schema bundle)]
    (is (= discovery/bundle-schema-id (:schema bundle)))
    (is (= ["io.github.example/ordinary-library" "local/checkout-advice"]
           (mapv #(get-in % [:identity :package]) fragments)))
    ;; Both publishers intentionally used the same project-relative source.
    ;; The bundle retains two artifact-scoped provenance records.
    (is (= [["src/shared.clj"] ["src/shared.clj"]]
           (mapv :sources fragments)))
    (is (= 2 (count (distinct (map :identity fragments)))))
    (is (= ["process.pid" "service.name"]
           (mapv :key (:entries merged))))
    (is (:unknown? (first (:entries merged))))))

(deftest explicit-classpath-index-resources-use-the-shared-core
  (let [{:keys [indexes resources]} (fixture-input)
        injected (discover indexes resources)
        classpath (discovery/discover-resources
                   (vec (reverse index-resource-paths)))]
    (is (= injected classpath))))

(deftest duplicate-classpath-resources-fail-closed
  (let [secret "/private/duplicate.jar"
        error
        (with-redefs-fn
          {#'otel.attribute-schema.discovery/resource-urls
           (fn [_path] [secret "file:/other/duplicate.jar"])}
          #(failure
            (fn []
              (discovery/discover-resources [(first index-resource-paths)]))))]
    (is (= {:otel.attribute-schema.discovery/error :invalid-discovery
            :reason :duplicate-classpath-resource}
           (ex-data error)))
    (is (not (str/includes? (pr-str (ex-data error)) secret)))))

(deftest shuffled-index-order-renders-byte-identically
  (let [{:keys [indexes resources]} (fixture-input)
        bundle (discover indexes resources)
        forward (discovery/render bundle)
        reverse (discovery/render (discover (vec (reverse indexes)) resources))
        reversed-maps
        (discovery/render
         (discover (mapv #(-> % parsed-index reverse-map-order index-text)
                         indexes)
                   resources))
        caller-reordered (discovery/render (reverse-map-order bundle))]
    (is (= forward reverse))
    (is (= forward reversed-maps))
    (is (= forward caller-reordered))
    (is (= :invalid-bundle
           (:reason
            (ex-data
             (failure #(discovery/render (assoc bundle :unchecked true)))))))))

(deftest serialized-bundles-round-trip-canonically
  (let [{:keys [indexes resources]} (fixture-input)
        bundle (discover indexes resources)
        rendered (discovery/render bundle)
        read-back (discovery/read-bundle rendered)
        validated (discovery/validate-bundle (reverse-map-order bundle))]
    (is (= bundle validated))
    (is (= (pr-str bundle) (pr-str validated)))
    (is (= bundle read-back))
    (is (= rendered (discovery/render read-back)))))

(deftest serialized-render-and-read-share-the-same-character-bound
  (let [{:keys [indexes resources]} (fixture-input)
        bundle (discover indexes resources)
        rendered (discovery/render bundle)
        exact-count (count rendered)]
    (with-redefs [discovery/max-bundle-text-chars exact-count]
      (is (= rendered (discovery/render bundle)))
      (is (= bundle (discovery/read-bundle rendered))))
    (with-redefs [discovery/max-bundle-text-chars (dec exact-count)]
      (is (= {:otel.attribute-schema.discovery/error :invalid-discovery
              :reason :invalid-bundle}
             (ex-data (failure #(discovery/render bundle)))))
      (is (= :invalid-bundle
             (:reason
              (ex-data (failure #(discovery/read-bundle rendered)))))))))

(deftest serialized-bundles-require-exactly-one-valid-value
  (let [{:keys [indexes resources]} (fixture-input)
        rendered (discovery/render (discover indexes resources))]
    (doseq [suffix ["\n{}\n" "\n]"]]
      (is (= {:otel.attribute-schema.discovery/error :invalid-discovery
              :reason :invalid-bundle}
             (ex-data
              (failure #(discovery/read-bundle (str rendered suffix)))))))))

(deftest serialized-bundle-failures-are-bounded-and-redacted
  (let [{:keys [indexes resources]} (fixture-input)
        bundle (discover indexes resources)
        secret "/private/catalog/OTEL_TOKEN=secret"
        conflict
        (schema/analyze-form
         {:source "src/private-host.clj" :aliases {'res 'otel.resource}}
         '(res/resource {:service.name 42}))
        inputs [(str "{:schema \"" secret "\"}")
                (pr-str (assoc bundle :private secret))
                (pr-str (assoc bundle :attribute-schema conflict))]]
    (doseq [input inputs]
      (let [error (failure #(discovery/read-bundle input))]
        (is (= {:otel.attribute-schema.discovery/error :invalid-discovery
                :reason :invalid-bundle}
               (ex-data error)))
        (is (not (str/includes? (ex-message error) secret)))
        (is (not (str/includes? (pr-str (ex-data error)) secret)))))
    (doseq [input [nil 42 {:private secret}]]
      (is (= :invalid-bundle
             (:reason (ex-data (failure #(discovery/read-bundle input)))))))
    (with-redefs [discovery/max-bundle-text-chars 8]
      (let [error (failure #(discovery/read-bundle (str secret "padding")))]
        (is (= {:otel.attribute-schema.discovery/error :invalid-discovery
                :reason :invalid-bundle}
               (ex-data error)))
        (is (not (str/includes? (pr-str (ex-data error)) secret)))))))

(deftest explicit-package-selection-is-deterministic
  (let [{:keys [indexes resources]} (fixture-input)
        advice (discover indexes resources
                         {:include #{"local/checkout-advice"}})
        library (discover indexes resources
                          {:exclude #{"local/checkout-advice"}})]
    (is (= ["local/checkout-advice"]
           (mapv #(get-in % [:identity :package]) (:fragments advice))))
    (is (= ["io.github.example/ordinary-library"]
           (mapv #(get-in % [:identity :package]) (:fragments library))))
    (is (= :missing-included-artifact
           (:reason
            (ex-data
             (failure #(discover indexes resources
                                  {:include #{"io.github.example/missing"}}))))))))

(deftest option-maps-are-closed-before-discovery-work
  (let [{:keys [indexes]} (fixture-input)
        reads (atom 0)
        reader (fn [_] (swap! reads inc) nil)]
    (doseq [options [nil [] {:include #{} :unexpected "private-value"}]]
      (is (= :invalid-options
             (:reason
              (ex-data
               (failure #(discovery/discover indexes reader options)))))))
    (is (zero? @reads))
    (is (= :invalid-options
           (:reason
            (ex-data
             (failure #(discovery/discover-resources
                        ["/invalid-before-lookup"]
                        {:unknown true}))))))))

(deftest exact-one-edn-value-is-required
  (let [{:keys [indexes resources]} (fixture-input)
        library-index (parsed-index (first indexes))
        library-path (get-in library-index [:fragments 0 :path])
        library-fragment (get resources library-path)]
    (testing "index trailing values and junk"
      (doseq [suffix ["\n{}\n" "\n]"]]
        (let [error (failure #(discover [(str (first indexes) suffix)]
                                        resources))]
          (is (= :malformed-index (:reason (ex-data error)))))))
    (testing "fragment trailing values and junk after a matching digest"
      (doseq [suffix ["\n{}\n" "\n]"]]
        (let [tainted (str library-fragment suffix)
              index (assoc-in library-index [:fragments 0 :sha256]
                              (discovery/content-sha256 tainted))
              error (failure #(discover [(index-text index)]
                                        (assoc resources library-path tainted)))]
          (is (= :invalid-fragment (:reason (ex-data error)))))))
    (testing "trailing whitespace and comments are allowed"
      (is (= library-index
             (discovery/read-index (str (first indexes) "\n ; comment\n")))))))

(deftest digest-input-must-be-exact-resource-text
  (doseq [value [nil 42 :text ["text"]]]
    (is (= :invalid-fragment
           (:reason
            (ex-data (failure #(discovery/content-sha256 value))))))))

(deftest indexes-and-resources-fail-closed
  (let [{:keys [indexes resources]} (fixture-input)
        library (parsed-index (first indexes))
        advice (parsed-index (second indexes))
        zero-digest (apply str (repeat 64 "0"))
        changed-digest (assoc-in library [:fragments 0 :sha256] zero-digest)
        changed-repository
        (-> advice
            (assoc-in [:artifact :package]
                      (get-in library [:artifact :package]))
            (assoc-in [:artifact :revision]
                      (get-in library [:artifact :revision]))
            (update :artifact dissoc :version))
        colliding-path
        (assoc-in advice [:fragments 0 :path]
                  (get-in library [:fragments 0 :path]))]
    (testing "missing resource"
      (is (= :missing-resource
             (:reason (ex-data (failure #(discover indexes {})))))))
    (testing "digest drift"
      (is (= :digest-mismatch
             (:reason
              (ex-data
               (failure #(discover [(index-text changed-digest)] resources)))))))
    (testing "exact duplicate identity"
      (is (= :duplicate-identity
             (:reason
              (ex-data
               (failure #(discover [(first indexes) (first indexes)]
                                   resources)))))))
    (testing "one locator claiming different content"
      (is (= :conflicting-identity
             (:reason
              (ex-data
               (failure #(discover [(first indexes)
                                    (index-text changed-digest)] resources)))))))
    (testing "conflicting artifact claim"
      (is (= :conflicting-artifact-claim
             (:reason
              (ex-data
               (failure #(discover [(first indexes)
                                    (index-text changed-repository)]
                                   resources)))))))
    (testing "resource path collision"
      (is (= :path-collision
             (:reason
              (ex-data
               (failure #(discover [(first indexes)
                                    (index-text colliding-path)] resources)))))))))

(deftest malformed-input-diagnostics-are-bounded-and-private
  (let [secret "/home/private/OTEL_TOKEN=secret-value"
        malformed (str "{:schema \"" secret "\"}")
        error (failure #(discover [malformed] {}))]
    (is (= {:otel.attribute-schema.discovery/error :invalid-discovery
            :reason :malformed-index}
           (ex-data error)))
    (is (not (str/includes? (ex-message error) secret)))
    (is (not (str/includes? (pr-str (ex-data error)) secret)))))

(deftest pinned-semconv-conflicts-fail-without-provenance-leaks
  (let [{:keys [indexes resources]} (fixture-input)
        valid-bundle (discover indexes resources)
        conflict (fragment-text "src/private-host.clj"
                                '(res/resource {:service.name 42}))
        index (-> (parsed-index (first indexes))
                  (assoc-in [:fragments 0 :sha256]
                            (discovery/content-sha256 conflict)))
        conflict-resources
        (assoc resources
               "META-INF/otel/attribute-schema/ordinary-library.edn"
               conflict)
        selected-indexes [(index-text index) (second indexes)]
        first-error (failure #(discover selected-indexes conflict-resources))
        second-error (failure #(discover (vec (reverse selected-indexes))
                                         conflict-resources))
        render-error
        (failure #(discovery/render
                   (assoc valid-bundle :attribute-schema
                          (schema/analyze-form
                           {:source "src/private-host.clj"
                            :aliases {'res 'otel.resource}}
                           '(res/resource {:service.name 42})))))]
    (is (= (ex-data first-error) (ex-data second-error)))
    (is (= :mismatch
           (:otel.semantic-conventions/error (ex-data first-error))))
    (is (= :type-mismatch (:reason (ex-data first-error))))
    (is (not (str/includes? (pr-str (ex-data first-error))
                            "private-host.clj")))
    (is (not (str/includes? (ex-message first-error) "/home/")))
    (is (= :mismatch
           (:otel.semantic-conventions/error (ex-data render-error))))))
