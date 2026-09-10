(ns otel.attribute-schema.discovery
  "Validated, deterministic discovery of published attribute-schema fragments.

  Discovery reads only explicitly named EDN resources.  The merge core accepts
  an injected resource reader so build tools do not need to execute dependency
  code or depend on classpath traversal order."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [otel.attribute-schema :as attribute-schema]
            [otel.semantic-conventions :as semconv])
  (:import [java.security MessageDigest]))

(def index-schema-id "otel.attribute-schema.index/v1")
(def bundle-schema-id "otel.attribute-schema.bundle/v1")

(def ^:private max-index-chars (* 2 1024 1024))
(def ^:private max-fragment-chars (* 16 1024 1024))
(def ^:private max-indexes 256)
(def ^:private max-fragments 4096)

(defn- problem! [reason]
  ;; Resource contents, paths, URLs, selectors, host paths and resolver errors
  ;; are deliberately absent.  These classifications are safe for CI logs.
  (throw (ex-info "invalid attribute-schema artifact discovery"
                  {:otel.attribute-schema.discovery/error :invalid-discovery
                   :reason reason})))

(defn- sha256? [value]
  (boolean (and (string? value) (re-matches #"[0-9a-f]{64}" value))))

(defn- revision? [value]
  (boolean (and (string? value) (re-matches #"[0-9a-f]{40}" value))))

(defn- immutable-version? [value]
  (boolean
   (and (string? value)
        (<= 1 (count value) 128)
        (re-matches #"v?[0-9]+[.][0-9]+[.][0-9]+(?:[-+][0-9A-Za-z.-]+)?"
                    value)
        (not (re-find #"(?i)(snapshot|latest)" value)))))

(defn- package? [value]
  (boolean
   (and (string? value)
        (<= 1 (count value) 256)
        (re-matches #"[a-z0-9][a-z0-9._-]*(?:/[a-z0-9][a-z0-9._-]*)*"
                    value))))

(defn- repository? [value]
  (boolean
   (and (string? value)
        (<= 12 (count value) 512)
        (re-matches #"https://[A-Za-z0-9.-]+/[A-Za-z0-9._~/-]+" value)
        (not (str/ends-with? value "/"))
        (not (str/includes? (subs value 8) "//"))
        (not (re-find #"/(?:[.][.]?)(?:/|$)" value)))))

(defn- artifact? [artifact]
  (let [ks (set (keys artifact))]
    (and (map? artifact)
         (or (= #{:package :repository :revision} ks)
             (= #{:package :repository :version} ks))
         (package? (:package artifact))
         (repository? (:repository artifact))
         (if (contains? artifact :revision)
           (revision? (:revision artifact))
           (immutable-version? (:version artifact))))))

(defn- fragment-ref? [fragment]
  (and (map? fragment)
       (= #{:path :sha256} (set (keys fragment)))
       (string? (:path fragment))
       (= (:path fragment)
          (try
            (attribute-schema/canonical-source (:path fragment))
            (catch Exception _error nil)))
       (str/starts-with? (:path fragment) "META-INF/otel/attribute-schema/")
       (str/ends-with? (:path fragment) ".edn")
       (sha256? (:sha256 fragment))))

(defn validate-index
  "Validate one closed, canonical otel.attribute-schema.index/v1 value."
  [index]
  (let [fragments (:fragments index)]
    (when-not
     (and (map? index)
          (= #{:schema :artifact :fragments} (set (keys index)))
          (= index-schema-id (:schema index))
          (artifact? (:artifact index))
          (vector? fragments)
          (not (empty? fragments))
          (<= (count fragments) max-fragments)
          (every? fragment-ref? fragments)
          (= fragments (vec (sort-by :path fragments)))
          (= (count fragments) (count (distinct (map :path fragments)))))
      (problem! :malformed-index)))
  index)

(defn read-index
  "Read and validate one bounded EDN index string without evaluation."
  [text]
  (when-not (and (string? text) (<= (count text) max-index-chars))
    (problem! :malformed-index))
  (try
    (validate-index (edn/read-string text))
    (catch Exception error
      (if (:otel.attribute-schema.discovery/error (ex-data error))
        (throw error)
        (problem! :malformed-index)))))

(defn- bytes->hex [bytes]
  (let [digits "0123456789abcdef"]
    (apply str
           (mapcat (fn [byte]
                     (let [value (bit-and (int byte) 255)]
                       [(.charAt digits (bit-shift-right value 4))
                        (.charAt digits (bit-and value 15))]))
                   bytes))))

(defn content-sha256
  "Return the lowercase SHA-256 digest of the exact UTF-8 resource text."
  [text]
  (bytes->hex
   (.digest (MessageDigest/getInstance "SHA-256")
            (.getBytes (str text) "UTF-8"))))

(defn- release-key [artifact]
  (if (contains? artifact :revision)
    [:revision (:revision artifact)]
    [:version (:version artifact)]))

(defn- identity [artifact fragment]
  (merge artifact fragment))

(defn- identity? [value]
  (let [artifact-keys (if (contains? value :revision)
                        #{:package :repository :revision}
                        #{:package :repository :version})
        fragment-keys #{:path :sha256}]
    (and (map? value)
         (= (set (keys value)) (into artifact-keys fragment-keys))
         (artifact? (select-keys value artifact-keys))
         (fragment-ref? (select-keys value fragment-keys)))))

(defn- identity-sort-key [item]
  (let [identity (:identity item)]
    [(:package identity) (:repository identity) (release-key identity)
     (:path identity) (:sha256 identity)]))

(defn- selected? [artifact include exclude]
  (let [package (:package artifact)]
    (and (or (empty? include) (contains? include package))
         (not (contains? exclude package)))))

(defn- selectors [value]
  (let [value (or value #{})]
    (when-not (and (coll? value) (every? package? value))
      (problem! :invalid-selection))
    (set value)))

(defn- check-global-claims! [items]
  (let [locator #(dissoc (:identity %) :sha256)
        locators (map locator items)
        paths (map #(get-in % [:identity :path]) items)
        claims (group-by (fn [{:keys [identity]}]
                           [(:package identity) (release-key identity)])
                         items)]
    (when-not (= (count locators) (count (distinct locators)))
      (if (= (count (map :identity items))
             (count (distinct (map :identity items))))
        (problem! :conflicting-identity)
        (problem! :duplicate-identity)))
    (when-not (= (count paths) (count (distinct paths)))
      (problem! :path-collision))
    (when (some (fn [[_ group]]
                  (< 1 (count (distinct (map #(get-in % [:identity :repository])
                                             group)))))
                claims)
      (problem! :conflicting-artifact-claim))))

(defn- read-fragment! [read-resource item]
  (let [text (try
               (read-resource (get-in item [:identity :path]))
               (catch Exception _error
                 (problem! :unreadable-resource)))]
    (when (nil? text)
      (problem! :missing-resource))
    (when-not (and (string? text) (<= (count text) max-fragment-chars))
      (problem! :invalid-fragment))
    (when-not (= (get-in item [:identity :sha256]) (content-sha256 text))
      (problem! :digest-mismatch))
    (let [fragment
          (try
            (attribute-schema/validate (edn/read-string text))
            (catch Exception _error
              (problem! :invalid-fragment)))]
      (assoc item :sources (:sources fragment) :fragment fragment))))

(defn discover
  "Discover and merge fragments from explicit index texts.

  `read-resource` receives each validated project-relative resource path and
  returns its exact UTF-8 text or nil.  `:include` and `:exclude` contain
  canonical package strings; exclusion wins.  A non-empty include set must be
  fully satisfied.  Returns a deterministic pure build value."
  ([index-texts read-resource]
   (discover index-texts read-resource {}))
  ([index-texts read-resource {:keys [include exclude]}]
   (when-not (and (coll? index-texts) (<= (count index-texts) max-indexes))
     (problem! :malformed-index))
   (when-not (fn? read-resource)
     (problem! :invalid-resource-reader))
   (let [include (selectors include)
         exclude (selectors exclude)
         indexes (mapv read-index index-texts)
         available (set (map #(get-in % [:artifact :package]) indexes))]
     (when-not (every? available include)
       (problem! :missing-included-artifact))
     (let [items (->> indexes
                      (filter #(selected? (:artifact %) include exclude))
                      (mapcat (fn [{:keys [artifact fragments]}]
                                (map (fn [fragment]
                                       {:identity (identity artifact fragment)})
                                     fragments)))
                      vec)]
       (when (> (count items) max-fragments)
         (problem! :too-many-fragments))
       (check-global-claims! items)
       (let [loaded (->> items
                         (sort-by identity-sort-key)
                         (map #(read-fragment! read-resource %))
                         vec)
             checked (mapv #(update % :fragment semconv/check) loaded)
             merged (apply attribute-schema/merge-fragments
                           (map :fragment checked))]
         {:schema bundle-schema-id
          :fragments (mapv #(select-keys % [:identity :sources]) checked)
          :attribute-schema merged})))))

(defn render
  "Render a discovered bundle as byte-deterministic canonical EDN."
  [bundle]
  (let [fragments (:fragments bundle)]
    (when-not
     (and (map? bundle)
          (= #{:schema :fragments :attribute-schema} (set (keys bundle)))
          (= bundle-schema-id (:schema bundle))
          (vector? fragments)
          (<= (count fragments) max-fragments)
          (every? (fn [item]
                    (and (map? item)
                         (= #{:identity :sources} (set (keys item)))
                         (identity? (:identity item))
                         (vector? (:sources item))
                         (= (:sources item)
                            (vec (sort (distinct (:sources item)))))
                         (every? #(try
                                    (= % (attribute-schema/canonical-source %))
                                    (catch Exception _error false))
                                 (:sources item))))
                  fragments)
          (= fragments (vec (sort-by identity-sort-key fragments))))
      (problem! :invalid-bundle))
    (check-global-claims! fragments)
    (try
      (attribute-schema/validate (:attribute-schema bundle))
      (catch Exception _error
        (problem! :invalid-bundle)))
    (str (pr-str bundle) "\n")))

(defn discover-resources
  "Discover from explicitly named classpath index resources.

  There is intentionally no directory scan.  Each index path and every
  fragment path is validated before classpath lookup."
  ([index-resources]
   (discover-resources index-resources {}))
  ([index-resources options]
   (when-not (and (coll? index-resources)
                  (<= (count index-resources) max-indexes)
                  (= (count index-resources) (count (distinct index-resources))))
     (problem! :malformed-index))
   (let [paths (mapv (fn [path]
                       (try
                         (attribute-schema/canonical-source path)
                         (catch Exception _error
                           (problem! :malformed-index))))
                     index-resources)
         read-classpath (fn [path]
                          (when-let [resource (io/resource path)]
                            (slurp resource)))
         index-texts (mapv (fn [path]
                             (or (read-classpath path)
                                 (problem! :missing-index-resource)))
                           paths)]
     (discover index-texts read-classpath options))))
