(ns otel.semantic-conventions
  "Pinned OpenTelemetry semantic-convention metadata for attributes emitted by
  this library.

  The registry is immutable source data. Checking an attribute-schema fragment
  never observes runtime values and never changes either input."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [otel.attribute-schema :as attribute-schema]))

(def schema-id "otel.semantic-conventions/v1")

(def registry-resource "otel/semantic-conventions.edn")

(def ^:private allowed-types
  #{:string :boolean :int64 :double :bytes})

(def ^:private allowed-stabilities
  #{:development :release-candidate :stable})

(def ^:private allowed-signals
  #{:span :metric :log :resource})

(def ^:private allowed-locations
  #{:attributes :scope-attributes})

(defn- problem! [error reason]
  ;; Registry contents, fragments, source paths and observed values are
  ;; intentionally absent. Callers get a bounded classification, not a dump of
  ;; potentially sensitive build input.
  (throw (ex-info "invalid OpenTelemetry semantic-convention data"
                  {:otel.semantic-conventions/error error
                   :reason reason})))

(defn- sha256? [value]
  (boolean (and (string? value) (re-matches #"[0-9a-f]{64}" value))))

(defn- commit? [value]
  (boolean (and (string? value) (re-matches #"[0-9a-f]{40}" value))))

(defn- source-file? [item]
  (and (map? item)
       (= #{:path :sha256} (set (keys item)))
       (string? (:path item))
       (re-matches #"model/[a-z0-9_-]+/registry[.]yaml" (:path item))
       (sha256? (:sha256 item))))

(defn- location? [location]
  (and (vector? location)
       (= 2 (count location))
       (contains? allowed-signals (first location))
       (contains? allowed-locations (second location))
       (or (= :attributes (second location))
           (contains? #{:span :metric :log} (first location)))))

(defn- entry? [entry]
  (and (map? entry)
       (= #{:key :locations :type :stability} (set (keys entry)))
       (string? (:key entry))
       (not (empty? (:key entry)))
       (vector? (:locations entry))
       (not (empty? (:locations entry)))
       (every? location? (:locations entry))
       (= (:locations entry) (vec (sort-by pr-str (distinct (:locations entry)))))
       (contains? allowed-types (:type entry))
       (contains? allowed-stabilities (:stability entry))))

(defn validate
  "Validate the closed, canonical v1 registry and return it.

  Invalid input reports only a fixed error classification; registry contents
  are never copied into exception data."
  [registry]
  (let [provenance (:provenance registry)
        files (:files provenance)
        entries (:entries registry)]
    (when-not
     (and (map? registry)
          (= #{:schema :provenance :entries} (set (keys registry)))
          (= schema-id (:schema registry))
          (map? provenance)
          (= #{:repository :release :commit :files} (set (keys provenance)))
          (= "https://github.com/open-telemetry/semantic-conventions"
             (:repository provenance))
          (boolean (and (string? (:release provenance))
                        (re-matches #"v[0-9]+[.][0-9]+[.][0-9]+"
                                    (:release provenance))))
          (commit? (:commit provenance))
          (vector? files)
          (not (empty? files))
          (<= (count files) 64)
          (every? source-file? files)
          (= files (vec (sort-by :path (distinct files))))
          (= (count files) (count (distinct (map :path files))))
          (vector? entries)
          (not (empty? entries))
          (<= (count entries) 4096)
          (every? entry? entries)
          (= entries (vec (sort-by :key (distinct entries))))
          (= (count entries) (count (distinct (map :key entries)))))
      (problem! :invalid-registry :invalid-registry)))
  registry)

(defn read-registry
  "Read and validate one registry EDN string without evaluation."
  [text]
  (try
    (validate (edn/read-string text))
    (catch Exception error
      (if (:otel.semantic-conventions/error (ex-data error))
        (throw error)
        (problem! :invalid-registry :unreadable-registry)))))

(defn load-registry
  "Load the pinned registry from this library's classpath resource."
  []
  (if-let [resource (io/resource registry-resource)]
    (try
      (read-registry (slurp resource))
      (catch Exception error
        (if (:otel.semantic-conventions/error (ex-data error))
          (throw error)
          (problem! :invalid-registry :unreadable-registry))))
    (problem! :invalid-registry :missing-registry)))

(def ^:private pinned-registry (delay (load-registry)))

(defn registry
  "Return the validated pinned registry."
  []
  @pinned-registry)

(defn lookup
  "Look up a standard attribute by key.

  With a location pair, return the entry only when the convention applies at
  that [signal location]. Unknown keys and locations return nil."
  ([key]
   (first (filter #(= key (:key %)) (:entries (registry)))))
  ([key location]
   (when-let [entry (lookup key)]
     (when (some #{location} (:locations entry)) entry))))

(defn- concrete-types [types]
  (remove #{:unknown} types))

(defn- public-type [descriptor]
  ;; Compound descriptors carry application map keys. Report only their
  ;; top-level AnyValue arm so a mismatch cannot disclose those keys.
  (if (keyword? descriptor)
    descriptor
    (cond
      (contains? descriptor :array) :array
      (contains? descriptor :kvlist) :kvlist
      :else :compound)))

(defn- validate-fragment [fragment]
  (try
    (attribute-schema/validate fragment)
    (catch Exception _error
      (problem! :invalid-fragment :invalid-fragment))))

(defn- mismatch! [entry location actual]
  (throw
   (ex-info "attribute type conflicts with pinned OpenTelemetry semantics"
            {:otel.semantic-conventions/error :mismatch
             :reason :type-mismatch
             :key (:key entry)
             :location location
             :expected (:type entry)
             :actual actual
             :stability (:stability entry)})))

(defn check
  "Validate an otel.attribute-schema/v1 fragment against pinned conventions.

  Concrete inferred types at registered locations must match exactly. Unknown
  or dynamic evidence remains on the existing fallback path. Unregistered keys
  and locations retain the attribute-schema fragment's existing behavior.
  Returns the original validated fragment and performs no widening or mutation."
  [fragment]
  (let [by-key (into {} (map (juxt :key identity) (:entries (registry))))
        fragment (validate-fragment fragment)]
    (doseq [{:keys [signal location key types]} (:entries fragment)]
      (let [location [signal location]
            convention (get by-key key)]
        (when (and convention
                   (some #{location} (:locations convention)))
          (when-let [actual (first (sort-by pr-str
                                            (map public-type
                                                 (remove #{(:type convention)}
                                                         (concrete-types types)))))]
            (mismatch! convention location actual)))))
    fragment))
