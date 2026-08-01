(ns otel.baggage
  "Baggage: application-defined key/value pairs that travel with a request across
  service boundaries.

  Baggage is not telemetry — it is context. A front end can attach the customer
  tier or an experiment arm once, and every downstream service can read it and
  put it on its own spans. Because it crosses trust boundaries in a plain header,
  never put anything sensitive in it: it is readable, and modifiable, by every
  hop in between."
  (:refer-clojure :exclude [get remove get-in])
  (:require [clojure.string :as str]
            [otel.context :as ctx]))

(def ^:private context-key ::baggage)

(defrecord Baggage [entries])

(defn baggage
  "Baggage holding `m`, a map of string keys to string values. Values may carry
  metadata by supplying {:value v :metadata s} instead of a bare string."
  ([] (->Baggage {}))
  ([m] (->Baggage (reduce-kv (fn [acc k v]
                               (assoc acc (if (keyword? k) (name k) (str k)) v))
                             {}
                             (or m {})))))

(defn- entry-value [v]
  (if (map? v) (:value v) v))

(defn get-value
  "The value stored under `k`, or nil."
  [b k]
  (entry-value (clojure.core/get (:entries b) k)))

(defn get-metadata
  "The metadata string attached to `k`, or nil."
  [b k]
  (let [v (clojure.core/get (:entries b) k)]
    (when (map? v) (:metadata v))))

(defn put
  "Baggage with `k` set to `v`. Returns new baggage; the original is unchanged."
  ([b k v] (->Baggage (assoc (:entries b) k v)))
  ([b k v metadata] (->Baggage (assoc (:entries b) k {:value v :metadata metadata}))))

(defn remove-key
  "Baggage with `k` absent."
  [b k]
  (->Baggage (dissoc (:entries b) k)))

(defn ->map
  "The baggage as a plain map of key to value, dropping metadata."
  [b]
  (reduce-kv (fn [acc k v] (assoc acc k (entry-value v))) {} (:entries b)))

(defn empty-baggage? [b]
  (empty? (:entries b)))

(def empty-baggage (baggage {}))

;; --- context integration ----------------------------------------------------

(defn with-baggage
  "A context carrying `b`."
  [context b]
  (ctx/with-value context context-key b))

(defn from-context
  "The baggage in `context`, or empty baggage."
  [context]
  (or (ctx/get-value context context-key) empty-baggage))

(defn current
  "The baggage active on this thread."
  []
  (from-context (ctx/current)))
