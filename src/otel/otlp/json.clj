(ns otel.otlp.json
  "A minimal JSON writer, sized for OTLP payloads.

  This exists because jolt has no JSON library and OTLP/JSON needs only the
  writing half of one — the exporter serializes requests and, apart from a
  partial-success count, never parses responses. Keeping it here rather than
  taking a dependency also keeps the encoder honest about the one rule that
  matters most for OTLP: a 64-bit integer must be written as a decimal *string*,
  because JSON numbers are doubles and a nanosecond timestamp does not survive
  the round trip."
  (:require [clojure.string :as str]))

(defn- escape
  "JSON string escaping. Control characters below 0x20 must be escaped, and are
  written in \\u form since they have no shorthand."
  [s]
  (let [sb (StringBuilder.)]
    (.append sb \")
    (doseq [c (str s)]
      (let [n (int c)]
        (cond
          (= c \") (.append sb "\\\"")
          (= c \\) (.append sb "\\\\")
          (= c \newline) (.append sb "\\n")
          (= c \return) (.append sb "\\r")
          (= c \tab) (.append sb "\\t")
          (= n 8) (.append sb "\\b")
          (= n 12) (.append sb "\\f")
          (< n 0x20) (.append sb (format "\\u%04x" n))
          :else (.append sb c))))
    (.append sb \")
    (.toString sb)))

(declare write-value)

(defn- write-object [sb m]
  (.append sb \{)
  (let [entries (seq m)]
    (loop [es entries first? true]
      (when-let [[k v] (first es)]
        (when-not first? (.append sb \,))
        (.append sb (escape (if (keyword? k) (name k) (str k))))
        (.append sb \:)
        (write-value sb v)
        (recur (rest es) false))))
  (.append sb \}))

(defn- write-array [sb xs]
  (.append sb \[)
  (loop [xs (seq xs) first? true]
    (when-let [x (first xs)]
      (when-not first? (.append sb \,))
      (write-value sb x)
      (recur (rest xs) false)))
  (.append sb \]))

(defn- write-value [sb v]
  (cond
    (nil? v) (.append sb "null")
    (true? v) (.append sb "true")
    (false? v) (.append sb "false")
    (string? v) (.append sb (escape v))
    (keyword? v) (.append sb (escape (name v)))
    (map? v) (write-object sb v)
    (or (vector? v) (seq? v) (list? v) (set? v)) (write-array sb v)
    (integer? v) (.append sb (str v))
    (float? v) (.append sb (str (double v)))
    :else (.append sb (escape (str v)))))

(defn write-str
  "Serialize `v` to a JSON string."
  [v]
  (let [sb (StringBuilder.)]
    (write-value sb v)
    (.toString sb)))

;; --- reading (only what a response needs) -----------------------------------

(defn find-number
  "The first number following \"`field`\" in a JSON document, or nil.

  Deliberately not a parser. The exporter reads exactly one thing out of an OTLP
  response — a rejected-records count in a partial-success body — and a scan is
  both sufficient and impossible to get wrong on hostile input."
  [s field]
  (when (and s field)
    (when-let [i (str/index-of s (str "\"" field "\""))]
      (let [after (subs s (+ i (count field) 2))
            digits (re-find #"-?\d+" after)]
        (when digits (Long/parseLong digits))))))
