(ns otel.otlp.any-value
  "Shared OTLP/JSON wire codec for canonical otel.any-value values."
  (:require [clojure.string :as str]
            [otel.any-value :as any]))

(def ^:private base64-alphabet
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/")
(def ^:private base64-index
  (into {} (map-indexed (fn [index character] [character index])
                        base64-alphabet)))

(defn- encode-base64 [octets]
  (apply str
         (loop [index 0 output []]
           (if (>= index (count octets))
             output
             (let [remaining (- (count octets) index)
                   a (nth octets index)
                   b (if (> remaining 1) (nth octets (inc index)) 0)
                   c (if (> remaining 2) (nth octets (+ index 2)) 0)
                   word (+ (bit-shift-left a 16)
                           (bit-shift-left b 8) c)]
               (recur (+ index 3)
                      (into output
                            [(nth base64-alphabet (bit-and 63 (bit-shift-right word 18)))
                             (nth base64-alphabet (bit-and 63 (bit-shift-right word 12)))
                             (if (> remaining 1)
                               (nth base64-alphabet
                                    (bit-and 63 (bit-shift-right word 6))) \=)
                             (if (> remaining 2)
                               (nth base64-alphabet (bit-and 63 word)) \=)])))))))

(defn- fail! [path reason expected actual]
  (throw (ex-info "invalid OTLP AnyValue"
                  {:otel.otlp.any-value/error true
                   :path path :reason reason :expected expected :actual actual})))

(defn- decode-base64 [value path]
  (when-not (and (string? value) (zero? (mod (count value) 4)))
    (fail! path :invalid-base64 "padded base64 string" value))
  (loop [index 0 output []]
    (if (= index (count value))
      output
      (let [last-group? (= (+ index 4) (count value))
            chars (mapv #(nth value (+ index %)) (range 4))
            [a b c d] chars
            padding (cond (= c \=) 2 (= d \=) 1 :else 0)]
        (when-not (and (contains? base64-index a)
                       (contains? base64-index b)
                       (or (contains? base64-index c) (= c \=))
                       (or (contains? base64-index d) (= d \=))
                       (or (zero? padding) last-group?)
                       (or (not= c \=) (= d \=)))
          (fail! path :invalid-base64 "padded base64 string" value))
        (let [word (+ (bit-shift-left (get base64-index a) 18)
                      (bit-shift-left (get base64-index b) 12)
                      (bit-shift-left (get base64-index c 0) 6)
                      (get base64-index d 0))
              decoded [(bit-and 255 (bit-shift-right word 16))
                       (bit-and 255 (bit-shift-right word 8))
                       (bit-and 255 word)]]
          (recur (+ index 4)
                 (into output (take (- 3 padding) decoded))))))))

(declare encode)

(defn- key-values [value]
  (mapv (fn [[key item]] {:key key :value (encode item)}) value))

(defn encode
  "Encode one already-canonical value as an OTLP/JSON AnyValue map."
  [value]
  (cond
    (any/empty-value? value) {}
    (any/bytes? value) {:bytesValue (encode-base64 (any/byte-values value))}
    (string? value) {:stringValue value}
    (or (true? value) (false? value)) {:boolValue value}
    (integer? value) {:intValue (str value)}
    (float? value)
    {:doubleValue (cond
                    (not= value value) "NaN"
                    (= value ##Inf) "Infinity"
                    (= value ##-Inf) "-Infinity"
                    :else (double value))}
    (map? value) {:kvlistValue {:values (key-values value)}}
    (vector? value) {:arrayValue {:values (mapv encode value)}}
    :else (throw (ex-info "OTLP encoder requires a canonical AnyValue"
                          {:otel.otlp.any-value/error true
                           :reason :noncanonical-value}))))

(defn- field [value key]
  (let [text (name key)]
    (cond
      (and (map? value) (contains? value key)) (get value key)
      (and (map? value) (contains? value text)) (get value text)
      :else ::absent)))

(defn- present? [value] (not= ::absent value))

(defn- decimal-int64 [value path]
  (let [parsed
        (cond
          (integer? value) value
          (and (string? value) (re-matches #"-?[0-9]+" value))
          (let [negative? (str/starts-with? value "-")
                digits (if negative? (subs value 1) value)
                magnitude (reduce (fn [result character]
                                    (+ (* result 10) (- (int character) (int \0))))
                                  0 digits)]
            (if negative? (- magnitude) magnitude))
          :else nil)]
    (if (and (integer? parsed) (<= any/min-int64 parsed any/max-int64))
      parsed
      (fail! path :invalid-int64 "signed 64-bit decimal" value))))

(declare decode*)

(defn- decode-array [value path options depth nodes]
  (let [values (field value :values)]
    (when-not (or (not (present? values)) (vector? values))
      (fail! (conj path "values") :wrong-type "array" values))
    (mapv (fn [index item]
            (decode* item (conj path "values" index) options (inc depth) nodes))
          (range (count (if (present? values) values [])))
          (if (present? values) values []))))

(defn- decode-map [value path options depth nodes]
  (let [values (field value :values)
        values (if (present? values) values [])]
    (when-not (vector? values)
      (fail! (conj path "values") :wrong-type "array" values))
    (reduce
     (fn [result index]
       (let [entry (nth values index)
             entry-path (conj path "values" index)]
         (when-not (map? entry)
           (fail! entry-path :wrong-type "KeyValue object" entry))
         (let [key (field entry :key)
               wire (field entry :value)]
           (when-not (and (string? key) (not (empty? key)))
             (fail! (conj entry-path "key") :invalid-key "non-empty string" key))
           (when (contains? result key)
             (fail! (conj entry-path "key") :duplicate-key "unique string" key))
           (when-not (present? wire)
             (fail! (conj entry-path "value") :missing-value "AnyValue" nil))
           (assoc result key
                  (decode* wire (conj entry-path "value")
                           options (inc depth) nodes)))))
     (sorted-map) (range (count values)))))

(defn- decode* [wire path options depth nodes]
  (when (> depth (:max-depth options))
    (fail! path :depth-limit (:max-depth options) depth))
  (when (> (swap! nodes inc) (:max-nodes options))
    (fail! path :node-limit (:max-nodes options) @nodes))
  (when-not (map? wire)
    (fail! path :wrong-type "AnyValue object" wire))
  (let [arms (filter #(present? (field wire %))
                     [:stringValue :boolValue :intValue :doubleValue
                      :arrayValue :kvlistValue :bytesValue])]
    (when (> (count arms) 1)
      (fail! path :multiple-arms "zero or one AnyValue field" wire))
    (if (empty? arms)
      any/empty-value
      (let [arm (first arms)
            value (field wire arm)
            arm-path (conj path (name arm))]
        (case arm
          :stringValue
          (if (string? value) value
              (fail! arm-path :wrong-type "string" value))
          :boolValue
          (if (or (true? value) (false? value)) value
              (fail! arm-path :wrong-type "boolean" value))
          :intValue (decimal-int64 value arm-path)
          :doubleValue
          (cond
            (= value "NaN") ##NaN
            (= value "Infinity") ##Inf
            (= value "-Infinity") ##-Inf
            (or (integer? value) (float? value))
            (let [value (double value)]
              (if (and (= value value) (not= value ##Inf) (not= value ##-Inf))
                value
                (fail! arm-path :wrong-type
                       "finite double or special-value string" value)))
            :else (fail! arm-path :wrong-type "double JSON value" value))
          :bytesValue (any/bytes (decode-base64 value arm-path))
          :arrayValue
          (if (map? value)
            (decode-array value arm-path options depth nodes)
            (fail! arm-path :wrong-type "ArrayValue object" value))
          :kvlistValue
          (if (map? value)
            (decode-map value arm-path options depth nodes)
            (fail! arm-path :wrong-type "KeyValueList object" value)))))))

(defn decode
  "Decode one OTLP/JSON AnyValue without throwing for malformed wire data."
  ([wire] (decode wire any/default-limits))
  ([wire options]
   (let [options (any/limits options)]
     (try
       (let [value (decode* wire [] options 0 (atom 0))
             normalized (any/canonicalize value options)]
         (if-let [reason (:error normalized)]
           {:error {:path [] :reason reason :details (:details normalized)}}
           {:value (:value normalized)}))
       (catch :default error
         (if (:otel.otlp.any-value/error (ex-data error))
           {:error (dissoc (ex-data error) :otel.otlp.any-value/error)}
           (throw error)))))))
