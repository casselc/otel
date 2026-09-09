(ns otel.any-value-test
  (:require [clojure.test :refer [deftest is testing]]
            [otel.any-value :as any]
            [otel.otlp.any-value :as wire]))

(deftest canonical-scalar-algebra
  (is (= "x" (:value (any/canonicalize "x"))))
  (is (= true (:value (any/canonicalize true))))
  (is (= 42 (:value (any/canonicalize 42))))
  (is (= 1.5 (:value (any/canonicalize 1.5))))
  (is (= :nil-value (:error (any/canonicalize nil))))
  (is (any/empty-value? (:value (any/canonicalize any/empty-value))))
  (testing "present empty is not absent, an empty string, or an empty array"
    (is (not= any/empty-value nil))
    (is (not= any/empty-value ""))
    (is (not= any/empty-value []))))

(deftest signed-int64-is-exact
  (is (= any/min-int64 (:value (any/canonicalize any/min-int64))))
  (is (= any/max-int64 (:value (any/canonicalize any/max-int64))))
  (is (= :int64-range (:error (any/canonicalize (dec any/min-int64)))))
  (is (= :int64-range (:error (any/canonicalize (inc any/max-int64))))))

(deftest byte-strings-have-immutable-value-semantics
  (let [left (any/bytes [0 1 254 255])
        right (any/bytes '(0 1 254 255))]
    (is (= left right))
    (is (= [0 1 254 255] (any/byte-values left)))
    (is (vector? (any/byte-values right)))
    (is (= :invalid-bytes
           (:reason (ex-data
                     (try (any/bytes [256])
                          (catch :default error error))))))))

(deftest recursive-values-are-canonical-and-deterministic
  (let [result (any/canonicalize
                {:z [1 "two" any/empty-value (any/bytes [3])]
                 :a {:nested :value}})
        value (:value result)]
    (is (= ["a" "z"] (vec (keys value))))
    (is (= {"nested" "value"} (get value "a")))
    (is (any/empty-value? (get-in value ["z" 2])))
    (is (= (any/bytes [3]) (get-in value ["z" 3]))))
  (is (= :duplicate-map-key
         (:error (any/canonicalize {:a 1 "a" 2})))))

(deftest recursive-budgets-fail-closed
  (is (= :depth-limit
         (:error (any/canonicalize [[1]] {:max-depth 1}))))
  (is (= :node-limit
         (:error (any/canonicalize [1 2] {:max-nodes 2}))))
  (is (= :byte-limit
         (:error (any/canonicalize "ab" {:max-bytes 7}))))
  (let [result (any/canonicalize ["abcdef" (any/bytes [1 2 3 4])]
                                 {:value-length-limit 3})]
    (is (= ["abc" (any/bytes [1 2 3])] (:value result)))
    (is (:truncated? result))))

(deftest wire-roundtrip-covers-every-any-value-arm
  (let [values [any/empty-value "x" true any/min-int64 any/max-int64
                1.5 (any/bytes [0 1 254 255])
                [1 "two" any/empty-value]
                (sorted-map "a" false "nested" [3])]]
    (doseq [value values]
      (is (= value (:value (wire/decode (wire/encode value)))))))
  (is (= "AAH+/w==" (:bytesValue (wire/encode (any/bytes [0 1 254 255])))))
  (doseq [value [##Inf ##-Inf]]
    (is (= value (:value (wire/decode (wire/encode value))))))
  (is (let [value (:value (wire/decode (wire/encode ##NaN)))]
        (not= value value))))

(deftest absent-application-nil-and-present-wire-empty-stay-distinct
  (is (= :nil-value (:error (any/canonicalize nil))))
  (is (= {} (wire/encode any/empty-value)))
  (is (any/empty-value? (:value (wire/decode {})))))

(deftest malformed-wire-values-are-explicit-errors
  (is (= :multiple-arms
         (get-in (wire/decode {:stringValue "x" :intValue "1"})
                 [:error :reason])))
  (is (= :invalid-int64
         (get-in (wire/decode {:intValue "9223372036854775808"})
                 [:error :reason])))
  (is (= :invalid-base64
         (get-in (wire/decode {:bytesValue "%%%="}) [:error :reason])))
  (is (= :duplicate-key
         (get-in
          (wire/decode
           {:kvlistValue
            {:values [{:key "a" :value {:intValue "1"}}
                      {:key "a" :value {:intValue "2"}}]}})
          [:error :reason])))
  (is (= :wrong-type
         (get-in (wire/decode {:doubleValue ##Inf}) [:error :reason]))))
