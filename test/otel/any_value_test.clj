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

(defn- same-result? [left right]
  ;; NaN is intentionally unequal to itself. All other result fields must
  ;; still agree, including budgets, truncation and error details.
  (and (= (dissoc left :value) (dissoc right :value))
       (let [a (:value left) b (:value right)]
         (or (= a b)
             (and (float? a) (float? b) (not= a a) (not= b b))))))

(deftest default-root-scalars-match-generic-whole-results
  (doseq [value ["" "plain" "quote\"slash\\newline\n漢字😀" true false
                 0 -1 1 9007199254740993 any/min-int64 any/max-int64
                 (dec any/min-int64) (inc any/max-int64)
                 18446744073709551615 nil 1.5 ##Inf ##-Inf ##NaN
                 :a/b 'a/b any/empty-value (any/bytes [0 255])
                 \a #{} [] {} [1 "two" false]
                 {:nested ["😀" {:x true}]} {:a 1 "a" 2}]]
    (is (same-result? (any/canonicalize value)
                      (any/canonicalize value any/default-limits))
        (str "whole-result parity for " (pr-str value))))
  (is (= {:value false :nodes 1 :bytes 1 :truncated? false}
         (any/canonicalize false)))
  (is (= {:value 9007199254740993 :nodes 1 :bytes 8 :truncated? false}
         (any/canonicalize 9007199254740993))))

(deftest default-root-string-conservative-boundaries
  (let [limit (quot (:max-bytes any/default-limits) 4)]
    (doseq [unit ["a" "漢" "😀"]
            delta [-1 0 1]]
      ;; Jolt counts codepoints, JVM counts UTF-16 units. Build the boundary
      ;; using this host's count, just like the existing conservative charge.
      (let [value (str (apply str (repeat (quot limit (count unit)) unit))
                       (if (pos? delta) "a" ""))
            value (if (neg? delta) (subs value 0 (dec (count value))) value)
            result (any/canonicalize value)]
        (is (= (any/canonicalize value any/default-limits) result))
        (if (pos? delta)
          (is (= :byte-limit (:error result)))
          (is (= {:value value :nodes 1 :bytes (* 4 (count value))
                  :truncated? false} result)))))))

(deftest generic-options-and-redefined-defaults-retain-budgets
  (doseq [options [{:max-depth -1} {:max-nodes 0} {:max-bytes 0}
                   {:max-bytes 7} {:value-length-limit 1}
                   {:max-depth 1 :max-nodes 2 :max-bytes 12}
                   {:unknown-option true}]
          value ["漢😀" true false 42 nil [1 [2]]
                 {:x ["abcdef" (any/bytes [1 2 3])]}]]
    (let [expected (any/canonicalize value options)
          defaults (merge any/default-limits options)]
      (with-redefs [any/default-limits defaults]
        (is (= expected (any/canonicalize value))))))
  (is (= (any/canonicalize "漢😀")
         (any/canonicalize "漢😀" {:max-bytes nil :unknown-option true})))
  (doseq [i (range 32)
          value [i (- i) (str "漢😀" i)
                 [i false {:nested (str i)}]
                 {:a [i :symbol] :b (any/bytes [i])}]]
    (is (= (any/canonicalize value any/default-limits)
           (any/canonicalize value)))))

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
