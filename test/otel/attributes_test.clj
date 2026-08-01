(ns otel.attributes-test
  (:require [clojure.test :refer [deftest is testing]]
            [otel.attributes :as attr]))

(deftest keys-normalize-to-strings
  (testing "keyword and symbol keys become the dotted string form OTLP carries"
    (is (= {"http.method" "GET"} (attr/normalize {:http.method "GET"})))
    (is (= {"http.method" "GET"} (attr/normalize {"http.method" "GET"})))
    (is (= {"db/system" "postgres"} (attr/normalize {:db/system "postgres"})))))

(deftest scalar-values-are-kept
  (is (= {"s" "x"} (attr/normalize {:s "x"})))
  (is (= {"b" true} (attr/normalize {:b true})))
  (is (= {"b" false} (attr/normalize {:b false})))
  (is (= {"i" 42} (attr/normalize {:i 42})))
  (is (= {"d" 1.5} (attr/normalize {:d 1.5}))))

(deftest keyword-values-become-strings
  (testing "a keyword value is a Clojure idiom OTLP has no type for"
    (is (= {"k" "green"} (attr/normalize {:k :green})))
    (is (= {"k" "a/b"} (attr/normalize {:k :a/b})))))

(deftest nil-values-are-dropped
  (testing "OTLP has no null attribute value — the key is omitted entirely"
    (is (= {"a" 1} (attr/normalize {:a 1 :b nil})))
    (is (= {} (attr/normalize {:b nil})))))

(deftest homogeneous-sequences-become-arrays
  (is (= {"a" ["x" "y"]} (attr/normalize {:a ["x" "y"]})))
  (is (= {"a" [1 2 3]} (attr/normalize {:a [1 2 3]})))
  (is (= {"a" [true false]} (attr/normalize {:a [true false]})))
  (testing "a seq is accepted and realized as a vector"
    (is (= {"a" [1 2 3]} (attr/normalize {:a (map inc [0 1 2])})))))

(deftest heterogeneous-sequences-are-dropped
  (testing "OTLP arrays are homogeneous; a mixed array has no representation"
    (is (= {} (attr/normalize {:a [1 "x"]})))
    (is (= {} (attr/normalize {:a [1 true]})))))

(deftest empty-sequences-are-kept
  (testing "an empty array is representable and carries the fact that it was empty"
    (is (= {"a" []} (attr/normalize {:a []})))))

(deftest unrepresentable-values-are-dropped
  (testing "maps, sets and functions have no OTLP attribute type"
    (is (= {} (attr/normalize {:a {:nested 1}})))
    (is (= {} (attr/normalize {:a #{1 2}})))
    (is (= {} (attr/normalize {:a (fn [] 1)})))))

(deftest nil-and-empty-input
  (is (= {} (attr/normalize nil)))
  (is (= {} (attr/normalize {}))))

(deftest value-length-limit-truncates-strings
  (let [limits (attr/limits {:value-length-limit 5})]
    (is (= {"a" "abcde"} (attr/normalize {:a "abcdefghij"} limits)))
    (testing "strings inside an array are truncated too"
      (is (= {"a" ["abcde" "xy"]} (attr/normalize {:a ["abcdefghij" "xy"]} limits))))
    (testing "non-string values are unaffected by a value length limit"
      (is (= {"a" 123456789} (attr/normalize {:a 123456789} limits))))))

(deftest count-limit-drops-excess-attributes
  (let [limits (attr/limits {:count-limit 2})
        result (attr/normalize {:a 1 :b 2 :c 3 :d 4} limits)]
    (is (= 2 (count result)))
    (testing "the kept keys are a subset of the input"
      (is (every? #{"a" "b" "c" "d"} (keys result))))))

(deftest count-limit-counts-only-kept-attributes
  (testing "a dropped nil must not consume a slot against the limit"
    (let [limits (attr/limits {:count-limit 2})]
      (is (= 2 (count (attr/normalize {:a nil :b 1 :c 2} limits)))))))

(deftest default-limits-are-permissive
  (let [big (zipmap (map #(str "k" %) (range 200)) (range 200))]
    (is (= 128 (count (attr/normalize big))))
    (testing "the default has no value length limit"
      (is (= 5000 (count (get (attr/normalize {:a (apply str (repeat 5000 "x"))}) "a")))))))

(deftest merging-prefers-the-later-map
  (is (= {"a" 1 "b" 3 "c" 4} (attr/merge-attrs {:a 1 :b 2} {:b 3 :c 4}))))
