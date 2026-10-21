(ns otel.scalar-string-map-test
  "Explicit native qualification; run with :scalar-map-native-test.
  This gate fails rather than skips if the optional native capability is absent."
  (:require [clojure.test :refer [deftest is testing] :as t]
            [jolt.scheme :as scheme]
            [otel.any-value-native :as native]
            [otel.any-value :as any]))

(defn fixture [n]
  (into {} (map (fn [i] [(str "key-" i) (case (mod i 3) 0 "λ\n\"" 1 false i)]) (range n))))

(defn scalar-oracle [m]
  (into {} (map (fn [[k v]] [k (any/try-scalar-string v)]) m)))

(deftest primitive-maps-retain-values-order-kind-and-input
  (doseq [n [9 16 32 128]]
    (let [input (with-meta (fixture n) {:source true})
          before (vec input) result (any/try-scalar-string-map input)]
      (is (map? result))
      (is (= (scalar-oracle input) result))
      (is (= (vec (keys input)) (vec (keys result))))
      (is (= (type input) (type result)))
      (is (nil? (meta result)))
      (is (= before (vec input)))
      (is (= {:source true} (meta input)))))
  (let [extremes ((scheme/eval-string "(lambda () (jolt-vector (most-negative-fixnum) (most-positive-fixnum)))"))]
    (doseq [v (into [true false "" "😀"] extremes)]
      (let [m (assoc (fixture 16) "value" v)]
        (is (= (scalar-oracle m) (any/try-scalar-string-map m)))))))

(deftest unsupported-shapes-decline-without-converting
  (doseq [m [nil {} (fixture 8) (fixture 129) (sorted-map "x" 3)
             (apply array-map (mapcat identity (fixture 16)))
             (assoc (fixture 16) :alias 1)
             (assoc (fixture 16) "nil" nil)
             (assoc (fixture 16) "float" 1.5)
             (assoc (fixture 16) "record" any/empty-value)
             (assoc (fixture 16) "bytes" (any/bytes [0 255]))
             (assoc (fixture 16) "nested" {:x 3})
             (assoc (fixture 16) "big" any/max-int64)]]
    (is (nil? (any/try-scalar-string-map m))))
  (is (= (hash "Aa") (hash "BB")))
  (is (nil? (any/try-scalar-string-map (assoc (fixture 16) "Aa" 1 "BB" 2)))))

(deftest source-owned-policy-roots-decline-without-invoking-replacements
  (let [m (fixture 16) calls (atom 0)
        replacement (fn [& _] (swap! calls inc) "custom")]
    (doseq [v [#'any/try-scalar-string #'any/canonicalize #'any/string-bytes]]
      (with-redefs-fn {v replacement}
        #(is (nil? (any/try-scalar-string-map m)))))
    (with-redefs [any/default-limits (into {} any/default-limits)]
      (is (nil? (any/try-scalar-string-map m))))
    (with-redefs [any/min-int64 -10]
      (is (nil? (any/try-scalar-string-map m))))
    (with-redefs [any/max-int64 10]
      (is (nil? (any/try-scalar-string-map m))))
    (is (zero? @calls))
    (is (= (scalar-oracle m) (any/try-scalar-string-map m)))))

(deftest character-boundaries-match-the-sdk-scalar-policy
  (let [maximum (any/default-string-value-max-chars)]
    (doseq [unit ["a" "😀"]]
      (let [text (apply str (repeat maximum unit))
            admitted (assoc (fixture 16) "text" text)
            declined (assoc admitted "text" (str text unit))]
        (is (= text (any/try-scalar-string text)))
        (is (= (scalar-oracle admitted) (any/try-scalar-string-map admitted)))
        (is (nil? (any/try-scalar-string-map declined)))))))

(deftest malformed-optional-native-code-declines-without-losing-the-fallback
  (with-redefs [native/converter-source "("]
    (let [decline (@#'any/load-scalar-string-map-converter)]
      (is (nil? (decline (fixture 16))))
      (is (= "42" (any/try-scalar-string 42)))))
  (is (some? (any/try-scalar-string-map (fixture 16)))))

(defn -main [& _]
  ;; Force the lazy native loader under a PREINSTALLED replacement. Its anchors
  ;; must come from the SDK definition, not this later loading context.
  (let [calls (atom 0)]
    (with-redefs [any/try-scalar-string (fn [& _] (swap! calls inc) "same-text")]
      (assert (nil? (any/try-scalar-string-map (fixture 16))))
      (assert (zero? @calls))))
  (assert (some? (any/try-scalar-string-map (fixture 16)))
          "native scalar-map capability must actually run in this explicit gate")
  (let [{:keys [test pass fail error]} (t/run-tests 'otel.scalar-string-map-test)]
    (assert (and (>= test 5) (>= pass 66)))
    (System/exit (if (zero? (+ fail error)) 0 1))))
