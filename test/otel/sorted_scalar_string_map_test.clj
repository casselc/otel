(ns otel.sorted-scalar-string-map-test
  (:require [clojure.test :refer [deftest is run-tests]]
            [otel.any-value :as any] [jolt.host :as host] [jolt.scheme :as scheme]
            [clojure.java.io :as io] [clojure.string :as str]))

(defn fixture [n]
  (into (sorted-map) (map (fn [i] [(str "key-" i) (case (mod i 3) 0 "λ😀" 1 false i)])) (range n)))

(defn oracle [m]
  (into {} (map (fn [[k v]] [k (any/try-scalar-string v)])) m))

(def preinstalled-result
  (with-redefs [any/try-scalar-string (fn [_] (throw (ex-info "must-not-call" {})))]
    (any/try-sorted-scalar-string-map (fixture 9))))

(deftest admitted-map-matches-plain-map-kind-order-metadata-and-values
  (is (nil? preinstalled-result))
  (doseq [n [0 1 8 9 16 128]]
    (let [m (with-meta (fixture n) {:source n}) before (vec m)
          expected (oracle m) result (any/try-sorted-scalar-string-map m)]
      (is (= expected result))
      (is (= (type expected) (type result)))
      (is (= (vec expected) (vec result)))
      (is (nil? (meta result)))
      (is (= before (vec m)))
      (is (= {:source n} (meta m)))))
  (doseq [m [(assoc (fixture 9) "Aa" 1 "BB" 2)
             (into (sorted-map-by #(compare %2 %1)) (fixture 16))]]
    (is (= (vec (oracle m)) (vec (any/try-sorted-scalar-string-map m)))))
  (let [[lo hi] (scheme/eval-string "(jolt-vector (most-negative-fixnum) (most-positive-fixnum))")
        m (sorted-map "lo" lo "hi" hi)]
    (is (= (oracle m) (any/try-sorted-scalar-string-map m)))))

(deftest unsupported-shapes-and-policy-roots-decline-without-helper-calls
  (doseq [m [nil {} (hash-map "a" 1) (sorted-set "a") (fixture 129)
             (sorted-map :a 1) (assoc (fixture 9) "nil" nil)
             (assoc (fixture 9) "float" 1.25) (assoc (fixture 9) "nested" [1])
             (assoc (fixture 9) "wide" 9223372036854775807)]]
    (is (nil? (any/try-sorted-scalar-string-map m))))
  (let [m (fixture 16) calls (atom 0)]
    (with-redefs [any/try-scalar-string (fn [_] (swap! calls inc) "changed")]
      (is (nil? (any/try-sorted-scalar-string-map m))))
    (with-redefs [any/canonicalize (fn [_] (swap! calls inc) {:value "changed"})]
      (is (nil? (any/try-sorted-scalar-string-map m))))
    (with-redefs [any/string-bytes (fn [_] (swap! calls inc) 0)]
      (is (nil? (any/try-sorted-scalar-string-map m))))
    (with-redefs [any/default-limits (into {} any/default-limits)]
      (is (nil? (any/try-sorted-scalar-string-map m))))
    (with-redefs [any/min-int64 -1] (is (nil? (any/try-sorted-scalar-string-map m))))
    (with-redefs [any/max-int64 1] (is (nil? (any/try-sorted-scalar-string-map m))))
    (is (zero? @calls))))

(deftest unknown-table-operations-decline-without-trial-callbacks
  (doseq [operation [:seq :count :get]]
    (let [m (fixture 16) calls (atom 0)]
      (host/ref-put! m :ops
                     (assoc (host/ref-get m :ops) operation
                            (fn [& _] (swap! calls inc) (throw (ex-info "custom-op" {})))))
      (is (nil? (any/try-sorted-scalar-string-map m)))
      (is (zero? @calls)))))

(deftest comparator-is-never-called-during-admission-or-copy
  (let [armed (atom false) calls (atom 0)
        m (into (sorted-map-by (fn [a b]
                                (when @armed (swap! calls inc) (throw (ex-info "comparison" {})))
                                (compare b a))) (fixture 16))
        expected (oracle m)]
    (reset! armed true)
    (is (= expected (any/try-sorted-scalar-string-map m)))
    (is (zero? @calls))))

(deftest malformed-node-shapes-and-counts-decline
  (doseq [[field value] [[:cnt -1] [:cnt 0] [:cnt "one"]
                         [:tree [nil "a" 1 nil]] [:tree [nil "a" 1 nil nil :extra]]
                         [:tree [nil :not-string 1 nil nil]]
                         [:tree [nil "a" 1 :not-a-node nil]]]]
    (let [m (sorted-map "a" 1)]
      (host/ref-put! m field value)
      (is (nil? (any/try-sorted-scalar-string-map m)))))
  (let [m (fixture 1) maximum (any/default-string-value-max-chars)]
    (is (some? (any/try-sorted-scalar-string-map (assoc m "text" (apply str (repeat maximum "x"))))))
    (is (nil? (any/try-sorted-scalar-string-map (assoc m "text" (apply str (repeat (inc maximum) "x"))))))))

(deftest loader-geometry-probe-has-an-order-sensitive-negative-control
  (let [source (slurp (io/resource "otel/sorted_scalar_string_map.ss"))
        mutant (str/replace source "(copy-tree! (pvec-nth! tree 3) out)"
                                   "(copy-tree! (pvec-nth! tree 4) out)")
        ;; Replace only the left visit; the right subtree is now visited twice.
        ;; A mere wrapper UID check would accept this source unchanged.
        prototype @#'any/scalar-sorted-map-prototype
        factory (scheme/eval-string source)]
    (is (not= (count source) 0))
    (is (not= source mutant))
    (is (some? ((factory [] 100 prototype) (fixture 9))))
    (is (thrown? Throwable ((scheme/eval-string mutant) [] 100 prototype)))))

(defn -main [& _]
  (let [r (run-tests 'otel.sorted-scalar-string-map-test)]
    (assert (= 6 (:test r)))
    (System/exit (if (zero? (+ (:fail r) (:error r))) 0 1))))
