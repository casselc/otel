(ns otel.payload-scalar-string-map-cache-test
  "Explicit native qualification; run with :scalar-map-native-test.
  This gate fails rather than skips if the optional native capability is
  absent. Prototype coverage only; no exporter/adoption claim is tested."
  (:require [clojure.test :refer [deftest is testing] :as t]
            [jolt.host :as host]
            [jolt.scheme :as scheme]
            [otel.any-value :as any]))

(defn plain-fixture [n]
  (into {} (map (fn [i] [(str "key-" i) (case (mod i 3) 0 "λ\n\"" 1 false i)])
                (range n))))

(defn sorted-fixture [n]
  (into (sorted-map) (map (fn [i] [(str "key-" i) (case (mod i 3) 0 "λ😀" 1 false i)]))
        (range n)))

(defn oracle [m]
  (into {} (map (fn [[k v]] [k (any/try-scalar-string v)])) m))

(def eight-roots
  [#'any/try-scalar-string #'any/canonicalize #'any/default-limits
   #'any/string-bytes #'any/min-int64 #'any/max-int64
   #'any/try-scalar-string-map #'any/try-sorted-scalar-string-map])

(deftest fresh-wrapper-shares-root-but-never-the-same-wrapper
  (let [m (plain-fixture 16) convert (any/scalar-string-map-payload-converter)]
    (is (some? convert) "native payload cache must be available in this gate")
    (let [first-result (convert m) second-result (convert m)]
      (is (= (oracle m) first-result second-result))
      (is (not (identical? first-result second-result)))
      (is ((scheme/eval-string "(lambda (a b) (eq? (pmap-root a) (pmap-root b)))")
           first-result second-result)))))

(deftest repeated-calls-on-the-same-source-skip-reconversion
  (let [m (plain-fixture 24) convert (any/scalar-string-map-payload-converter)
        calls (atom 0) real @#'any/original-try-scalar-string-map]
    (with-redefs [any/original-try-scalar-string-map
                  (fn [v] (swap! calls inc) (real v))]
      (is (= (oracle m) (convert m)))
      (is (= 1 @calls))
      (is (= (oracle m) (convert m)))
      (is (= 1 @calls) "a warm identity hit must not re-run the original converter"))))

(deftest plain-and-sorted-maps-match-existing-helper-order-type-and-metadata
  (let [convert (any/scalar-string-map-payload-converter)]
    (doseq [n [9 16 128]]
      (let [plain (with-meta (plain-fixture n) {:source true})
            before (vec plain) result (convert plain)]
        (is (= (oracle plain) result))
        (is (= (vec (keys plain)) (vec (keys result))))
        (is (map? result))
        (is (nil? (meta result)))
        (is (= before (vec plain)) "input is never mutated")
        (is (= {:source true} (meta plain)))))
    (doseq [n [0 1 9 16 128]]
      (let [sorted (with-meta (sorted-fixture n) {:source true})
            before (vec sorted) result (convert sorted)]
        (is (= (oracle sorted) result))
        (is (= (vec (oracle sorted)) (vec result)))
        (is (nil? (meta result)))
        (is (= before (vec sorted)))))))

(deftest unsupported-shapes-decline-like-the-existing-helpers
  (let [convert (any/scalar-string-map-payload-converter)]
    (doseq [m [nil {} (plain-fixture 1) (plain-fixture 8)
               (plain-fixture 129) (hash-map "a" 1) (sorted-set "a")
               (apply array-map (mapcat identity (plain-fixture 16)))
               (assoc (plain-fixture 16) :alias 1)
               (assoc (plain-fixture 16) "nil" nil)
               (assoc (plain-fixture 16) "float" 1.5)
               (assoc (plain-fixture 16) "nested" {:x 3})
               (assoc (plain-fixture 16) "big" any/max-int64)
               (assoc (sorted-fixture 9) "float" 1.25)
               (assoc (sorted-fixture 9) "nested" [1])]]
      (is (nil? (convert m))))
    (is (= (hash "Aa") (hash "BB")))
    (is (nil? (convert (assoc (plain-fixture 16) "Aa" 1 "BB" 2))))))

(deftest eight-policy-roots-decline-before-first-construction-without-replacement-calls
  (doseq [root eight-roots]
    (let [calls (atom 0) replacement (fn [& _] (swap! calls inc) "replacement")]
      (with-redefs-fn {root replacement}
        (fn []
          (let [convert (any/scalar-string-map-payload-converter)
                m (plain-fixture 16)]
            (is (or (nil? convert) (nil? (convert m))))
            (is (zero? @calls))))))))

(deftest eight-policy-roots-decline-after-a-warm-hit-and-restore-cleanly
  (let [convert (any/scalar-string-map-payload-converter) m (plain-fixture 16)]
    (is (= (oracle m) (convert m)) "warm the cache with one real hit first")
    (doseq [root eight-roots]
      (let [calls (atom 0) replacement (fn [& _] (swap! calls inc) "replacement")]
        (with-redefs-fn {root replacement}
          (fn [] (is (nil? (convert m)))))
        (is (zero? @calls)
            (str "replaced root " root " must never be invoked by the cache"))))
    (is (= (oracle m) (convert m))
        "roots restored: the cache must resume ordinary reuse, not stay poisoned")))

(deftest sorted-map-ops-and-cnt-mutation-miss-rather-than-stale-hit
  (let [convert (any/scalar-string-map-payload-converter)]
    (testing "an :ops swap misses the cache instead of replaying the primed value"
      (let [m (sorted-fixture 9) good (oracle m)]
        (is (= good (convert m)) "prime the cache for this sorted map identity")
        (host/ref-put! m :ops (into {} (host/ref-get m :ops)))
        (is (nil? (convert m))
            "a non-stock :ops must decline like the original helper, never a stale hit")))
    (testing "a :cnt corruption misses the cache instead of replaying the primed value"
      (let [m (sorted-fixture 9) good (oracle m)]
        (is (= good (convert m)) "prime the cache for this sorted map identity")
        (host/ref-put! m :cnt 8)
        (is (nil? (convert m))
            "a :cnt that no longer matches the tree must decline, never a stale hit")))))

(deftest a-budget-decline-still-returns-a-correct-uncached-conversion
  (let [convert (any/scalar-string-map-payload-converter* 2 65536)
        maps (mapv (fn [i] (plain-fixture (+ 10 i))) (range 4))]
    (doseq [m maps] (is (= (oracle m) (convert m))))
    (let [over-budget (last maps) calls (atom 0) real @#'any/original-try-scalar-string-map]
      (with-redefs [any/original-try-scalar-string-map
                    (fn [v] (swap! calls inc) (real v))]
        (is (= (oracle over-budget) (convert over-budget)))
        (is (= (oracle over-budget) (convert over-budget)))
        (is (= 2 @calls)
            "an entry outside the insertion budget must recompute on every call")))))

(deftest a-tiny-character-budget-still-returns-correct-uncached-conversions
  (let [convert (any/scalar-string-map-payload-converter* 64 1)
        m (plain-fixture 16) calls (atom 0) real @#'any/original-try-scalar-string-map]
    (with-redefs [any/original-try-scalar-string-map
                  (fn [v] (swap! calls inc) (real v))]
      (is (= (oracle m) (convert m)))
      (is (= (oracle m) (convert m)))
      (is (= 2 @calls) "a 1-char budget admits no entry, so every call recomputes"))))

(deftest sorted-tree-replacement-cannot-replay-the-old-value
  (let [convert (any/scalar-string-map-payload-converter)
        m (sorted-fixture 16)
        replacement (assoc (sorted-fixture 16) "key-0" "new-tree")]
    (is (= (oracle m) (convert m)))
    (host/ref-put! m :tree (host/ref-get replacement :tree))
    (is (= (oracle replacement) (convert m)))
    (is (not= (oracle (sorted-fixture 16)) (convert m)))))

(deftest oversized-keys-count-against-the-character-budget
  (let [convert (any/scalar-string-map-payload-converter* 64 1024)
        m (assoc (plain-fixture 16) (apply str (repeat 2048 "k")) 1)
        calls (atom 0) real @#'any/original-try-scalar-string-map]
    (with-redefs [any/original-try-scalar-string-map
                  (fn [v] (swap! calls inc) (real v))]
      (is (= (oracle m) (convert m)))
      (is (= (oracle m) (convert m)))
      (is (= 2 @calls) "keys, not just formatted values, consume the budget"))))

(deftest independently-created-payloads-do-not-share-conversions
  (let [a (any/scalar-string-map-payload-converter)
        b (any/scalar-string-map-payload-converter)
        m (plain-fixture 16) calls (atom 0)
        real @#'any/original-try-scalar-string-map]
    (with-redefs [any/original-try-scalar-string-map
                  (fn [v] (swap! calls inc) (real v))]
      (doseq [convert [a a b b]] (is (= (oracle m) (convert m))))
      (is (= 2 @calls)))))

(deftest cache-does-not-retain-source-wrapper-or-its-metadata
  ;; The weak witness is constructed in a separate native frame, so the test
  ;; itself cannot accidentally keep the metadata alive through a local slot.
  ;; A separate strong witness calibrates that collection is not just clearing
  ;; every weak pointer indiscriminately. Both weak pairs stay live throughout.
  (let [convert (any/scalar-string-map-payload-converter)
        make-input (fn []
                     (let [sentinel (Object.)]
                       [(with-meta (plain-fixture 16) {:sentinel sentinel}) sentinel]))
        probe (scheme/eval-string
                "(lambda (make-input convert retain?)
                   (let* ((strong (vector #f))
                          (witness
                            ((lambda ()
                               (let* ((input (make-input))
                                      (m (pvec-nth! input 0))
                                      (sentinel (pvec-nth! input 1)))
                                 (convert m)
                                 (when retain? (vector-set! strong 0 sentinel))
                                 (weak-cons sentinel #f))))))
                     (collect (collect-maximum-generation))
                     (collect (collect-maximum-generation))
                     (let ((cleared? (bwp-object? (car witness))))
                       (convert (pvec-nth! (make-input) 0))
                       (if retain?
                           (and (not cleared?) (eq? (car witness) (vector-ref strong 0)))
                           cleared?))))")]
    (is (probe make-input convert false))
    (is (probe make-input convert true))))

(defn -main [& _]
  (let [convert (any/scalar-string-map-payload-converter)]
    (assert (some? convert)
            "native scalar-string-map payload cache must actually run in this explicit gate")
    (assert (some? (convert (plain-fixture 16))))
    (assert (some? (convert (sorted-fixture 16)))))
  (let [{:keys [test pass fail error]} (t/run-tests 'otel.payload-scalar-string-map-cache-test)]
    (assert (and (>= test 9) (>= pass 40)))
    (System/exit (if (zero? (+ fail error)) 0 1))))
