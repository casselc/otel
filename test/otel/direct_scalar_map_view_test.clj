(ns otel.direct-scalar-map-view-test
  (:require [clojure.test :refer [deftest is run-tests]]
            [otel.any-value :as any] [otel.any-value-native :as native]
            [otel.payload-scalar-string-map-cache-test :as existing]
            [otel.scalar-text-payload-projector-test :as old]
            [jolt.codec.experimental :as codec]
            [jolt.scheme :as scheme]))

(defn projector
  ([] (projector (codec/native-direct-scalar-map-box)))
  ([box] (any/scalar-text-payload-projector @old/folder
           {:shape-plan-factory (old/factory) :ordinary-map-view-box box})))
(defn encoded [v] (codec/write-nested (codec/open-codec) v))

(deftest ordinary-map-views-preserve-order-values-and-immutable-inputs
  (let [p (projector)
        inspect (scheme/eval-string
                  "(lambda (v source)
                     (let ((m ((record-accessor (record-rtd v) 0) v)))
                       (jolt-vector (not (eq? m source))
                         (eq? (pmap-root m) (pmap-root source))
                         (jolt-nil? (pmap-meta m)))))")]
    (doseq [n [9 16 32 128]]
      (let [m (with-meta (existing/plain-fixture n) {:do-not-retain "metadata"})
            a (p m) b (p m) changed (assoc m "key-0" "changed")]
        (is (some? a)) (is (not (map? a))) (is (not (identical? a b)))
        (is (true? (= (encoded (existing/oracle m)) (encoded a) (encoded b))))
        (is (= [true true true] (inspect a m)))
        (is (true? (= (encoded (existing/oracle changed)) (encoded (p changed)))))
        (is (true? (not= (encoded a) (encoded (p changed)))))
        (is (= {:do-not-retain "metadata"} (meta m)))))
    (doseq [v ((scheme/eval-string
                 "(lambda () (jolt-vector (most-negative-fixnum) (most-positive-fixnum) #t #f \"quote\\\"slash\\\\\\n漢字😀\"))"))]
      (let [m (assoc (existing/plain-fixture 16) "value" v)]
        (is (true? (= (encoded (existing/oracle m)) (encoded (p m)))))))))

(deftest all-eight-original-policy-anchors-fence-existing-and-new-projectors
  (let [p (projector) m (existing/plain-fixture 17) calls (atom 0)]
    (is (some? (p m)))
    (doseq [root existing/eight-roots]
      (with-redefs-fn {root (fn [& _] (swap! calls inc) "replacement")}
        #(do (is (nil? (p m)))
             (is (nil? ((projector) m))))))
    (is (zero? @calls))
    (is (true? (= (encoded (existing/oracle m)) (encoded (p m)))))))

(deftest admission-is-identical-and-unsupported-inputs-never-reach-box
  (let [calls (atom 0) box (codec/native-direct-scalar-map-box)
        p (projector (fn [m] (swap! calls inc) (box m)))
        m (existing/plain-fixture 16)
        maximum (any/default-string-value-max-chars)]
    (doseq [bad [nil {} (existing/plain-fixture 8) (existing/plain-fixture 129)
                 (apply array-map (mapcat identity m))
                 (assoc m :keyword 1) (assoc m "null" nil) (assoc m "float" 1.25)
                 (assoc m "nested" [1]) (assoc m "big" any/max-int64)
                 (assoc m "text" (apply str (repeat (inc maximum) "😀")))
                 (assoc m "Aa" 1 "BB" 2)]]
      (is (nil? (any/try-scalar-string-map bad)))
      (is (nil? (p bad))))
    (is (zero? @calls))
    (doseq [v [(apply str (repeat maximum "😀")) false -42]]
      (let [good (assoc m "value" v)]
        (is (some? (any/try-scalar-string-map good)))
        (is (true? (= (encoded (existing/oracle good)) (encoded (p good)))))))
    (is (= 3 @calls))
    (let [sorted (existing/sorted-fixture 17) ordinary (any/scalar-string-map-payload-converter @old/folder)]
      (is (true? (= (encoded (ordinary sorted)) (encoded (p sorted)))))
      (is (true? (= (encoded (existing/oracle sorted)) (encoded (p (into (sorted-map) sorted)))))))
    (is (= 3 @calls))))

(deftest output-limits-depth-independent-output-and-failure-stop
  (let [p (projector) c (codec/open-codec) m (existing/plain-fixture 17)
        v (p m) ordinary (existing/oracle m)
        expected (codec/encode-prefixed-records c "SQL\n" [ordinary ordinary] vector 100000)
        limit (- (alength expected) 4)
        output (codec/encode-prefixed-records c "SQL\n" [v v] vector limit)]
    (is (true? (= (vec expected) (vec output))))
    (is (thrown? Throwable (codec/encode-prefixed-records c "SQL\n" [v v] vector (dec limit))))
    (aset-byte output 0 (byte 0))
    (is (true? (= (vec expected) (vec (codec/encode-prefixed-records c "SQL\n" [v v] vector limit)))))
    (doseq [depth [127 128]]
      (let [nest #(nth (iterate vector %) depth)
            attempt #(try [:ok (encoded (nest %))] (catch Throwable _ [:rejected]))]
        (is (true? (= (attempt ordinary) (attempt v))))))
    (let [calls (atom 0)]
      (is (thrown? Throwable (codec/encode-prefixed-records c "" [v :later]
                              (fn [x] (swap! calls inc) [x]) 1)))
      (is (= 1 @calls)))))

(deftest explicit-capability-validation-and-old-api-stays-map-returning
  (doseq [box [nil false 42]]
    (is (thrown? Throwable (projector box))))
  (with-redefs [native/load-scalar-map-view-projector! (fn [& _] nil)]
    (is (nil? (projector))))
  (let [m (existing/plain-fixture 17) p (projector)
        old-convert (any/scalar-string-map-payload-converter @old/folder)]
    (is (not (map? (p m))))
    (is (map? (old-convert m)))
    (is (= (existing/oracle m) (old-convert m)))))

(defn -main [& _]
  (let [r (run-tests 'otel.direct-scalar-map-view-test 'otel.scalar-text-payload-projector-test)]
    (System/exit (if (zero? (+ (:fail r) (:error r))) 0 1))))
