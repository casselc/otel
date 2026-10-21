(ns otel.scalar-text-payload-projector-test
  (:require [clojure.test :refer [deftest is run-tests]]
            [clojure.java.io :as io] [clojure.string :as str]
            [otel.any-value :as any]
            [otel.payload-scalar-string-map-cache-test :as existing]
            [jolt.codec.experimental :as codec]
            [jolt.codec.scalar-text.experimental :as shapes]
            [jolt.traversal.experimental :as traversal]))

(def folder (delay (traversal/native-pmap-folder (traversal/open-traversal))))
(defn factory [] (shapes/native-view-plan-factory (codec/native-scalar-text-view-box)))
(defn projector [] (any/scalar-text-payload-projector @folder {:shape-plan-factory (factory)}))
(defn fixture [] (existing/sorted-fixture 17))
(defn encoded [value] (codec/write-nested (codec/open-codec) value))

(deftest original-root-hits-win-and-hamt-learning-follows-original-admission
  (let [ops (atom []) native (factory)
        project (any/scalar-text-payload-projector @folder
                  {:shape-plan-factory (fn [& args]
                                         (let [d (apply native args)]
                                           (fn [op value] (swap! ops conj op) (d op value))))})
        m (fixture) first-result (project m) before @ops
        hit (project m) another-hit (project m)
        distinct (into (sorted-map) m)]
    (is (= (existing/oracle m) first-result hit another-hit))
    (is (not (identical? hit another-hit)))
    (is (= before @ops) "root cache wins before the learned plan")
    (let [view (project distinct)]
      (is (not (map? view)))
      (is (= (encoded first-result) (encoded view))))
    (let [plain (existing/plain-fixture 17) before (count @ops)]
      (is (= (existing/oracle plain) (project plain)))
      (is (= [0 1] (subvec @ops before)) "unknown HAMT checks then learns after admission")
      (let [before @ops]
        (is (map? (project plain)))
        (is (= before @ops) "existing root cache still wins"))
      (let [distinct (into {} plain) view (project distinct)]
        (is view) (is (not (map? view)))
        (is (= (encoded (existing/oracle plain)) (encoded view)))))))

(deftest all-eight-policy-anchors-still-fence-every-representation
  (let [project (projector) m (fixture)]
    (is (map? (project m)))
    (is (not (map? (project (into (sorted-map) m)))))
    (doseq [root existing/eight-roots]
      (with-redefs-fn {root (Object.)}
        #(do (is (nil? (project m)))
             (is (nil? (project (into (sorted-map) m)))))))
    (is (= (encoded (existing/oracle m)) (encoded (project (into (sorted-map) m)))))))

(deftest unsupported-inputs-decline-without-changing-the-old-api
  (let [project (projector) m (fixture)
        old (any/scalar-string-map-payload-converter @folder
              {:shape-plan-factory (shapes/native-plan-factory)})]
    (is (map? (project m)))
    (doseq [bad [nil
                (assoc m "key-0" nil)
                (assoc m "key-0" [1])
                (assoc m "key-0" 1.25)
                (assoc m "key-0" 9223372036854775807)
                (existing/sorted-fixture 129)]]
      (is (nil? (project bad))))
    (dotimes [_ 3]
      (let [result (old (into (sorted-map) m))]
        (is (map? result)) (is (= (existing/oracle m) result))))))

(deftest shape-retention-is-bounded-and-unknown-shapes-still-fall-back
  (let [dispatch (atom nil) native (factory)
        project (any/scalar-text-payload-projector @folder
                  {:shape-plan-factory (fn [& args]
                                         (let [d (apply native args)] (reset! dispatch d) d))})]
    (doseq [i (range 9)]
      (let [m (assoc (fixture) (str "new-key-" i) i)]
        (is (= (encoded (existing/oracle m)) (encoded (project m))))
        (is (= (min (inc i) 8) (first (@dispatch 2 nil))))))
    (let [m (assoc (fixture) "ninth-uncached-shape" 9)]
      (is (map? (project m)))
      (is (map? (project (into (sorted-map) m)))))))

(deftest invalid-options-and-unavailable-capabilities
  (doseq [options [nil {} {:shape-plan-factory 42} {:shape-plan-factory (factory) :max-shapes 1}]]
    (is (thrown? Throwable (any/scalar-text-payload-projector @folder options))))
  (doseq [f [(fn [& _] nil) (fn [& _] 42) (fn [& _] (throw (ex-info "private detail" {})))]]
    (is (nil? (any/scalar-text-payload-projector @folder {:shape-plan-factory f}))))
  (is (nil? (any/scalar-text-payload-projector nil {:shape-plan-factory (factory)}))))

(deftest learned-hamt-views-retain-all-policy-fences-and-original-fallback
  (let [project (projector) m (existing/plain-fixture 17)]
    (is (map? (project m)))
    (let [view (project (into {} m))]
      (is view) (is (not (map? view))))
    (doseq [root existing/eight-roots]
      (with-redefs-fn {root (Object.)}
        #(do (is (nil? (project m))) (is (nil? (project (into {} m)))))))
    (doseq [bad [(assoc m "key-0" nil) (assoc m "key-0" [1])
                 (assoc m "key-0" 1.25) (assoc m "key-0" 9223372036854775807)]]
      (is (nil? (project bad))))
    (let [changed (assoc m "key-0" -42)]
      (is (= (encoded (existing/oracle changed)) (encoded (project changed)))))))

(deftest hamt-view-hits-avoid-the-original-copying-converter
  (doseq [enabled? [false true]]
    (let [calls (atom 0) original @#'any/original-try-scalar-string-map
          project (any/scalar-text-payload-projector @folder
                    {:shape-plan-factory
                     (shapes/native-view-plan-factory (codec/native-scalar-text-view-box) enabled?)})
          base (existing/plain-fixture 17)]
      (with-redefs [any/original-try-scalar-string-map
                    (fn [v] (swap! calls inc) (original v))]
        (doseq [i (range 3)]
          (let [m (assoc base "key-0" i) result (project m)]
            (is result)
            (is (= (encoded (existing/oracle m)) (encoded result))))))
      (is (= (if enabled? 1 3) @calls)))))

(defn -main [& [expected-codec]]
  (when expected-codec
    (assert (str/ends-with? (str (io/resource "jolt/codec/scalar_text/experimental.clj"))
                           (str "/" expected-codec "/incubator/codec/src/jolt/codec/scalar_text/experimental.clj"))))
  (let [r (run-tests 'otel.scalar-text-payload-projector-test)]
    (System/exit (if (zero? (+ (:fail r) (:error r))) 0 1))))
