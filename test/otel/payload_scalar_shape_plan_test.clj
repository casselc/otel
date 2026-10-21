(ns otel.payload-scalar-shape-plan-test
  "Explicit stock-native optional capability gate; no runtime-global helpers."
  (:require [clojure.test :refer [deftest is run-tests]]
            [otel.any-value :as any]
            [otel.payload-scalar-string-map-cache-test :as existing]
            [jolt.codec.scalar-text.experimental :as shapes]
            [jolt.traversal.experimental :as traversal]
            [jolt.scheme :as scheme]))

(def folder (delay (traversal/native-pmap-folder (traversal/open-traversal))))
(defn new-converter
  ([] (new-converter {}))
  ([options] (any/scalar-string-map-payload-converter
               @folder (merge {:shape-plan-factory (shapes/native-plan-factory)} options))))
(defn fixture [] (existing/sorted-fixture 16))
(defn global-folder? [] ((scheme/eval-string "(lambda () (top-level-bound? 'pmap-fold-seq-order))")))

(deftest original-sorted-fallback-is-only-used-for-first-new-shape
  (let [calls (atom 0) original @#'any/original-try-sorted-scalar-string-map
        m (with-meta (fixture) {:not-output "metadata"})
        other (into (sorted-map) m) changed (assoc other "key-0" "changed")
        convert (new-converter)]
    (is (ifn? convert)) (is (false? (global-folder?)))
    (with-redefs [any/original-try-sorted-scalar-string-map
                  (fn [v] (swap! calls inc) (original v))]
      (let [one (convert m) two (convert m) three (convert other)]
        (is (= (existing/oracle m) one two three))
        (is (= (vec (existing/oracle m)) (vec three)))
        (is (= (type (existing/oracle m)) (type three)))
        (is (nil? (meta one))) (is (not (identical? one two)))
        (is (not (identical? two three))))
      (is (= (existing/oracle changed) (convert changed)))
      (is (= 1 @calls))
      (let [unknown (assoc m "other-field" true)]
        (is (= (existing/oracle unknown) (convert unknown)))
        (is (= 2 @calls))
        (is (= (existing/oracle unknown) (convert (into (sorted-map) unknown))))
        (is (= 2 @calls))))))

(deftest shared-roots-hit-before-the-plan-and-return-fresh-wrappers
  (let [ops (atom []) factory (shapes/native-plan-factory)
        convert (new-converter {:shape-plan-factory
                                (fn [& args]
                                  (let [dispatch (apply factory args)]
                                    (fn [op value] (swap! ops conj op) (dispatch op value))))})
        m (fixture) expected (existing/oracle m)]
    (is (= expected (convert m)))
    (let [before @ops one (convert m) two (convert m)]
      (is (= before @ops)) (is (= expected one two))
      (is (not (identical? one two))))))

(deftest schema-and-root-cache-budgets-are-independent-and-bounded
  (doseq [options [{:max-shapes 1} {:max-shape-key-chars 90}]]
    (let [dispatch (atom nil) factory (shapes/native-plan-factory)
          calls (atom 0) original @#'any/original-try-sorted-scalar-string-map
          convert (new-converter (assoc options :shape-plan-factory
                                    (fn [& args] (let [d (apply factory args)] (reset! dispatch d) d))))
          m (fixture) other (sorted-map "other-shape" false)]
      (with-redefs [any/original-try-sorted-scalar-string-map
                    (fn [v] (swap! calls inc) (original v))]
        (is (= (existing/oracle m) (convert m)))
        (is (= {"other-shape" "false"} (convert other)))
        (is (= [1 86] (@dispatch 2 nil)))
        (is (= {"other-shape" "false"} (convert (into (sorted-map) other))))
        (is (= 3 @calls) "full plan budget must leave new-shape conversion on original fallback")
        (is (= (existing/oracle m) (convert (into (sorted-map) m))))
        (is (= 3 @calls)))))
  ;; No source-value caching at all; the learned key geometry still works.
  (let [calls (atom 0) original @#'any/original-try-sorted-scalar-string-map
        convert (any/scalar-string-map-payload-converter* 0 0 @folder
                  {:shape-plan-factory (shapes/native-plan-factory)}) m (fixture)]
    (with-redefs [any/original-try-sorted-scalar-string-map
                  (fn [v] (swap! calls inc) (original v))]
      (dotimes [_ 3] (is (= (existing/oracle m) (convert (into (sorted-map) m)))))
      (is (= 1 @calls)))))

(deftest all-original-policy-roots-still-fence-cold-plan-and-root-hits
  (let [m (fixture) convert (new-converter)]
    (is (= (existing/oracle m) (convert m)))
    (doseq [root existing/eight-roots]
      (with-redefs-fn {root (Object.)}
        #(do (is (nil? (convert m)))
             (is (nil? (convert (into (sorted-map) m)))))))
    (is (= (existing/oracle m) (convert (into (sorted-map) m)))))
  (let [convert (with-redefs [any/default-limits {}] (new-converter)) m (fixture)]
    (is (= (existing/oracle m) (convert m)))))

(deftest unsupported-values-and-plain-inputs-keep-original-behavior
  (let [convert (new-converter) m (fixture)]
    (is (= (existing/oracle m) (convert m)))
    (doseq [bad [nil (assoc m "key-0" nil) (assoc m "key-0" 1.25)
                 (assoc m "key-0" [1]) (existing/sorted-fixture 129)
                 (assoc m "key-0" 9223372036854775807)]]
      (is (nil? (convert bad))))
    (let [plain (existing/plain-fixture 16)]
      (is (= (existing/oracle plain) (convert plain))))))

(deftest invalid-options-and-unavailable-capability-fail-at-construction
  (doseq [options [{} {:shape-plan-factory 42} {:unknown true}
                   {:shape-plan-factory (shapes/native-plan-factory) :max-shapes 9}
                   {:shape-plan-factory (shapes/native-plan-factory) :max-shape-key-chars 8193}]]
    (is (thrown? Throwable (any/scalar-string-map-payload-converter @folder options))))
  (doseq [factory [(fn [& _] nil) (fn [& _] 42)
                   (fn [& _] (throw (ex-info "private-constructor-detail" {})))]]
    (is (nil? (any/scalar-string-map-payload-converter @folder {:shape-plan-factory factory}))))
  (is (nil? (any/scalar-string-map-payload-converter nil {:shape-plan-factory (shapes/native-plan-factory)})))
  (is (ifn? (any/scalar-string-map-payload-converter @folder nil))))

(defn -main [& _]
  (let [r (run-tests 'otel.payload-scalar-shape-plan-test)]
    (is (false? (global-folder?)))
    (System/exit (if (zero? (+ (:fail r) (:error r))) 0 1))))
