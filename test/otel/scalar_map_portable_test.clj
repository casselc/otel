(ns otel.scalar-map-portable-test
  "Isolated BB/JVM qualification of the optional helper's decline contract.
  Does not claim portability of the full SDK or OTLP codec."
  (:require [clojure.test :refer [deftest is] :as t]
            [otel.any-value :as any]))

(deftest unsupported-host-retains-the-scalar-fallback
  (is (nil? (find-ns 'jolt.scheme)))
  (let [m (into {} (map (fn [i] [(str "key-" i) (case (mod i 3) 0 "λ" 1 false i)]) (range 16)))
        before (vec m)]
    (is (nil? (any/try-scalar-string-map m)))
    (is (= before (vec m)))
    (is (= "false" (any/try-scalar-string false)))
    (is (= "42" (any/try-scalar-string 42)))
    (is (= "λ" (any/try-scalar-string "λ")))
    (is (= :int64-range (:error (any/canonicalize 9223372036854775808N))))))

(defn -main [& _]
  (let [{:keys [test pass fail error]} (t/run-tests 'otel.scalar-map-portable-test)]
    (assert (and (= test 1) (>= pass 7)))
    (System/exit (if (zero? (+ fail error)) 0 1))))
