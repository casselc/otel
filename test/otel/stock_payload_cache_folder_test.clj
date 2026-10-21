(ns otel.stock-payload-cache-folder-test
  "Stock-Jolt cache gate with an injected folder; never installs global helpers."
  (:require [clojure.test :as t :refer [deftest is]]
            [jolt.scheme :as scheme]
            [otel.any-value :as any]
            [otel.payload-scalar-string-map-cache-test :as existing]))

;; Independent correctness fixture, deliberately sequence-based. Actual library
;; walker mode reuses the same tests but does not add a production SDK dependency.
(def sequence-folder
  (delay (scheme/eval-string
           "(lambda (m visit initial)
              (let loop ((s (jolt-seq m)) (a initial))
                (if (jolt-nil? s) a
                    (let ((entry (seq-first s)))
                      (loop (seq-more s)
                            (visit (jolt-nth entry 0) (jolt-nth entry 1) a))))))")))
(def ^:dynamic *folder* nil)
(def constructor any/scalar-string-map-payload-converter)
(def budget-constructor any/scalar-string-map-payload-converter*)
(defn global-folder? []
  ((scheme/eval-string "(lambda () (top-level-bound? 'pmap-fold-seq-order))")))

(deftest provided-folder-is-used-for-store-but-not-warm-hits
  (let [calls (atom 0) folder *folder*
        convert (constructor (fn [m visit initial]
                               (swap! calls inc) (folder m visit initial)))
        source (existing/plain-fixture 16)]
    (is (some? convert))
    (is (= (existing/oracle source) (convert source)))
    (is (= (existing/oracle source) (convert source)))
    (is (= 1 @calls))))

(deftest bad-arity-and-noncallable-folders-decline-at-construction
  (is (nil? (constructor (scheme/eval-string "(lambda (m) m)"))))
  (is (nil? (constructor 42)))
  (is (false? (global-folder?))))

(deftest no-injection-retains-the-stock-optional-decline
  (is (nil? (budget-constructor 64 65536 nil)))
  (is (false? (global-folder?))))

(defn -main [& [mode]]
  (assert (false? (global-folder?)) "This gate must run on stock without a global folder")
  (assert (nil? (constructor)))
  (let [folder (if (= mode "library")
                 (do (require 'jolt.codec.experimental)
                     ((ns-resolve 'jolt.codec.experimental 'native-map-folder)
                      ((ns-resolve 'jolt.codec.experimental 'open-codec))))
                 @sequence-folder)]
    (assert (some? (constructor folder)))
    (binding [*folder* folder]
      (with-redefs [any/scalar-string-map-payload-converter
                    (fn ([] (budget-constructor 64 65536 folder))
                      ([provided] (budget-constructor 64 65536 provided)))
                    any/scalar-string-map-payload-converter*
                    (fn ([n chars] (budget-constructor n chars folder))
                      ([n chars provided] (budget-constructor n chars provided)))]
        (let [r (t/run-tests 'otel.payload-scalar-string-map-cache-test
                             'otel.stock-payload-cache-folder-test)]
          (assert (= 19 (:test r)))
          (assert (false? (global-folder?)))
          (System/exit (if (zero? (+ (:fail r) (:error r))) 0 1)))))))
