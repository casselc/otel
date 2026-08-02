(ns otel.id-test
  (:require [clojure.test :refer [deftest is testing]]
            [otel.id :as id]))

(deftest trace-id-shape
  (testing "a trace id is 32 lowercase hex characters (16 bytes)"
    (dotimes [_ 50]
      (let [t (id/trace-id)]
        (is (= 32 (count t)))
        (is (re-matches #"[0-9a-f]{32}" t))))))

(deftest span-id-shape
  (testing "a span id is 16 lowercase hex characters (8 bytes)"
    (dotimes [_ 50]
      (let [s (id/span-id)]
        (is (= 16 (count s)))
        (is (re-matches #"[0-9a-f]{16}" s))))))

(deftest ids-are-distinct
  (testing "generation is random, not a constant or a counter collision"
    (is (= 200 (count (distinct (repeatedly 200 id/trace-id)))))
    (is (= 200 (count (distinct (repeatedly 200 id/span-id)))))))

(deftest ids-use-the-whole-space
  (testing "every hex position varies — a broken generator often pins nibbles to 0"
    (let [ids (repeatedly 300 id/trace-id)]
      (doseq [i (range 32)]
        (is (> (count (distinct (map #(nth % i) ids))) 4)
            (str "position " i " is not varying"))))))

(deftest invalid-ids-are-all-zero
  (is (= "00000000000000000000000000000000" id/invalid-trace-id))
  (is (= "0000000000000000" id/invalid-span-id)))

(deftest validity-predicates
  (testing "the all-zero id is the one invalid value per the W3C trace-context spec"
    (is (not (id/valid-trace-id? id/invalid-trace-id)))
    (is (not (id/valid-span-id? id/invalid-span-id)))
    (is (id/valid-trace-id? (id/trace-id)))
    (is (id/valid-span-id? (id/span-id)))))

(deftest validity-rejects-malformed
  (testing "wrong length, wrong case, and non-hex are all invalid"
    (is (not (id/valid-trace-id? "abc")))
    (is (not (id/valid-trace-id? nil)))
    (is (not (id/valid-trace-id? (.toUpperCase (id/trace-id)))))
    (is (not (id/valid-trace-id? (str (id/trace-id) "0"))))
    (is (not (id/valid-trace-id? (apply str (repeat 32 "g")))))
    (is (not (id/valid-span-id? "abc")))
    (is (not (id/valid-span-id? nil)))
    (is (not (id/valid-span-id? (apply str (repeat 16 "z")))))))

(deftest generated-ids-are-never-the-invalid-value
  (testing "generation must not emit the all-zero id"
    (is (every? id/valid-trace-id? (repeatedly 100 id/trace-id)))
    (is (every? id/valid-span-id? (repeatedly 100 id/span-id)))))

;; jolt's rand-int runs off a PRNG seeded identically in every process, so ids
;; drawn from it repeat exactly across runs -- every service in a fleet would
;; emit the same trace id and the backend would splice unrelated traces
;; together. Ids must come from OS entropy instead, which is what pinning
;; rand-int to a constant demonstrates: it must not change the output.
(deftest ids-do-not-come-from-the-process-prng
  (let [pinned (with-redefs [rand-int (constantly 1)]
                 (repeatedly 5 id/trace-id))]
    (is (every? id/valid-trace-id? pinned))
    (is (= 5 (count (distinct pinned))))
    (is (not-any? #(= % (apply str (repeat 32 "1"))) pinned)))
  (let [pinned (with-redefs [rand-int (constantly 1)]
                 (repeatedly 5 id/span-id))]
    (is (= 5 (count (distinct pinned))))))

(deftest the-entropy-source-is-the-os
  (testing "distinct draws, and enough spread that it is not a counter"
    (let [draws (repeatedly 20 #(vec (@#'id/entropy 16)))]
      (is (= 20 (count (distinct draws))))
      (is (every? #(= 16 (count %)) draws)))))

(deftest the-fallback-still-produces-distinct-ids
  (testing "a host without OpenSSL gets weaker bytes, but not a repeating stream"
    (let [draws (repeatedly 50 #(vec (@#'id/fallback-bytes 16)))]
      (is (every? #(= 16 (count %)) draws))
      (is (= 50 (count (distinct draws))))))
  (testing "and the ids built from them are still well-formed"
    (let [ids (with-redefs [id/os-entropy? false]
                (repeatedly 50 id/trace-id))]
      (is (every? id/valid-trace-id? ids))
      (is (= 50 (count (distinct ids)))))))
