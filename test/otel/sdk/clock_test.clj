(ns otel.sdk.clock-test
  (:require [clojure.test :refer [deftest is testing]]
            [otel.sdk.clock :as clock]))

(deftest system-clock-wall-is-epoch-nanos
  (testing "wall reads the epoch clock, in nanoseconds"
    (let [t (clock/wall-nanos clock/system)]
      ;; nanoseconds since 1970 — a seconds- or millis-valued clock would be many
      ;; orders of magnitude below this floor (2023-11-14).
      (is (> t 1700000000000000000))
      (is (< t 4000000000000000000)))))

(deftest system-clock-mono-is-not-the-wall-clock
  (testing "mono counts from an arbitrary origin, so it is far below epoch nanos"
    (is (< (clock/mono-nanos clock/system) (clock/wall-nanos clock/system)))))

(deftest system-clock-mono-never-goes-backwards
  (let [readings (repeatedly 50 #(clock/mono-nanos clock/system))]
    (is (= readings (sort readings)))))

(deftest system-clock-has-sub-millisecond-resolution
  (testing "a millisecond clock scaled to nanos would leave every reading on a 1e6 boundary"
    (is (some #(pos? (rem % 1000000))
              (repeatedly 200 #(clock/mono-nanos clock/system))))))

(deftest anchored-clock-derives-wall-from-the-monotonic-delta
  (testing "an anchored clock reports wall time that advances by the monotonic delta"
    (let [fake (clock/fake-clock {:wall 1000 :mono 500})
          anch (clock/anchored fake)]
      (is (= 1000 (clock/wall-nanos anch)))
      ;; Step the monotonic clock by 250; wall must advance by exactly that much.
      (clock/advance! fake {:mono 250})
      (is (= 1250 (clock/wall-nanos anch)))
      ;; The underlying wall clock jumping backwards (an ntp step) must NOT be
      ;; visible through the anchor — that is the whole point of anchoring.
      (clock/set-wall! fake 1)
      (is (= 1250 (clock/wall-nanos anch))))))

(deftest anchored-clock-passes-through-monotonic-readings
  (let [fake (clock/fake-clock {:wall 1000 :mono 500})
        anch (clock/anchored fake)]
    (is (= 500 (clock/mono-nanos anch)))
    (clock/advance! fake {:mono 7})
    (is (= 507 (clock/mono-nanos anch)))))

(deftest fake-clock-is-controllable
  (let [c (clock/fake-clock {:wall 10 :mono 20})]
    (is (= 10 (clock/wall-nanos c)))
    (is (= 20 (clock/mono-nanos c)))
    (clock/advance! c {:wall 5 :mono 6})
    (is (= 15 (clock/wall-nanos c)))
    (is (= 26 (clock/mono-nanos c)))))
