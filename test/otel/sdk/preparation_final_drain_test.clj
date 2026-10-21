(ns otel.sdk.preparation-final-drain-test
  (:require [clojure.test :refer [deftest is]]
            [otel.sdk.export :as export]
            [otel.sdk.lifecycle :as lifecycle]))

(deftest premature-worker-exit-must-not-authorize-caller-preparation-or-close
  ;; Simulates a premature worker exit with leftover accepted spans. The new
  ;; worker must normally drain them itself; failure is not a caller handoff.
  ;; Use an actually joined thread, not a failed/timed wait or .isAlive guess.
  (let [closed (atom 0) live (atom 0)
        worker (Thread. (fn [] nil))
        adapter {:reserve! (fn [_] (swap! live inc) (Object.))
                 :prepare! (fn [& _] true) :submit! (fn [_] true)
                 :await! (fn [_] true) :abort! (fn [_] true)
                 :release! (fn [_] (throw (ex-info "controlled release failure" {})))
                 :retire! (fn [_] (swap! live dec) true) :fence! (fn [] true)}
        exporter (reify export/SpanExporter
                   (export-spans! [_ _] false)
                   (flush-exporter! [_] true)
                   (shutdown-exporter! [_] (swap! closed inc) true))
        state (atom {:queue [{:span-context {:trace-flags 1}}]
                     :shutdown? false :shutdown-cancelled? false :dropped 0
                     :attempted-span-count 0 :exported-span-count 0 :failed-span-count 0})
        owner (export/->BatchSpanProcessor
               exporter state {:max-export-batch-size 1
                               :experimental-span-preparation adapter}
               worker (lifecycle/terminal-action))]
    (.start worker)
    (.join worker)
    (is (not (.isAlive worker)))
    (is (thrown? Exception (export/shutdown! owner)))
    (is (= 0 @live) "no preparation starts after the worker ownership wait")
    (is (= 0 @closed) "a final-drain retirement failure is not close permission")
    (is (= :unconfirmed (:quiescence (lifecycle/settlement-status owner))))))
