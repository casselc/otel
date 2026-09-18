(ns otel.sdk.construction-rollback-test
  (:require [clojure.test :refer [deftest is]]
            [otel.sdk.lifecycle :as lifecycle]))

(deftest construction-receipt-is-invocation-and-failure-specific
  (let [receipt (lifecycle/construction-receipt)
        close-count (atom 0) retired (atom false) early (atom nil)
        owner (reify lifecycle/SettlementWitness
                (settlement-status [_]
                  {:quiescence (if @retired :confirmed :unconfirmed)}))
        value (lifecycle/with-construction!
               receipt
               #(do (lifecycle/acquire-owner!
                     receipt owner (fn [] (swap! close-count inc)
                                     (reset! retired true) true))
                    (reset! early (lifecycle/retire-construction! receipt))
                    owner))
        other-error (ex-info "outside successful constructor" {})]
    (is (identical? owner value))
    (is (= :closing (:status @early)))
    (is (zero? @close-count) "retirement cannot race an acquiring worker start")
    (is (= :closed (:status (lifecycle/retire-construction! receipt))))
    (is (= 1 @close-count))
    (is (= :unknown (:outcome (lifecycle/construction-failure-status receipt other-error))))
    (let [reuse-error (try (lifecycle/with-construction! receipt (constantly nil))
                          nil (catch Throwable error error))]
      (is (= :invalid-construction-receipt (:otel.sdk/error (ex-data reuse-error))))
      (is (= :unknown (:outcome (lifecycle/construction-failure-status receipt reuse-error)))))
    (is (= :unknown (:outcome (lifecycle/construction-failure-status nil other-error))))))

(deftest incomplete-construction-retries-refresh-proof-without-release-replay
  (let [receipt (lifecycle/construction-receipt) started (promise) finish (promise)
        background (Thread. #(do (deliver started true) @finish))
        retired (atom false) closes (atom 0)
        failure (ex-info "controlled acquisition failure" {})
        owner (reify lifecycle/SettlementWitness
                (settlement-status [_]
                  {:quiescence (if (and @retired (not (.isAlive background)))
                                 :confirmed :unconfirmed)}))]
    (.start background)
    (try
      (is (= true (deref started 2000 :timeout)))
      (let [reported (try
                       (lifecycle/with-construction!
                        receipt
                        #(do (lifecycle/acquire-owner!
                              receipt owner (fn [] (swap! closes inc)
                                              (reset! retired true) false))
                             (throw failure)))
                       nil (catch Throwable error error))
            data (ex-data reported) retry-stop! (:retry-stop! data)]
        (is (= #{:otel.sdk/error :retry-stop!} (set (keys data))))
        (is (= :construction-cleanup-incomplete (:otel.sdk/error data)))
        (is (nil? (.getCause reported)))
        (is (fn? retry-stop!))
        (is (= :unconfirmed (:quiescence
                            (lifecycle/construction-failure-status receipt reported))))
        (is (= :unconfirmed (:quiescence
                            (lifecycle/construction-failure-status receipt failure))))
        (is (= :unknown (:outcome (lifecycle/construction-failure-status receipt nil))))
        (is (= :unknown (:outcome
                         (lifecycle/construction-failure-status receipt
                                                                (ex-info "other" {})))))
        (is (= :closing (:status (retry-stop!))))
        (is (= 1 @closes))
        (deliver finish true) (.join background 2000)
        (is (not (.isAlive background)))
        (let [results [(promise) (promise)]
              workers (mapv #(Thread. (fn [] (deliver % (retry-stop!)))) results)]
          (doseq [worker workers] (.start worker))
          (try
            (doseq [result results] (is (= :closed (:status (deref result 2000 :timeout)))))
            (finally
              (doseq [worker workers] (.join worker 2000) (is (not (.isAlive worker)))))))
        (is (= :closed (:status (retry-stop!))))
        (is (= :confirmed (:quiescence
                          (lifecycle/construction-failure-status receipt failure))))
        (is (= 1 @closes)))
      (finally
        (deliver finish true) (.join background 2000)
        (lifecycle/retire-construction! receipt)
        (is (not (.isAlive background)))))))
