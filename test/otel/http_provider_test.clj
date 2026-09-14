(ns otel.http-provider-test
  (:require [clojure.test :refer [deftest is]]
            [jolt.http-client :as http]
            [jolt.socket]))

(defn- close-quietly [value]
  (when value
    (try (.close value) (catch Throwable _ nil))))

(deftest selected-provider-interrupts-a-blocked-response-read
  ;; A local peer accepts the request and deliberately withholds every response
  ;; byte. The selected provider has a 10 s socket timeout, so returning within
  ;; this bound proves cancellation reached its sliced read rather than merely
  ;; waiting for the ordinary timeout.
  (let [listener (java.net.ServerSocket. 0)
        accepted (promise)
        peer (atom nil)
        _acceptor (future
                    (try
                      (let [socket (.accept listener)]
                        (reset! peer socket)
                        (deliver accepted socket))
                      (catch Throwable error
                        (deliver accepted error))))
        outcome (promise)
        worker (Thread.
                (fn []
                  (deliver outcome
                           (try
                             (http/get
                              (str "http://127.0.0.1:"
                                   (.getLocalPort listener)
                                   "/v1/traces")
                              {:socket-timeout 10000})
                             :returned
                             (catch Throwable error (class error))))))]
    (try
      (.start worker)
      (let [connected (deref accepted 3000 ::not-accepted)]
        (is (not= ::not-accepted connected)
            "the causal peer must accept before cancellation")
        (is (not (instance? Throwable connected))
            (str "the causal peer failed to accept: " connected))
        (let [started (System/currentTimeMillis)]
          (.interrupt worker)
          (let [result (deref outcome 2000 ::still-blocked)
                elapsed (- (System/currentTimeMillis) started)]
            (is (= java.lang.InterruptedException result)
                (str "selected HTTP provider returned " result))
            (is (< elapsed 1500)
                (str "interrupt exceeded the read-slice bound: " elapsed " ms")))))
      (finally
        (close-quietly @peer)
        (close-quietly listener)
        (.join worker 2000)))))
