(ns otel.sdk.registry-publication-lifetime-test
  (:require [clojure.test :refer [deftest is]]
            [otel.sdk :as sdk]
            [otel.sdk.export :as export]
            [otel.sdk.lifecycle :as lifecycle]))

(defn- authority [] @(deref (ns-resolve 'otel.sdk 'registry)))
(defn- notification [] (deref (ns-resolve 'otel.sdk 'global)))

(deftest rollback-and-previous-retirement-have-two-safe-orders
  (doseq [order [:retire-first :rollback-first]]
    (let [previous (sdk/init! {:exporter :none :metrics? false})
          candidate (sdk/init! {:exporter :none :metrics? false})]
      (try
        (is (identical? (:tracer-provider candidate) (sdk/tracer-provider)))
        (when (= :retire-first order) (sdk/shutdown! previous))
        ((:retire! (:global-installation candidate)))
        (is (identical? (when (= :rollback-first order) (:tracer-provider previous))
                        (sdk/tracer-provider)))
        (sdk/shutdown! previous)
        (is (nil? (sdk/tracer-provider)))
        (is (true? @(:shutdown? (:tracer-provider previous))))
        (is (= :confirmed (:quiescence
                            (lifecycle/component-settlement
                              (:owner (:global-installation candidate))))))
        (finally
          (sdk/shutdown! candidate)
          (sdk/shutdown! previous)
          (is (empty? (:active (authority)))))))))

(deftest reentrant-notification-error-does-not-skip-real-resource-retirement
  (let [closes (atom {:spans 0 :metrics 0})
        exporter (reify export/SpanExporter
                   (export-spans! [_ _] true) (flush-exporter! [_] true)
                   (shutdown-exporter! [_] (swap! closes update :spans inc) true)
                   export/MetricExporter
                   (export-metrics! [_ _ _] true)
                   (shutdown-metric-exporter! [_] (swap! closes update :metrics inc) true))
        handle (sdk/init! {:exporter exporter :schedule-delay-ms 60000
                          :metric-interval-ms 60000})
        owners [(first (:shutdown-components handle)) (:reader handle)]
        replacement (atom nil) fired? (atom false)
        failure (ex-info "controlled notification failure" {})
        global (notification) key ::retirement-notification]
    (try
      (add-watch global key
        (fn [_ _ _ _]
          (when (compare-and-set! fired? false true)
            ;; Observe actual reentrant publication, never assert in observers.
            (reset! replacement (sdk/init! {:exporter :none :metrics? false}))
            (throw failure))))
      (let [error (try (sdk/shutdown! handle) nil (catch Throwable error error))]
        (is @fired?)
        (is (identical? failure error))
        (is (= {:spans 1 :metrics 1} @closes))
        (is (every? #(not (.isAlive (:worker %))) owners))
        (is (= :confirmed (:quiescence (sdk/shutdown-status handle))))
        (is (identical? (:tracer-provider @replacement) (sdk/tracer-provider)))
        (is (identical? failure
                        (try (sdk/shutdown! handle) nil (catch Throwable error error))))
        (is (= {:spans 1 :metrics 1} @closes)))
      (finally
        (remove-watch global key)
        (try (sdk/shutdown! handle) (catch Throwable _ nil))
        (when @replacement (sdk/shutdown! @replacement))
        (doseq [owner owners]
          (try (export/shutdown! owner) (catch Throwable _ nil))
          (.join (:worker owner) 2000)
          (is (not (.isAlive (:worker owner)))))
        (is (empty? (:active (authority))))))))

(deftest hidden-installation-retirement-and-versioned-view-do-not-regress
  (let [previous (sdk/init! {:exporter :none :metrics? false})
        old-snapshot (authority)
        replacement (sdk/init! {:exporter :none :metrics? false})
        global (notification) notifications (atom 0) key ::revision-notification
        notify! @(ns-resolve 'otel.sdk 'notify-registry!)]
    (try
      (is (= 2 (count (:active (authority)))))
      (sdk/shutdown! previous)
      (is (= 1 (count (:active (authority)))))
      (is (identical? (:tracer-provider replacement) (sdk/tracer-provider)))
      (let [snapshot (authority) revision (:revision snapshot)]
        (add-watch global key (fn [_ _ _ _] (swap! notifications inc)))
        (notify! old-snapshot)
        (notify! snapshot)
        (notify! snapshot)
        (is (zero? @notifications))
        (is (= revision (:registry-revision @global)))
        (is (identical? (:tracer-provider replacement) (:tracer-provider @global))))
      (sdk/shutdown! replacement)
      (is (empty? (:active (authority))))
      (is (nil? (sdk/tracer-provider)))
      (is (= 1 @notifications))
      (finally
        (remove-watch global key)
        (sdk/shutdown! replacement)
        (sdk/shutdown! previous)
        (is (empty? (:active (authority))))))))
