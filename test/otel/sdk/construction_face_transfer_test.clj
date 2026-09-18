(ns otel.sdk.construction-face-transfer-test
  (:require [clojure.test :refer [deftest is]]
            [otel.instrument.runtime :as runtime]
            [otel.sdk :as sdk]
            [otel.sdk.export :as export]
            [otel.sdk.lifecycle :as lifecycle]
            [otel.sdk.logs :as logs]
            [otel.sdk.metrics :as metrics]))

(defn- exporter [counts settled release-value]
  (reify export/SpanExporter
    (export-spans! [_ _] true) (flush-exporter! [_] true)
    (shutdown-exporter! [_] (swap! counts update :spans inc) release-value)
    export/MetricExporter
    (export-metrics! [_ _ _] true)
    (shutdown-metric-exporter! [_] (swap! counts update :metrics inc) release-value)
    logs/LogRecordExporter
    (export-logs! [_ _] true)
    (shutdown-log-exporter! [_] (swap! counts update :logs inc) release-value)
    lifecycle/SettlementWitness
    (settlement-status [_] {:quiescence (if @settled :confirmed :unconfirmed)})))

(defn- failure [options]
  (try (sdk/init! options) nil (catch Throwable error error)))

(defn- face [receipt error resource signal]
  (sdk/construction-face-status receipt error resource signal))

(defn- observe-reader [owners]
  (let [original metrics/periodic-reader]
    (fn [& args]
      (let [owner (apply original args)] (swap! owners conj owner) owner))))

(defn- cleanup! [owners]
  (doseq [owner @owners]
    (try (export/shutdown! owner) (catch Throwable _ nil))
    (.join (:worker owner) 2000)
    (is (not (.isAlive (:worker owner))))))

(deftest acquired-faces-bind-each-signal-and-exclude-consumer-replay
  (let [receipt (lifecycle/construction-receipt) owners (atom [])
        counts (atom {:spans 0 :metrics 0 :logs 0})
        resource (exporter counts (atom false) true)
        sentinel (ex-info "controlled registration failure" {})]
    (try
      (let [error (with-redefs [runtime/register! (fn [_] (throw sentinel))
                               metrics/periodic-reader (observe-reader owners)]
                    (failure {:exporter resource :processor :simple :logs? true
                              :bridge-logging? false :construction-receipt receipt}))]
        (is (identical? error sentinel))
        (is (= {:spans 1 :metrics 1 :logs 1} @counts))
        (doseq [signal [:spans :metrics :logs]]
          (is (= {:otel.sdk.construction/version 1 :ownership :sdk-owned
                  :quiescence :confirmed} (face receipt error resource signal))))
        (lifecycle/retire-construction! receipt)
        (is (= {:spans 1 :metrics 1 :logs 1} @counts))
        ;; Hypothetical consumer bug: real additional release, same count oracle.
        ;; This is not a claimed bug in current SDK or Oscope implementation.
        (export/shutdown-exporter! resource)
        (is (not= {:spans 1 :metrics 1 :logs 1} @counts))
        (is (= {:spans 2 :metrics 1 :logs 1} @counts)))
      (finally (cleanup! owners)))))

(deftest explicit-unacquired-is-not-absence-of-child-claim
  (let [receipt (lifecycle/construction-receipt)
        counts (atom {:spans 0 :metrics 0 :logs 0})
        resource (exporter counts (atom false) true)
        error (failure {:exporter resource :max-queue-size 0 :logs? true
                        :construction-receipt receipt})]
    (is (some? error))
    (is (= {:spans 1 :metrics 0 :logs 0} @counts))
    (is (= :sdk-owned (:ownership (face receipt error resource :spans))))
    (doseq [signal [:metrics :logs]]
      (is (= :unacquired (:ownership (face receipt error resource signal))))
      (is (= :confirmed (:quiescence (face receipt error resource signal)))))
    (is (= :unknown (:ownership
                     (lifecycle/construction-resource-status receipt error resource :logs))))
    (doseq [signal [nil :invalid "logs"]]
      (is (= :unknown (:ownership (face receipt error resource signal)))))
    (is (= :unknown (:ownership
                     (face receipt error (exporter (atom {}) (atom false) true) :logs))))))

(deftest foreign-factory-and-wrapper-evidence-stays-unknown
  (let [receipt (lifecycle/construction-receipt)
        counts (atom {:spans 0 :metrics 0 :logs 0})
        resource (exporter counts (atom false) true)
        sentinel (ex-info "unreturned foreign metric factory" {})
        factory (ns-resolve 'otel.sdk 'build-metric-exporter)
        entered (promise) release (promise) users (atom 0)
        worker (Thread. (fn []
                          (swap! users inc)
                          (try
                            (deliver entered true)
                            @release
                            (export/export-metrics! resource {} [])
                            (finally (swap! users dec))))) ]
    (try
      (let [error (with-redefs-fn
                    {factory (fn [& _]
                               (.start worker)
                               (when-not (= true (deref entered 2000 ::timeout))
                                 (throw (ex-info "foreign user did not acknowledge" {})))
                               (throw sentinel))}
                    #(failure {:exporter resource :processor :simple :logs? true
                               :construction-receipt receipt}))]
        (is (= true (deref entered 2000 ::timeout)))
        (is (.isAlive worker))
        (is (= 1 @users))
        (is (= :construction-cleanup-incomplete (:otel.sdk/error (ex-data error))))
        (is (= {:spans 1 :metrics 0 :logs 0} @counts))
        (is (= :unknown (:ownership (face receipt error resource :spans))))
        (doseq [signal [:metrics :logs]]
          (is (= :unknown (:ownership (face receipt error resource signal)))))
        (is (= :unknown (:ownership (face receipt sentinel resource :spans))))
        (doseq [other [nil (ex-info "replacement" {})]]
          (is (= :unknown (:ownership (face receipt other resource :spans)))))
        (is (= :unknown (:ownership (face nil error resource :spans))))
        (is (= :unknown (:ownership (face {} error resource :spans))))
        (let [reused (failure {:exporter resource :construction-receipt receipt})]
          (is (= :unknown (:ownership (face receipt reused resource :spans)))))
        (deliver release true)
        (.join worker 2000)
        (is (not (.isAlive worker)))
        (is (= 0 @users))
        (lifecycle/retire-construction! receipt)
        (is (= :unknown (:ownership (face receipt error resource :logs))))
        (is (= {:spans 1 :metrics 0 :logs 0} @counts)))
      (finally
        (deliver release true)
        (.join worker 2000)
        (is (not (.isAlive worker))))))
  (let [receipt (lifecycle/construction-receipt) counts (atom {:spans 0 :metrics 0 :logs 0})
        resource (exporter counts (atom false) true)
        handle (sdk/init! {:exporter resource :processor :simple :metrics? false
                          :runtime-metrics? false :construction-receipt receipt})]
    (try
      (is (= :unknown (:ownership (face receipt (ex-info "outside wrapper" {}) resource :spans))))
      (is (= :unknown (:ownership (face receipt nil resource :spans))))
      (finally (sdk/shutdown! handle))))
  ;; Successful foreign factories also stay unknown: returned ownership cannot
  ;; establish that the factory did not retain another face of the same object.
  (doseq [returns-resource? [true false]]
    (let [receipt (lifecycle/construction-receipt) owners (atom [])
          resource (exporter (atom {:spans 0 :metrics 0 :logs 0}) (atom false) true)
          factory (ns-resolve 'otel.sdk 'build-metric-exporter)
          sentinel (ex-info "controlled later registration failure" {})
          entered (promise) release (promise) users (atom 0)
          worker (Thread. (fn []
                            (swap! users inc)
                            (try
                              (deliver entered true)
                              @release
                              (logs/export-logs! resource [])
                              (finally (swap! users dec)))))]
      (try
        (let [error (with-redefs-fn
                      {factory (fn [& _]
                                 (.start worker)
                                 (when-not (= true (deref entered 2000 ::timeout))
                                   (throw (ex-info "foreign log user did not acknowledge" {})))
                                 (when returns-resource? resource))
                       #'runtime/register! (fn [_] (throw sentinel))
                       #'metrics/periodic-reader (observe-reader owners)}
                      #(failure {:exporter resource :processor :simple
                                 :construction-receipt receipt}))]
          (is (identical? error sentinel))
          (is (= :confirmed (:quiescence (lifecycle/construction-failure-status receipt error))))
          (is (= true (deref entered 2000 ::timeout)))
          (is (.isAlive worker))
          (is (= 1 @users))
          (doseq [signal [:spans :metrics :logs]]
            (is (= :unknown (:ownership (face receipt error resource signal)))))
          (deliver release true)
          (.join worker 2000)
          (is (not (.isAlive worker)))
          (is (= 0 @users)))
        (finally
          (deliver release true)
          (.join worker 2000)
          (is (not (.isAlive worker)))
          (cleanup! owners)))))
  ;; Deterministic check/use seam: replace the Var AFTER attestation. Actual
  ;; invocation must still use the identical lexically captured function.
  (let [receipt (lifecycle/construction-receipt) owners (atom [])
        counts (atom {:spans 0 :metrics 0 :logs 0})
        resource (exporter counts (atom false) true)
        factory (ns-resolve 'otel.sdk 'build-metric-exporter)
        entry (ns-resolve 'otel.sdk 'enter-exporter-factory!)
        original-factory @factory original-entry @entry
        attested (atom nil) foreign-calls (atom 0)
        sentinel (ex-info "controlled registration failure" {})]
    (try
      (let [error (with-redefs-fn
                    {factory original-factory
                     entry (fn [receipt resource signal selected]
                             (original-entry receipt resource signal selected)
                             (when (= :metrics signal)
                               (reset! attested selected)
                               (alter-var-root factory
                                 (constantly (fn [& _] (swap! foreign-calls inc) resource)))))
                     #'runtime/register! (fn [_] (throw sentinel))
                     #'metrics/periodic-reader (observe-reader owners)}
                    #(failure {:exporter resource :processor :simple
                               :construction-receipt receipt}))]
        (is (identical? error sentinel))
        (is (identical? original-factory @attested))
        (is (= 0 @foreign-calls))
        (is (= :sdk-owned (:ownership (face receipt error resource :metrics))))
        (is (= :unacquired (:ownership (face receipt error resource :logs))))
        (is (= {:spans 1 :metrics 1 :logs 0} @counts)))
      (finally (cleanup! owners)))))

(deftest supplied-processors-cannot-certify-skipped-exporter-faces
  (let [receipt (lifecycle/construction-receipt) pipeline-receipt (lifecycle/construction-receipt)
        counts (atom {:spans 0 :metrics 0 :logs 0}) resource (exporter counts (atom false) true)
        pipelines (export/independent-batch-pipelines
                    {:local {:exporter resource :config {:schedule-delay-ms 60000}}}
                    pipeline-receipt)
        owners (atom []) sentinel (ex-info "controlled registration failure" {})]
    (try
      (let [error (with-redefs [runtime/register! (fn [_] (throw sentinel))
                               metrics/periodic-reader (observe-reader owners)]
                    (failure {:exporter resource :span-processors [pipelines]
                              :construction-receipt receipt}))]
        (is (identical? error sentinel))
        (is (= :confirmed (:quiescence (lifecycle/construction-failure-status receipt error))))
        (is (= :unknown (:ownership (face receipt error resource :spans))))
        (is (= :unknown (:ownership (face receipt error resource :logs))))
        (is (= :sdk-owned (:ownership (face receipt error resource :metrics))))
        (is (= :unknown (:ownership
                             (lifecycle/construction-resource-status pipeline-receipt nil resource :spans))))
        (is (= :confirmed (:quiescence (lifecycle/component-settlement pipelines))))
        (is (= {:spans 1 :metrics 1 :logs 0} @counts)))
      (finally
        (cleanup! owners)
        (export/shutdown-pipelines! pipelines)
        (doseq [[_ processor] (:pipelines pipelines)]
          (.join (:worker processor) 2000)
          (is (not (.isAlive (:worker processor)))))))))

(deftest fresh-proof-does-not-replay-owned-face-callbacks
  (let [receipt (lifecycle/construction-receipt) owners (atom [])
        settled (atom false) counts (atom {:spans 0 :metrics 0 :logs 0})
        resource (exporter counts settled false) sentinel (ex-info "controlled failure" {})]
    (try
      (let [error (with-redefs [runtime/register! (fn [_] (throw sentinel))
                               metrics/periodic-reader (observe-reader owners)]
                    (failure {:exporter resource :processor :simple :logs? true
                              :bridge-logging? false :construction-receipt receipt}))]
        (is (= :construction-cleanup-incomplete (:otel.sdk/error (ex-data error))))
        (is (= {:spans 1 :metrics 1 :logs 1} @counts))
        (doseq [signal [:spans :metrics :logs]]
          (is (= :sdk-owned (:ownership (face receipt error resource signal))))
          (is (= :unconfirmed (:quiescence (face receipt error resource signal)))))
        (doseq [owner @owners]
          (.join (:worker owner) 2000)
          (is (not (.isAlive (:worker owner)))))
        ;; Controlled exporter-owned permanent retirement, after real worker exit.
        ;; This does not establish native exporter resource settlement.
        (reset! settled true)
        (is (= :closed (:status (lifecycle/retire-construction! receipt))))
        (doseq [signal [:spans :metrics :logs]]
          (is (= :confirmed (:quiescence (face receipt error resource signal)))))
        (is (= {:spans 1 :metrics 1 :logs 1} @counts)))
      (finally (cleanup! owners)))))
