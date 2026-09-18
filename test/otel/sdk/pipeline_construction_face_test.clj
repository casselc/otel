(ns otel.sdk.pipeline-construction-face-test
  (:require [clojure.test :refer [deftest is]]
            [otel.sdk.export :as export]
            [otel.sdk.lifecycle :as lifecycle]
            [otel.sdk.logs :as logs]))

(defn- resource [counts settled result]
  (reify export/SpanExporter
    (export-spans! [_ _] true) (flush-exporter! [_] true)
    (shutdown-exporter! [_]
      (swap! counts update :spans inc)
      (if (= :throw result) (throw (ex-info "controlled release failure" {})) result))
    export/MetricExporter
    (export-metrics! [_ _ _] true)
    (shutdown-metric-exporter! [_] (swap! counts update :metrics inc) result)
    logs/LogRecordExporter
    (export-logs! [_ _] true)
    (shutdown-log-exporter! [_] (swap! counts update :logs inc) result)
    lifecycle/SettlementWitness
    (settlement-status [_] {:quiescence (if @settled :confirmed :unconfirmed)})))

(defn- counts [] (atom {:spans 0 :metrics 0 :logs 0}))
(defn- failure [destinations receipt]
  (try (export/independent-batch-pipelines destinations receipt) nil
       (catch Throwable error error)))
(defn- face [receipt error exporter signal]
  (export/construction-face-status receipt error exporter signal))
(defn- cleanup! [owners]
  (doseq [owner @owners]
    (try (export/shutdown! owner) (catch Throwable _ nil))
    (.join (:worker owner) 2000)
    (is (not (.isAlive (:worker owner))))))
(defn- observe-start [owners fail-at sentinel]
  (let [original lifecycle/start-owned-worker!]
    (fn [receipt owner worker]
      (swap! owners conj owner)
      (let [result (original receipt owner worker)]
        (when (= fail-at (count @owners)) (throw sentinel))
        result))))

(deftest explicit-validation-ledger-is-span-only
  (let [receipt (lifecycle/construction-receipt) c (counts)
        exporter (resource c (atom false) true)
        error (failure {:local {:exporter exporter :config {:max-queue-size 0}}}
                       receipt)]
    (is (some? error))
    (is (= {:spans 0 :metrics 0 :logs 0} @c))
    (is (= {:otel.sdk.construction/version 1 :ownership :unacquired
            :quiescence :confirmed} (face receipt error exporter :spans)))
    (doseq [signal [:metrics :logs nil :invalid]]
      (is (= :unknown (:ownership (face receipt error exporter signal)))))
    (is (= :unknown (:ownership (face receipt (ex-info "other" {}) exporter :spans))))
    (is (= :unknown (:ownership (face receipt error (resource (counts) (atom false) true)
                                     :spans))))
    (is (= :unknown (:ownership (face {} error exporter :spans))))
    (let [malformed (lifecycle/construction-receipt)
          shape-error (failure {:local nil} malformed)]
      (is (= :unknown (:ownership (face malformed shape-error exporter :spans)))))
    (let [reused (failure {:local {:exporter exporter}} receipt)]
      (is (= :unknown (:ownership (face receipt reused exporter :spans)))))
    (let [successful (lifecycle/construction-receipt)
          handle (export/independent-batch-pipelines {} successful)]
      (try
        (is (= :unknown (:ownership (face successful (ex-info "outside wrapper" {})
                                         exporter :spans))))
        (finally (is (true? (export/shutdown! handle))))))))

(deftest partial-maintained-workers-transfer-exact-faces
  (let [receipt (lifecycle/construction-receipt) owners (atom [])
        a (counts) b (counts) c (counts)
        ea (resource a (atom false) true) eb (resource b (atom false) true)
        ec (resource c (atom false) true) sentinel (ex-info "second actual start" {})
        count-oracle #(= [1 1 0] (mapv (fn [counter] (get @counter :spans)) [a b c]))]
    (try
      (let [error (with-redefs [lifecycle/start-owned-worker!
                               (observe-start owners 2 sentinel)]
                    (failure (array-map :a {:exporter ea} :b {:exporter eb}
                                        :c {:exporter ec}) receipt))]
        (is (identical? sentinel error))
        (is (= 2 (count @owners)))
        (is (count-oracle))
        (doseq [exporter [ea eb]]
          (is (= :sdk-owned (:ownership (face receipt error exporter :spans))))
          (is (= :confirmed (:quiescence (face receipt error exporter :spans)))))
        (is (= :unacquired (:ownership (face receipt error ec :spans))))
        (is (= :unknown (:ownership (face receipt error ea :logs))))
        (lifecycle/retire-construction! receipt)
        (is (count-oracle))
        ;; Hypothetical consumer extra-release control, SAME count predicate.
        (export/shutdown-exporter! ea)
        (is (not (count-oracle)))
        (is (= [2 1 0] (mapv #(get @% :spans) [a b c]))))
      (finally (cleanup! owners)))))

(deftest aliases-retain-every-child-and-baseline-callback-count
  (let [receipt (lifecycle/construction-receipt) owners (atom []) c (counts)
        exporter (resource c (atom false) true) sentinel (ex-info "alias start" {})]
    (try
      (let [error (with-redefs [lifecycle/start-owned-worker!
                               (observe-start owners 2 sentinel)]
                    (failure (array-map :a {:exporter exporter} :b {:exporter exporter}
                                        :c {:exporter exporter}) receipt))]
        (is (identical? error sentinel))
        (is (= 2 (count @owners)))
        (is (= 2 (:spans @c)))
        (is (= :sdk-owned (:ownership (face receipt error exporter :spans))))
        (is (= :confirmed (:quiescence (face receipt error exporter :spans))))
        (lifecycle/retire-construction! receipt)
        (is (= {:spans 2 :metrics 0 :logs 0} @c)))
      (finally (cleanup! owners)))))

(deftest foreign-factory-hidden-users-poison-even-successful-returns
  (doseq [outcome [:throw :nil :resource]]
    (let [receipt (lifecycle/construction-receipt) c (counts) d (counts)
          exporter (resource c (atom false) true) untouched (resource d (atom false) true)
          entered (promise) release (promise) users (atom 0)
          sentinel (ex-info "foreign pipeline factory" {})
          worker (Thread. (fn []
                            (swap! users inc)
                            (try (deliver entered true) @release
                                 (export/export-spans! exporter [])
                                 (finally (swap! users dec)))))
          calls (atom 0)]
      (try
        (let [error (with-redefs [export/batch-processor
                                 (fn [& _]
                                   (if (= 1 (swap! calls inc))
                                     (do (.start worker)
                                         (when-not (= true (deref entered 2000 ::timeout))
                                           (throw (ex-info "foreign user missing" {})))
                                         (case outcome :throw (throw sentinel)
                                               :nil nil :resource exporter))
                                     (throw sentinel)))]
                      (failure (array-map :a {:exporter exporter}
                                          :b {:exporter untouched}) receipt))]
          (is (= true (deref entered 2000 ::timeout)))
          (is (.isAlive worker))
          (is (= 1 @users))
          (is (= :construction-cleanup-incomplete (:otel.sdk/error (ex-data error))))
          (doseq [input [exporter untouched]]
            (is (= :unknown (:ownership (face receipt error input :spans)))))
          (is (= {:spans 0 :metrics 0 :logs 0} @c))
          (is (= {:spans 0 :metrics 0 :logs 0} @d))
          (deliver release true)
          (.join worker 2000)
          (is (not (.isAlive worker)))
          (is (= 0 @users))
          (lifecycle/retire-construction! receipt)
          (is (= :unknown (:ownership (face receipt error untouched :spans)))))
        (finally (deliver release true) (.join worker 2000)
                 (is (not (.isAlive worker))))))))

(deftest factory-check-and-use-share-the-captured-function
  (let [receipt (lifecycle/construction-receipt) owners (atom []) c (counts)
        exporter (resource c (atom false) true) sentinel (ex-info "actual start" {})
        factory-var #'export/batch-processor
        gate-var (ns-resolve 'otel.sdk.export 'enter-pipeline-factory!)
        original-gate @gate-var original-factory @factory-var foreign-calls (atom 0)]
    (try
      (let [error (with-redefs-fn
                    {gate-var (fn [& args]
                                (apply original-gate args)
                                (alter-var-root factory-var
                                                (constantly (fn [& _]
                                                              (swap! foreign-calls inc)
                                                              (throw sentinel)))))
                     #'lifecycle/start-owned-worker! (observe-start owners 1 sentinel)}
                    #(failure {:local {:exporter exporter}} receipt))]
        (is (identical? sentinel error))
        (is (= 0 @foreign-calls))
        (is (= 1 (count @owners)))
        (is (= 1 (:spans @c)))
        (is (= :sdk-owned (:ownership (face receipt error exporter :spans))))
        (is (= :confirmed (:quiescence (face receipt error exporter :spans)))))
      (finally (alter-var-root factory-var (constantly original-factory))
               (cleanup! owners)))))

(deftest fresh-truthful-proof-does-not-replay-failed-release
  (doseq [outcome [false :throw]]
  (let [receipt (lifecycle/construction-receipt) owners (atom []) c (counts)
        settled (atom false) exporter (resource c settled outcome)
        untouched-counts (counts) untouched (resource untouched-counts (atom false) true)
        sentinel (ex-info "actual failed start" {})]
    (try
      (let [error (with-redefs [lifecycle/start-owned-worker!
                               (observe-start owners 1 sentinel)]
                    (failure (array-map :local {:exporter exporter}
                                        :later {:exporter untouched}) receipt))]
        (is (= :construction-cleanup-incomplete (:otel.sdk/error (ex-data error))))
        (is (= :sdk-owned (:ownership (face receipt error exporter :spans))))
        (is (= :unconfirmed (:quiescence (face receipt error exporter :spans))))
        (is (= 1 (:spans @c)))
        (is (= :unknown (:ownership (face receipt error untouched :spans))))
        (is (= {:spans 0 :metrics 0 :logs 0} @untouched-counts))
        (doseq [owner @owners]
          (.join (:worker owner) 2000)
          (is (not (.isAlive (:worker owner)))))
        ;; Independent controlled exporter retirement, ONLY after actual join.
        ;; This flag is a truthful fixture contract, not a native-resource proof.
        (reset! settled true)
        (is (= :confirmed (:quiescence (face receipt error exporter :spans))))
        (is (= :unacquired (:ownership (face receipt error untouched :spans))))
        (is (= {:spans 0 :metrics 0 :logs 0} @untouched-counts))
        (lifecycle/retire-construction! receipt)
        (is (= 1 (:spans @c)))
        (is (= :unknown (:ownership (face receipt sentinel exporter :metrics)))))
      (finally (cleanup! owners))))))
