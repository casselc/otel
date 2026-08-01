(ns otel.sdk-test
  (:require [clojure.test :refer [deftest is testing]]
            [otel.metrics :as metrics]
            [otel.resource :as res]
            [otel.sdk :as sdk]
            [otel.sdk.metrics :as sdk-metrics]
            [otel.trace :as trace]))

(defn- with-sdk [opts f]
  (let [handle (sdk/init! (merge {:exporter :none} opts))]
    (try (f handle)
         (finally (sdk/shutdown! handle)))))

(deftest init-installs-a-global-tracer
  (with-sdk {:service-name "checkout"}
    (fn [_]
      (is (some? (sdk/tracer-provider)))
      (let [t (sdk/tracer "my.lib")]
        (trace/with-span [sp t "op"]
          (is (trace/recording? sp))
          (is (trace/valid? (trace/span-context-of sp))))))))

(deftest service-name-reaches-the-resource
  (with-sdk {:service-name "checkout"}
    (fn [handle]
      (is (= "checkout"
             (get (res/attributes (:resource (:tracer-provider handle))) "service.name"))))))

(deftest the-detected-resource-is-still-present
  (with-sdk {:service-name "checkout"}
    (fn [handle]
      (let [a (res/attributes (:resource (:tracer-provider handle)))]
        (is (= "opentelemetry" (get a "telemetry.sdk.name")))
        (is (= "Chez Scheme" (get a "process.runtime.name")))))))

(deftest a-meter-is-installed-and-carries-runtime-instruments
  (with-sdk {}
    (fn [handle]
      (is (some? (sdk/meter-provider)))
      (let [names (->> (sdk-metrics/collect! (:meter-provider handle))
                       (mapcat :metrics)
                       (map :name)
                       set)]
        (is (contains? names "process.runtime.jolt.memory.heap"))
        (is (contains? names "process.runtime.jolt.gc.count"))))))

(deftest runtime-metrics-can-be-turned-off
  (with-sdk {:runtime-metrics? false}
    (fn [handle]
      (is (empty? (mapcat :metrics (sdk-metrics/collect! (:meter-provider handle))))))))

(deftest metrics-can-be-turned-off-entirely
  (with-sdk {:metrics? false}
    (fn [handle]
      (is (nil? (:meter-provider handle)))
      (testing "asking for a meter still yields a working no-op"
        (is (some? (metrics/add! (metrics/counter (sdk/meter "x") "c") 1)))))))

(deftest without-an-sdk-the-api-is-a-noop
  (testing "instrumentation written against sdk/tracer must work before init!"
    (sdk/shutdown! {})
    (let [t (sdk/tracer "my.lib")]
      (trace/with-span [sp t "op"]
        (is (not (trace/recording? sp)))))))

(deftest shutdown-clears-the-global-registry
  (let [handle (sdk/init! {:exporter :none})]
    (is (some? (sdk/tracer-provider)))
    (sdk/shutdown! handle)
    (is (nil? (sdk/tracer-provider)))
    (is (nil? (sdk/meter-provider)))))

(deftest console-exporter-wiring
  (let [handle (sdk/init! {:exporter :console :processor :simple :metrics? false})]
    (try
      (trace/with-span [sp (sdk/tracer "s") "printed-span"])
      (is (sdk/force-flush! handle))
      (finally (sdk/shutdown! handle)))))
