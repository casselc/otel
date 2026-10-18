(ns otel.sdk.batch-drain-boundary-test
  (:require [clojure.test :refer [deftest is]]
            [otel.sdk.export :as export]))

(defn- state [queue]
  (atom {:queue queue :shutdown? false :shutdown-cancelled? false
         :attempted-span-count 0 :exported-span-count 0 :failed-span-count 0}))

(defn- recording-exporter [f]
  (reify export/SpanExporter
    (export-spans! [_ spans] (f spans))
    (flush-exporter! [_] true)
    (shutdown-exporter! [_] true)))

(deftest a-live-worker-yields-after-one-batch-including-arrivals-during-export
  (let [s (state [:first :second]) calls (atom [])
        owner (recording-exporter
                (fn [batch]
                  (swap! calls conj (vec batch))
                  (when (= 1 (count @calls)) (swap! s update :queue conj :later))
                  true))]
    (is (true? (#'export/drain! owner s 2 true)))
    (is (= [[:first :second]] @calls))
    (is (= [:later] (:queue @s)))
    (is (= 2 (:attempted-span-count @s) (:exported-span-count @s)))))

(deftest caller-and-retiring-worker-drains-remain-complete
  (doseq [[worker-owned? retiring?] [[false false] [false true] [true true]]]
    (let [s (state [:a :b :c :d :e]) calls (atom [])
          _ (swap! s assoc :shutdown? retiring?)
          owner (recording-exporter #(do (swap! calls conj (vec %)) true))]
      (is (true? (#'export/drain! owner s 2 worker-owned?)))
      (is (= [[:a :b] [:c :d] [:e]] @calls))
      (is (empty? (:queue @s)))
      (is (= 5 (:attempted-span-count @s) (:exported-span-count @s))))))

(deftest retirement-and-cancellation-observed-during-export
  (doseq [cancelled? [false true]]
    (let [s (state [:a :b :c]) calls (atom [])
          owner (recording-exporter
                  (fn [batch]
                    (swap! calls conj (vec batch))
                    (swap! s assoc :shutdown? true :shutdown-cancelled? cancelled?)
                    true))]
      (is (= (not cancelled?) (#'export/drain! owner s 2 true)))
      (is (= (if cancelled? [[:a :b]] [[:a :b] [:c]]) @calls))
      (is (empty? (:queue @s)))
      (is (= 3 (:attempted-span-count @s)))
      (is (= (if cancelled? 1 0) (:failed-span-count @s))))))

(deftest actual-worker-rechecks-scheduling-before-an-underfilled-followup
  (let [kick (promise) entered (promise) release (promise)
        rechecked (promise) continue (promise) second-export (promise)
        calls (atom 0) waits (atom 0)
        wait-var (ns-resolve 'otel.sdk.export 'sleep-until-due) original @wait-var
        sampled {:span-context {:trace-flags 1}}
        owner (recording-exporter
                (fn [_]
                  (if (= 1 (swap! calls inc))
                    (do (deliver entered true) @release)
                    (deliver second-export true))
                  true))]
    (with-redefs-fn
      {wait-var (fn [& args]
                  (case (swap! waits inc)
                    1 @kick
                    2 (do (deliver rechecked true) @continue)
                    (apply original args)))}
      (fn []
        (let [processor (export/batch-processor owner
                          {:max-queue-size 8 :max-export-batch-size 2
                           :schedule-delay-ms 60000})]
          (try
            (export/on-end processor sampled)
            (export/on-end processor sampled)
            (deliver kick true)
            (is (= true (deref entered 1000 ::timeout)))
            (export/on-end processor sampled)
            (deliver release true)
            (is (= true (deref rechecked 1000 ::timeout)))
            (is (not (realized? second-export))
                "underfilled followup waits for readiness/deadline, not a drain hot loop")
            (is (true? (export/force-flush! processor))
                "explicit flush still drains while the scheduler is waiting")
            (is (= 3 (:exported-span-count (export/batch-processor-stats processor))))
            (finally
              (deliver kick true) (deliver release true) (deliver continue true)
              (is (true? (export/shutdown! processor))))))))))
