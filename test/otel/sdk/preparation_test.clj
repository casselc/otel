(ns otel.sdk.preparation-test
  (:require [clojure.test :refer [deftest is]]
            [otel.context :as ctx]
            [otel.sdk.export :as export]
            [otel.sdk.lifecycle :as lifecycle]
            [otel.sdk.preparation :as preparation]))

(defn- settled-deref [completion]
  (let [interrupted? (atom false)]
    (try
      (loop []
        (let [outcome (try {:value @completion}
                           (catch InterruptedException _ {:interrupted? true}))]
          (if (:interrupted? outcome)
            (do (reset! interrupted? true) (recur))
            (:value outcome))))
      (finally (when @interrupted? (.interrupt (Thread/currentThread)))))))

(defn- fixture-adapter [options]
  (let [events (atom []) ordinal (atom 0) live (atom #{}) maximum (atom 0)
        fenced? (atom false) gate (Object.)
        event! (fn [op id] (swap! events conj [op id]))
        adapter
        {:reserve! (fn [n]
                     (let [id (swap! ordinal inc)]
                       (swap! live conj id)
                       (swap! maximum max (count @live))
                       (event! :reserve id)
                       (atom {:id id :count n :phase :reserved})))
         :prepare! (fn [cap _]
                     (let [id (:id @cap)]
                       (is (ctx/instrumentation-suppressed?))
                       (event! :prepare id)
                       (when-let [entered (get options (keyword (str "prepared-" id)))]
                         (deliver entered true))
                       (when (= id (:fail-prepare options))
                         (throw (ex-info "controlled preparation failure" {})))
                       (swap! cap assoc :phase :prepared)
                       true))
         :submit! (fn [cap]
                    (locking gate
                      (when @fenced? (throw (ex-info "publication fenced" {})))
                      (event! :submit (:id @cap))
                      (swap! cap assoc :phase :submitted)
                      true))
         :await! (fn [cap]
                   (let [id (:id @cap)]
                     (event! :await id)
                     (when-let [entered (get options (keyword (str "awaiting-" id)))]
                       (deliver entered true))
                     (when-let [release (get options (keyword (str "release-" id)))]
                       (settled-deref release))
                     (swap! cap assoc :phase :settled)
                     (event! :settled id)
                     (when (= id (:fail-await options))
                       (throw (ex-info "controlled settled failure" {})))
                     (if (contains? options :await-result) (:await-result options) true)))
         :abort! (fn [cap] (event! :abort (:id @cap))
                   (swap! cap assoc :phase :aborted) true)
         :release! (fn [cap]
                     (when (= (:id @cap) (:fail-release options))
                       (throw (ex-info "controlled release failure" {})))
                     (event! :release (:id @cap)) true)
         :retire! (fn [cap] (event! :retire (:id @cap))
                    (swap! live disj (:id @cap)) true)
         :fence! (fn [] (locking gate
                         (reset! fenced? true) (event! :fence 0)
                         (when-let [p (:fence-entered options)] (deliver p true))
                         true))}]
    {:adapter adapter :events events :live live :maximum maximum :fenced? fenced?}))

(defn- exporter [closed]
  (reify export/SpanExporter
    (export-spans! [_ _] (throw (ex-info "synchronous path was not selected" {})))
    (flush-exporter! [_] true)
    (shutdown-exporter! [_] (swap! closed inc) true)))

(def sampled {:span-context {:trace-flags 1}})

(deftest actual-sdk-prepares-lookahead-without-counting-admission-as-delivery
  (let [prepared-b (promise) awaiting-a (promise) release-a (promise)
        {:keys [adapter events live maximum]}
        (fixture-adapter {:prepared-2 prepared-b :awaiting-1 awaiting-a :release-1 release-a})
        closed (atom 0)
        processor (export/batch-processor (exporter closed)
                    {:max-export-batch-size 1 :max-queue-size 8 :schedule-delay-ms 60000
                     :experimental-span-preparation adapter})]
    (try
      (dotimes [_ 3] (export/on-end processor sampled))
      (is (= true (deref prepared-b 3000 ::timeout)))
      (is (= true (deref awaiting-a 3000 ::timeout)))
      (is (= 2 @maximum))
      (is (= #{1 2} @live))
      (is (= 1 (export/queue-size processor)))
      (is (= {:attempted-span-count 2 :exported-span-count 0 :failed-span-count 0}
             (select-keys (export/batch-processor-stats processor)
                          [:attempted-span-count :exported-span-count :failed-span-count])))
      (is (= [[:reserve 1] [:prepare 1] [:submit 1]
              [:reserve 2] [:prepare 2] [:await 1]] @events))
      (deliver release-a true)
      (is (true? (export/force-flush! processor)))
      (is (= 3 (:exported-span-count (export/batch-processor-stats processor))))
      (is (= 2 @maximum))
      (is (empty? @live))
      (finally
        (deliver release-a true)
        (is (true? (export/shutdown! processor)))))
    (is (= 1 @closed))))

(deftest failed-lookahead-still-settles-the-earlier-request-and-retires-both
  (let [{:keys [adapter live events]} (fixture-adapter {:fail-prepare 2})
        closed (atom 0)
        processor (export/batch-processor (exporter closed)
                    {:max-export-batch-size 1 :schedule-delay-ms 60000
                     :experimental-span-preparation adapter})]
    (try
      (dotimes [_ 3] (export/on-end processor sampled))
      (is (false? (export/force-flush! processor)))
      (is (= {:attempted-span-count 3 :exported-span-count 2 :failed-span-count 1}
             (select-keys (export/batch-processor-stats processor)
                          [:attempted-span-count :exported-span-count :failed-span-count])))
      (is (empty? @live))
      (is (= [[:submit 1] [:submit 3]] (filterv #(= :submit (first %)) @events)))
      (finally (is (false? (export/shutdown! processor)))))
    (is (= 1 @closed))))

(deftest invalid-adapter-is-rejected-before-starting-a-worker
  (let [starts (atom 0) closed (atom 0) start lifecycle/start-owned-worker!]
    (with-redefs [lifecycle/start-owned-worker!
                  (fn [& args] (swap! starts inc) (apply start args))]
      (doseq [adapter [true {} {:reserve! identity}]]
        (is (thrown? Exception
                     (export/batch-processor (exporter closed)
                       {:experimental-span-preparation adapter}))))
      (is (= 0 @starts))
      (is (= 0 @closed)))))

(deftest local-admission-and-execution-values-cannot-count-as-success
  (doseq [receipt [{:status :admitted} {:status :executed-local} :admitted]]
    (let [{:keys [adapter live]} (fixture-adapter {:await-result receipt})
          closed (atom 0)
          processor (export/batch-processor (exporter closed)
                      {:max-export-batch-size 8 :schedule-delay-ms 60000
                       :experimental-span-preparation adapter})]
      (try
        (export/on-end processor sampled)
        (is (false? (export/force-flush! processor)))
        (is (= 0 (:exported-span-count (export/batch-processor-stats processor))))
        (is (= 1 (:failed-span-count (export/batch-processor-stats processor))))
        (is (empty? @live))
        (finally (is (false? (export/shutdown! processor))))))))

(deftest cancellation-fences-new-submission-but-settles-admitted-publication
  (let [prepared-b (promise) awaiting-a (promise) release-a (promise)
        fenced (promise) outcome (promise)
        {:keys [adapter live events]}
        (fixture-adapter {:prepared-2 prepared-b :awaiting-1 awaiting-a
                          :release-1 release-a :fence-entered fenced})
        closed (atom 0)
        processor (export/batch-processor (exporter closed)
                    {:max-export-batch-size 1 :max-queue-size 8 :schedule-delay-ms 60000
                     :experimental-span-preparation adapter})
        shutdown (Thread. (fn [] (deliver outcome
                                    (try {:value (export/shutdown! processor)}
                                         (catch Throwable error {:error error})))))]
    (try
      (dotimes [_ 3] (export/on-end processor sampled))
      (is (= true (deref prepared-b 3000 ::timeout)))
      (is (= true (deref awaiting-a 3000 ::timeout)))
      (with-redefs [lifecycle/shutdown-policy
                    (fn [_] {:shutdown-cancel-grace-ms 0 :shutdown-join-bound-ms 2000})]
        (.start shutdown)
        (is (= true (deref fenced 3000 ::timeout)))
        (is (= ::pending (deref outcome 20 ::pending)))
        (is (= 0 @closed))
        (deliver release-a true)
        (.join shutdown 3000))
      (is (= {:value false} (deref outcome 100 ::timeout)))
      (is (= [[:submit 1]] (filterv #(= :submit (first %)) @events)))
      (is (= {:attempted-span-count 3 :exported-span-count 1 :failed-span-count 2}
             (select-keys (export/batch-processor-stats processor)
                          [:attempted-span-count :exported-span-count :failed-span-count])))
      (is (empty? @live))
      (is (= 1 @closed))
      (is (= :confirmed (:quiescence (lifecycle/settlement-status processor))))
      (finally
        (deliver release-a true)
        (when (.isAlive shutdown) (.join shutdown 3000))
        (try (export/shutdown! processor) (catch Throwable _ nil))))))

(deftest failed-retirement-does-not-authorize-exporter-close-or-cleanup-proof
  (let [{:keys [adapter live]} (fixture-adapter {:fail-release 1})
        closed (atom 0)
        processor (export/batch-processor (exporter closed)
                    {:max-export-batch-size 8 :schedule-delay-ms 60000
                     :experimental-span-preparation adapter})]
    (export/on-end processor sampled)
    (is (thrown? Exception (export/force-flush! processor)))
    (is (thrown? Exception (export/shutdown! processor)))
    (is (= 0 @closed))
    (is (seq @live))
    (is (= :unconfirmed (:quiescence (lifecycle/settlement-status processor))))))

(defn- take-one! [state _]
  (let [[old _] (swap-vals! state update :queue
                           #(if (seq %) (subvec % 1) %))]
    (if (seq (:queue old)) [(first (:queue old))] [])))

(defn- fail-queued! [state]
  (swap! state (fn [s] (let [n (count (:queue s))]
                        (-> s (assoc :queue [])
                            (update :attempted-span-count + n)
                            (update :failed-span-count + n))))))

(deftest sibling-cleanup-is-attempted-after-earlier-retirement-fails
  (let [state (atom {:queue [sampled sampled sampled]
                     :attempted-span-count 0 :exported-span-count 0 :failed-span-count 0})
        {:keys [adapter live events]} (fixture-adapter {:fail-release 1})]
    (is (thrown? Exception
                 (ctx/with-instrumentation-suppressed
                   (preparation/drain! adapter state 1 take-one! fail-queued!))))
    (is (= #{1} @live) "A is quarantined; B is independently retired")
    (is (some #{[:abort 2]} @events))
    (is (some #{[:release 2]} @events))
    (is (some #{[:retire 2]} @events))
    (is (true? (:preparation-unsettled? @state)))))

(deftest cancellation-accounts-for-queued-spans-even-if-pending-retirement-fails
  (let [state (atom {:queue [sampled sampled sampled]
                     :attempted-span-count 0 :exported-span-count 0 :failed-span-count 0})
        {:keys [adapter live]} (fixture-adapter {:fail-release 1})
        submit (:submit! adapter)
        adapter (assoc adapter :submit! (fn [cap]
                                           (submit cap)
                                           ((:fence! adapter))
                                           (swap! state assoc :shutdown-cancelled? true)
                                           true))]
    (is (thrown? Exception
                 (ctx/with-instrumentation-suppressed
                   (preparation/drain! adapter state 1 take-one! fail-queued!))))
    (is (= [] (:queue @state)))
    (is (= {:attempted-span-count 3 :exported-span-count 1 :failed-span-count 2}
           (select-keys @state [:attempted-span-count :exported-span-count :failed-span-count])))
    (is (= #{1} @live))))

(deftest accepted-late-span-is-drained-by-the-worker-before-it-exits
  (let [empty-drain (promise) release-drain (promise) wait-entered (promise)
        outcome (promise) first-drain? (atom true) phase (atom nil) phases (atom [])
        {:keys [adapter live]} (fixture-adapter {})
        prepare (:prepare! adapter)
        adapter (assoc adapter :prepare! (fn [& args]
                                           (swap! phases conj @phase)
                                           (apply prepare args)))
        closed (atom 0)
        drain-var (ns-resolve 'otel.sdk.export 'drain!) drain @drain-var
        sleep-var (ns-resolve 'otel.sdk.export 'sleep-until-due)
        await-worker lifecycle/await-owned-worker!]
    (with-redefs-fn
      {sleep-var (fn [& _] nil)
       drain-var (fn [exporter state size worker? config]
                   (reset! phase (if worker? :worker :caller))
                   (let [value (drain exporter state size worker? config)]
                     (when (and worker? (compare-and-set! first-drain? true false))
                       (deliver empty-drain true)
                       @release-drain)
                     value))
       #'lifecycle/await-owned-worker! (fn [& args]
                                        (deliver wait-entered true)
                                        (apply await-worker args))}
      (fn []
        (let [owner (export/batch-processor (exporter closed)
                      {:max-export-batch-size 1 :schedule-delay-ms 60000
                       :experimental-span-preparation adapter})
              stopping (Thread. #(deliver outcome
                                   (try {:value (export/shutdown! owner)}
                                        (catch Throwable error {:error error}))))]
          (try
            (is (= true (deref empty-drain 3000 ::timeout)))
            (export/on-end owner sampled)
            (.start stopping)
            (is (= true (deref wait-entered 3000 ::timeout)))
            (deliver release-drain true)
            (.join stopping 3000)
            (is (= {:value true} (deref outcome 100 ::timeout)))
            (is (= [:worker] @phases)
                "late accepted work must not move to the shutdown caller")
            (is (= 1 (:exported-span-count (export/batch-processor-stats owner))))
            (is (empty? @live))
            (is (= 1 @closed))
            (finally
              (deliver release-drain true)
              (when (.isAlive stopping) (.join stopping 3000))
              (try (export/shutdown! owner) (catch Throwable _ nil)))))))))
