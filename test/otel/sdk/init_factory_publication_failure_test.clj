(ns otel.sdk.init-factory-publication-failure-test
  (:require [clojure.test :refer [deftest is]]
            [otel.sdk :as sdk]
            [otel.sdk.export :as export]
            [otel.sdk.lifecycle :as lifecycle]
            [otel.sdk.tracer :as tracer]))

(deftest unreturned-factory-ownership-remains-unknown-after-known-workers-retire
  (doseq [factory-kind [:exporter :provider]]
    (let [receipt (lifecycle/construction-receipt) owner (atom nil)
          started (promise) finish (promise)
          user (Thread. #(do (deliver started true) @finish))
          retired? (atom false) closes (atom 0)
          failure (ex-info "controlled unreturned factory failure" {})
          exporter (reify export/SpanExporter
                     (export-spans! [_ _] true) (flush-exporter! [_] true)
                     (shutdown-exporter! [_] (swap! closes inc) (reset! retired? true) false)
                     lifecycle/SettlementWitness
                     (settlement-status [_]
                       {:quiescence (if (and @retired? (not (.isAlive user)))
                                      :confirmed :unconfirmed)}))
          batch-var (ns-resolve 'otel.sdk.export 'batch-processor)
          batch @batch-var
          factory-var (if (= :exporter factory-kind)
                        (ns-resolve 'otel.sdk 'build-span-exporter)
                        (ns-resolve 'otel.sdk.tracer 'tracer-provider))]
      (.setDaemon user true) (.start user)
      (try
        (is (nil? (sdk/tracer-provider)))
        (is (= true (deref started 2000 :timeout)))
        (let [error (with-redefs-fn
                      {batch-var (fn [& args] (let [acquired (apply batch args)]
                                               (reset! owner acquired) acquired))
                       factory-var (fn [& _] (throw failure))}
                      #(try (sdk/init! {:exporter exporter :metrics? false
                                       :schedule-delay-ms 60000 :construction-receipt receipt})
                            nil (catch Throwable error error)))
              retry-stop! (:retry-stop! (ex-data error))]
          (is (not (identical? failure error)))
          (is (= #{:otel.sdk/error :retry-stop!} (set (keys (ex-data error)))))
          (is (= :construction-cleanup-incomplete (:otel.sdk/error (ex-data error))))
          (is (nil? (.getCause error)))
          (is (= :unconfirmed (:quiescence (lifecycle/construction-failure-status receipt error))))
          (is (= :closing (:status (retry-stop!))))
          (is (= (if (= :provider factory-kind) 1 0) @closes))
          (when @owner (is (not (.isAlive (:worker @owner)))))
          (deliver finish true) (.join user 2000)
          (is (not (.isAlive user)))
          (when @owner
            (is (= :confirmed (:quiescence (lifecycle/component-settlement @owner)))))
          ;; A retired known face cannot attest unreturned foreign factory owners.
          (is (= :unconfirmed (:quiescence (lifecycle/construction-failure-status receipt error))))
          (is (= :closing (:status (retry-stop!))))
          (is (= (if (= :provider factory-kind) 1 0) @closes)))
        (finally
          (deliver finish true) (.join user 2000)
          (when @owner
            (export/shutdown! @owner) (.join (:worker @owner) 2000)
            (is (not (.isAlive (:worker @owner)))))
          ;; Explicit fixture ownership is not SDK proof for the failed factory.
          (when (zero? @closes) (export/shutdown-exporter! exporter))
          (lifecycle/retire-construction! receipt)
          (is (not (.isAlive user)))
          (is (= 1 @closes)))))))

(defn- registry-safe? [actual expected]
  (and (identical? actual expected)
       (or (nil? actual) (false? @(:shutdown? actual)))))

(deftest postpublication-rollback-does-not-resurrect-a-retired-previous-provider
  ;; Same actual identity/admission oracle for all three histories. The retired
  ;; previous case is expected causal RED on the current production candidate.
  (doseq [history [:active-previous :previous-retired :later-replacement]]
    (is (nil? (sdk/tracer-provider)))
    (let [global-var (ns-resolve 'otel.sdk 'global) global @global-var
          before @global previous (sdk/init! {:exporter :none :metrics? false})
          receipt (lifecycle/construction-receipt) candidate (atom nil)
          replacement (atom nil) owners (atom []) providers (atom [])
          fired? (atom false) closes (atom 0)
          failure (ex-info "controlled post-commit publication failure" {})
          exporter (reify export/SpanExporter
                     (export-spans! [_ _] true) (flush-exporter! [_] true)
                     (shutdown-exporter! [_] (swap! closes inc) true))
          batch export/batch-processor make-provider tracer/tracer-provider
          watch-key ::publication-failure]
      (try
        (add-watch global watch-key
          (fn [_ _ _ published]
            (when (and (not (identical? (:tracer-provider previous) (:tracer-provider published)))
                       (compare-and-set! fired? false true))
              ;; Observe/inject only: assertions remain outside the callback.
              (case history
                :previous-retired (sdk/shutdown! previous)
                :later-replacement
                (reset! replacement (sdk/init! {:exporter :none :metrics? false}))
                nil)
              (throw failure))))
        (let [error
              (with-redefs [export/batch-processor
                            (fn [& args] (let [owner (apply batch args)]
                                          (swap! owners conj owner) owner))
                            tracer/tracer-provider
                            (fn [& args] (let [provider (apply make-provider args)]
                                          (swap! providers conj provider) provider))]
                (try (reset! candidate (sdk/init! {:exporter exporter :metrics? false
                                                  :schedule-delay-ms 60000
                                                  :construction-receipt receipt}))
                     nil (catch Throwable error error)))
              actual (sdk/tracer-provider)
              expected (case history
                         :active-previous (:tracer-provider previous)
                         :later-replacement (:tracer-provider @replacement)
                         :previous-retired nil)]
          (is @fired?)
          (is (identical? failure error))
          (is (nil? @candidate))
          (is (= :confirmed (:quiescence (lifecycle/construction-failure-status receipt error))))
          (is (= 1 @closes))
          (is (= (= :previous-retired history) @(:shutdown? (:tracer-provider previous))))
          (is (every? #(not (.isAlive (:worker %))) @owners))
          (prn {:phase :sdk43-global-rollback :history history
                :actual-is-previous? (identical? actual (:tracer-provider previous))
                :actual-is-replacement? (and (some? @replacement)
                                            (identical? actual (:tracer-provider @replacement)))
                :actual-admission-retired? (when actual @(:shutdown? actual))
                :registry-safe? (registry-safe? actual expected)})
          (is (registry-safe? actual expected)))
        (finally
          (remove-watch global watch-key)
          (when @candidate (sdk/shutdown! @candidate))
          (when @replacement (sdk/shutdown! @replacement))
          (sdk/shutdown! previous)
          (doseq [provider @providers] (tracer/shutdown! provider))
          (doseq [owner @owners]
            (export/shutdown! owner) (.join (:worker owner) 2000)
            (is (not (.isAlive (:worker owner)))))
          ;; Restore isolated fixture state only after all acquired real owners
          ;; retire. The causal observation/assertion precedes this teardown.
          (reset! global before)
          (is (nil? (sdk/tracer-provider))))))))
