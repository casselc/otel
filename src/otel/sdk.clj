(ns otel.sdk
  "One-call SDK setup, and the global handles instrumentation reaches for.

  Most applications want the same thing: read the standard OTEL_* environment
  variables, batch spans to a collector, poll the runtime metrics, and shut down
  cleanly. `init!` does exactly that and returns a handle to close.

      (require '[otel.sdk :as sdk] '[otel.trace :as trace])

      (def otel (sdk/init! {:service-name \"checkout\"}))

      (trace/with-span [sp (sdk/tracer \"checkout.http\") \"GET /cart\"]
        (trace/set-attribute! sp :http.route \"/cart\"))

      (sdk/shutdown! otel)

  Nothing here is required to use the library — a caller who wants explicit
  control builds `otel.sdk.tracer/tracer-provider` directly and never touches
  this namespace. What this adds is the global registry, which exists so a
  library can call `(sdk/tracer \"my.lib\")` at any point and get a working tracer
  without the application having to thread a provider into it."
  (:refer-clojure :exclude [name])
  (:require [clojure.string :as str]
            [otel.bridge.tools-logging :as tools-logging]
            [otel.exporter.otlp :as otlp]
            [otel.exporter.stdout :as stdout]
            [otel.instrument.runtime :as runtime]
            [otel.logs :as logs-api]
            [otel.metrics :as metrics-api]
            [otel.propagation :as propagation]
            [otel.resource :as res]
            [otel.sdk.export :as export]
            [otel.sdk.lifecycle :as lifecycle]
            [otel.sdk.logs :as sdk-logs]
            [otel.sdk.metrics :as sdk-metrics]
            [otel.sdk.sampler :as sampler]
            [otel.sdk.tracer :as sdk-tracer]
            [otel.trace :as trace]))

;; --- global registry --------------------------------------------------------

(defonce ^:private global (atom {:tracer-provider nil :meter-provider nil :logger-provider nil}))
;; One CAS state binds SDK registry ownership and installation lifetime. Global
;; remains an internal watched notification view; foreign provider retirement
;; bypassing SDK handles is not synchronized by this contract.
(defonce ^:private registry
  (atom {:providers {:tracer-provider nil :meter-provider nil :logger-provider nil}
         :owners {} :active #{} :revision 0}))

(defn tracer-provider
  "The installed tracer provider, or nil."
  []
  (:tracer-provider (:providers @registry)))

(defn meter-provider
  "The installed meter provider, or nil."
  []
  (:meter-provider (:providers @registry)))

(defn logger-provider
  "The installed logger provider, or nil."
  []
  (:logger-provider (:providers @registry)))

(defn tracer
  "A tracer for `scope-name` from the installed provider. Falls back to the API's
  no-op tracer when no SDK has been installed, so instrumentation is safe to
  write before — or without — any configuration."
  ([scope-name] (tracer scope-name {}))
  ([scope-name opts]
   (if-let [p (tracer-provider)]
     (sdk-tracer/get-tracer p (assoc opts :name scope-name))
     trace/noop-tracer)))

(defn meter
  "A meter for `scope-name` from the installed provider, or the no-op meter."
  ([scope-name] (meter scope-name {}))
  ([scope-name opts]
   (if-let [p (meter-provider)]
     (sdk-metrics/get-meter p (assoc opts :name scope-name))
     metrics-api/noop-meter)))

(defn logger
  "A logger for `scope-name` from the installed provider, or the no-op logger.
  Most code should not need this — configure `:logs? true` and keep using
  clojure.tools.logging, which the bridge routes here."
  ([scope-name] (logger scope-name {}))
  ([scope-name opts]
   (if-let [p (logger-provider)]
     (sdk-logs/get-logger p (assoc opts :name scope-name))
     logs-api/noop-logger)))

;; --- configuration ----------------------------------------------------------

(defn- env [k] (jolt.host/getenv k))

(defn- env-sampler []
  (sampler/from-config (env "OTEL_TRACES_SAMPLER") (env "OTEL_TRACES_SAMPLER_ARG")))

(defn- disabled? []
  (= "true" (some-> (env "OTEL_SDK_DISABLED") str/trim str/lower-case)))

(def ^:private exporter-kinds #{:otlp :console :json :none})

(defn- check-exporter
  "`:exporter` is a kind or an exporter instance. Anything else is a mistake --
  a typo used to fall through to OTLP, so telemetry silently went to
  localhost:4318 instead of where it was asked to go."
  [exporter]
  (when-not (or (contains? exporter-kinds exporter)
                (satisfies? export/SpanExporter exporter)
                (satisfies? export/MetricExporter exporter)
                (satisfies? sdk-logs/LogRecordExporter exporter))
    (throw (ex-info (str "unknown :exporter " (pr-str exporter)
                         " -- expected one of " (str/join ", " (sort (map str exporter-kinds)))
                         ", or an exporter instance")
                    {:exporter exporter :kinds exporter-kinds})))
  exporter)

(defn- check-span-processors
  [processors]
  (when (and (some? processors)
             (or (not (sequential? processors))
                 (not-every? #(satisfies? export/SpanProcessor %) processors)))
    (throw (ex-info ":span-processors must be a sequence of span processors"
                    {:otel.sdk/error :invalid-span-processors})))
  processors)

;; An instance is used for whichever signals it actually implements; the others
;; get no exporter rather than quietly falling back to the network.
(defn- build-span-exporter [exporter opts]
  (case exporter
    :none nil
    :console (stdout/exporter {})
    :json (stdout/json-exporter {})
    :otlp (otlp/exporter opts)
    (when (satisfies? export/SpanExporter exporter) exporter)))

(defn- build-metric-exporter [exporter opts]
  (case exporter
    :none nil
    (:console :json) (stdout/metric-exporter {})
    :otlp (otlp/metric-exporter opts)
    (when (satisfies? export/MetricExporter exporter) exporter)))

(defn- build-log-exporter [exporter opts]
  (case exporter
    :none nil
    (:console :json) (stdout/log-exporter {})
    :otlp (otlp/log-exporter opts)
    (when (satisfies? sdk-logs/LogRecordExporter exporter) exporter)))

(def ^:private maintained-exporter-factories
  {:spans build-span-exporter :metrics build-metric-exporter :logs build-log-exporter})

(defn- enter-exporter-factory!
  [receipt exporter signal factory]
  ;; A foreign factory receives a multi-protocol object and may use any face,
  ;; even if it returns successfully. Built-in factories only select this face.
  (let [maintained? (identical? factory (get maintained-exporter-factories signal))]
    (doseq [face (if maintained? [signal] [:spans :metrics :logs])]
      (lifecycle/mark-construction-face! receipt exporter face
                                         (if maintained? :unknown :foreign-unknown)))))

(defn- acquire-signal!
  "Register the raw face before explicit constructor handoff. Never wrap payloads."
  [receipt exporter signal release! construct]
  (let [child (lifecycle/construction-receipt) ownership (atom :init-owned)
        raw-terminal (lifecycle/terminal-action)
        face (reify lifecycle/SettlementWitness
               (settlement-status [_]
                 {:quiescence
                  (case @ownership
                    :child-owned (:quiescence (lifecycle/construction-settlement child))
                    (:init-owned :orphan)
                    (if (and (contains? #{:returned-true :returned-false :threw}
                                        (lifecycle/terminal-status raw-terminal))
                             (or (= :returned-true (lifecycle/terminal-status raw-terminal))
                                 (= :confirmed (:quiescence
                                                 (lifecycle/component-settlement exporter)))))
                      :confirmed :unconfirmed)
                    :unconfirmed)}))
        close! (fn []
                 (case @ownership
                   (:init-owned :orphan) (lifecycle/run-terminal! raw-terminal release!)
                   :child-owned (lifecycle/retire-construction! child)
                   ;; Retire known child records, but do not attest foreign errors
                   ;; or independently release the original raw exporter face.
                   (lifecycle/retire-construction! child)))]
    (lifecycle/acquire-owner! receipt face close!)
    (lifecycle/mark-construction-face! receipt exporter signal :sdk-owned)
    (reset! ownership :unknown)
    (try
      (let [owner (construct child)]
        (reset! ownership (:ownership (lifecycle/construction-resource-status child nil exporter signal)))
        owner)
      (catch :default error
        (reset! ownership (:ownership (lifecycle/construction-resource-status child error exporter signal)))
        (throw error)))))

(defn- construct-return!
  "Preinstall an unknown-acquisition barrier before a replaceable factory call.
  A throw without returned ownership cannot become empty-resource permission."
  [receipt construct]
  (let [returned? (atom false)
        observer (reify lifecycle/SettlementWitness
                   (settlement-status [_]
                     {:quiescence (if @returned? :confirmed :unconfirmed)}))]
    (lifecycle/acquire-owner! receipt observer (constantly true))
    (let [value (construct)]
      (reset! returned? true)
      value)))

(defn- acquire-provider!
  "Admission-only adapter for an actual maintained provider. Child receipts
  independently gate every processor/exporter face; flags alone do not do so."
  [receipt provider kind]
  (let [terminal (:terminal provider)
        state (if (= :traces kind) (:shutdown? provider) (:state provider))
        known? (if (= :traces kind)
                 (instance? otel.sdk.tracer.SdkTracerProvider provider)
                 (instance? otel.sdk.metrics.SdkMeterProvider provider))
        witness (reify lifecycle/SettlementWitness
                  (settlement-status [_]
                    {:quiescence
                     (if (and known?
                              (contains? #{:returned-true :returned-false :threw}
                                         (lifecycle/terminal-status terminal))
                              (if (= :traces kind) (true? @state)
                                  (true? (:shutdown? @state))))
                       :confirmed :unconfirmed)}))]
    (lifecycle/acquire-owner! receipt witness
      #(if (= :traces kind) (sdk-tracer/shutdown! provider)
           (sdk-metrics/shutdown! provider)))
    provider))

(defn- registry-transition!
  "Pure retry computation; capture previous receipt only after successful CAS."
  [transition]
  (loop []
    (let [before @registry [after result] (transition before)]
      (if (compare-and-set! registry before after)
        (assoc result :snapshot after)
        (recur)))))

(defn- notify-registry!
  "Internal versioned notification view, never authority or liveness proof.
  No lock is held across synchronous watches, including reentrant SDK calls."
  [snapshot]
  (let [view (assoc (:providers snapshot) :registry-revision (:revision snapshot))]
    (loop []
      (let [before @global]
        (cond
          (>= (get before :registry-revision 0) (:revision snapshot)) nil
          (compare-and-set! global before view) nil
          :else (recur))))))

(defn- global-installation
  [providers]
  (let [id (Object.) state (atom {}) terminal (lifecycle/terminal-action)
        witness (reify lifecycle/SettlementWitness
                  (settlement-status [_]
                    {:quiescence
                     (if (and (:retired? @state)
                              (not (contains? (:active @registry) id))
                              (contains? #{:returned-true :returned-false :threw}
                                         (lifecycle/terminal-status terminal)))
                       :confirmed :unconfirmed)}))
        retire! (fn [restore-previous?]
                  (lifecycle/run-terminal! terminal
                    (fn []
                      (let [{:keys [previous previous-owners]} @state
                            result
                            (registry-transition!
                              (fn [before]
                                (let [active (disj (:active before) id)
                                      restored
                                      (reduce-kv
                                        (fn [snapshot key installed]
                                          (if (and (identical? id (get (:owners before) key))
                                                   (identical? installed (get (:providers before) key)))
                                            (let [prior-id (get previous-owners key)
                                                  restore? (and restore-previous? (some? prior-id)
                                                                (contains? active prior-id))]
                                              (-> snapshot
                                                  (assoc-in [:providers key] (if restore? (get previous key) nil))
                                                  (assoc-in [:owners key] (when restore? prior-id))))
                                            snapshot))
                                        before providers)]
                                  [(assoc restored :active active :revision (inc (:revision before))) {}])))]
                        ;; Record committed lifetime retirement BEFORE watch
                        ;; dispatch; a notification throw is not an active owner.
                        (swap! state assoc :retired? true)
                        (notify-registry! (:snapshot result))
                        true))))]
    {:owner witness
     :retire! #(retire! true)
     :clear! #(retire! false)
     :install!
     (fn []
       (let [result
             (registry-transition!
               (fn [before]
                 [(-> before
                      (assoc :providers (merge (:providers before) providers))
                      (assoc :owners (reduce-kv (fn [owners key _] (assoc owners key id))
                                                (:owners before) providers))
                      (update :active conj id)
                      (update :revision inc))
                  {:previous (:providers before) :previous-owners (:owners before)}]))]
         (reset! state {:previous (:previous result) :previous-owners (:previous-owners result)
                        :published? true})
         (notify-registry! (:snapshot result))))}))

(defn- clear-legacy-publication!
  "No lifetime evidence: clear only this handle's exact still-current fields."
  [handle]
  (let [result
        (registry-transition!
          (fn [before]
            [(reduce (fn [snapshot key]
                       (if (and (some? (get handle key))
                                (identical? (get handle key) (get (:providers before) key)))
                         (-> snapshot (assoc-in [:providers key] nil) (assoc-in [:owners key] nil))
                         snapshot))
                     (update before :revision inc)
                     [:tracer-provider :meter-provider :logger-provider]) {}]))]
    (notify-registry! (:snapshot result))
    true))

(defn init!
  "Configure and install a tracing (and, unless disabled, metrics) SDK.

  Options — every one falls back to its standard OTEL_* environment variable, so
  a deployment can be configured without touching code:

    :service-name     sets service.name (OTEL_SERVICE_NAME)
    :resource         a resource merged over the detected defaults
    :sampler          a sampler (OTEL_TRACES_SAMPLER / _ARG)
    :exporter         :otlp (default), :console, :json, :none, or an exporter
                      instance -- which is used for whichever signals it
                      implements, so a memory exporter in a test collects spans
                      without metrics reaching the network
    :endpoint         OTLP base endpoint (OTEL_EXPORTER_OTLP_ENDPOINT)
    :headers          extra OTLP request headers
    :processor        :batch (default) or :simple
    :span-processors  optional replacement tracing processor sequence; the SDK
                      owns and shuts down every supplied processor
    :metrics?         collect metrics (default true)
    :runtime-metrics? register the Chez runtime instruments (default true)
    :metric-interval-ms  metric collection period (default 60000)
    :logs?            emit the logs signal (default false)
    :bridge-logging?  route clojure.tools.logging through it (default true when
                      :logs? is on) -- additive, the existing backend keeps working
    :construction-receipt caller-created per-invocation receipt for matching
                      failure settlement evidence; never reuse it

  Returns a handle for `shutdown!`. Honours OTEL_SDK_DISABLED=true by installing
  nothing, which is how the spec says to turn telemetry off without code changes."
  ([] (init! {}))
  ([{:keys [service-name resource sampler exporter processor span-processors
            metrics? runtime-metrics? metric-interval-ms logs? bridge-logging?]
     :or {exporter :otlp processor :batch metrics? true runtime-metrics? true
          logs? false bridge-logging? true}
     :as opts}]
   (let [receipt (or (:construction-receipt opts) (lifecycle/construction-receipt))]
     (lifecycle/with-construction! receipt
       (fn []
         (check-exporter exporter)
         (check-span-processors span-processors)
         ;; Account only exact caller-supplied faces. Unknown/keyword factories
         ;; cannot manufacture a queryable input resource identity.
         (lifecycle/account-construction-faces! receipt
           (cond-> []
             (satisfies? export/SpanExporter exporter)
             (conj {:resource exporter :signal :spans})
             (satisfies? export/MetricExporter exporter)
             (conj {:resource exporter :signal :metrics})
             (satisfies? sdk-logs/LogRecordExporter exporter)
             (conj {:resource exporter :signal :logs})))
         ;; Supplied processors can retain a multi-protocol object, not merely
         ;; its span face. No skipped factory establishes negative ownership.
         (when (some? span-processors)
           (doseq [signal [:spans :metrics :logs]]
             (lifecycle/mark-construction-face! receipt exporter signal :unknown)))
         (if (disabled?)
           {:disabled? true :terminal (lifecycle/terminal-action)}
           (let [base (res/merge-resources
                        (res/default-resource)
                        (cond-> (or resource res/empty-resource)
                          service-name (res/merge-resources
                                         (res/resource {:service.name service-name}))))
                 span-factory build-span-exporter
                 span-exporter (when-not (some? span-processors)
                                 (enter-exporter-factory! receipt exporter :spans span-factory)
                                 (construct-return! receipt
                                   #(span-factory exporter
                                      (select-keys opts [:endpoint :headers :traces-url :timeout-ms]))))
                 processors (if (some? span-processors)
                              (mapv #(lifecycle/acquire-owner! receipt % (fn [] (export/shutdown! %)))
                                    span-processors)
                              (if span-exporter
                                [(acquire-signal! receipt span-exporter :spans
                                   #(export/shutdown-exporter! span-exporter)
                                   (fn [child]
                                     (if (= :simple processor)
                                       (export/simple-processor span-exporter child)
                                       (export/batch-processor span-exporter
                                         (select-keys opts [:schedule-delay-ms :max-queue-size
                                                            :max-export-batch-size]) child))))]
                                []))
                 tp (acquire-provider! receipt
                      (construct-return! receipt
                        #(sdk-tracer/tracer-provider
                           {:resource base :sampler (or sampler (env-sampler) sampler/default-sampler)
                            :processors processors :limits (:limits opts)})) :traces)
                 mp (when metrics?
                      (acquire-provider! receipt
                        (construct-return! receipt
                          #(sdk-metrics/meter-provider
                             {:resource base :temporality (:temporality opts)})) :metrics))
                 metric-factory build-metric-exporter
                 metric-exporter (when mp
                                   (enter-exporter-factory! receipt exporter :metrics metric-factory)
                                   (construct-return! receipt
                                     #(metric-factory exporter
                                        (select-keys opts [:endpoint :headers :metrics-url :timeout-ms]))))
                 reader (when metric-exporter
                          (acquire-signal! receipt metric-exporter :metrics
                            #(export/shutdown-metric-exporter! metric-exporter)
                            #(sdk-metrics/periodic-reader mp metric-exporter
                               {:interval-ms (or metric-interval-ms 60000)} %)))
                 log-factory build-log-exporter
                 log-exporter (when logs?
                                (enter-exporter-factory! receipt exporter :logs log-factory)
                                (construct-return! receipt
                                  #(log-factory exporter
                                     (select-keys opts [:endpoint :headers :logs-url :timeout-ms]))))
                 log-processor (when log-exporter
                                 (acquire-signal! receipt log-exporter :logs
                                   #(sdk-logs/shutdown-log-exporter! log-exporter)
                                   #(if (= :simple processor)
                                      (sdk-logs/simple-processor log-exporter %)
                                      (sdk-logs/batch-processor log-exporter {} %))))
                 lp (when log-processor
                      (let [provider (construct-return! receipt
                                       #(sdk-logs/logger-provider
                                          {:resource base :processors [log-processor]}))]
                        (lifecycle/acquire-owner! receipt provider #(sdk-logs/shutdown! provider))))
                 logging-installation (when (and lp bridge-logging?)
                                        (let [token (tools-logging/installation lp)]
                                          (lifecycle/acquire-owner! receipt token
                                            #(tools-logging/retire-installation! token))))
                 previous-factory (when logging-installation
                                    (tools-logging/install-owned! logging-installation))
                 publication (global-installation
                               {:tracer-provider tp :meter-provider mp :logger-provider lp})]
             (lifecycle/acquire-owner! receipt (:owner publication) (:retire! publication))
             (when (and mp runtime-metrics?)
               (runtime/register! (sdk-metrics/get-meter mp
                                    {:name "otel.instrument.runtime" :version res/sdk-version})))
             ((:install! publication))
             {:tracer-provider tp :meter-provider mp :logger-provider lp :reader reader
              :shutdown-components (cond-> (vec processors) reader (conj reader) lp (conj lp))
              :terminal (lifecycle/terminal-action)
              :previous-logger-factory previous-factory
              :logging-installation logging-installation
              :global-installation publication
              :propagator propagation/default-propagator})))))))

(defn construction-face-status
  "Observe root startup transfer for an exact reported failure/exporter/signal.
  :sdk-owned must not be independently released; :unacquired requires explicit
  accounting and full rollback proof; all other evidence stays :unknown."
  [receipt error exporter signal]
  (lifecycle/construction-face-status receipt error exporter signal))

(defn force-flush!
  "Push everything buffered to the exporters now."
  [handle]
  (boolean
    (and (some-> (:tracer-provider handle) sdk-tracer/force-flush!)
         (or (nil? (:reader handle)) (export/force-flush! (:reader handle)))
         (or (nil? (:logger-provider handle)) (sdk-logs/force-flush! (:logger-provider handle))))))

(def ^:private shutdown-component-keys
  [:previous-logger-factory :reader :logger-provider :meter-provider
   :tracer-provider])

(defn- component-handle?
  [handle]
  (boolean (some #(some? (get handle %)) shutdown-component-keys)))

(defn shutdown-status
  "Closed ownership evidence for the handle returned by init!.
  Maintained and contract-conforming witnesses observe bounded, nonwaiting
  snapshots. Trusted custom witnesses are invoked synchronously and MUST obey
  that contract; this API does not isolate or enforce their execution latency.
  A failed terminal action is not quiescence proof. Both SDK operations and
  exporter resources must be settled. Exporter release must declare literal
  true or supply its own stable retired-settlement witness; failed/void release
  and unknown/custom components remain unconfirmed. Later settlement refreshes this observation
  without replaying shutdown. No resource, exception or payload is returned."
  [handle]
  (let [terminal (:terminal handle)
        components (cond
                     (:disabled? handle) []
                     (contains? handle :shutdown-components) (:shutdown-components handle)
                     :else [nil])]
    (assoc (lifecycle/combined-settlement components terminal)
           :otel.sdk.shutdown-status/version 1)))

(defn shutdown!
  "Retire this SDK's publication, then stop every owned resource.
  Notification failures cannot skip resource callbacks. Foreign provider
  retirement outside the SDK handle does not participate in lifetime tracking."
  [handle]
  (let [action
        (fn []
          (let [actions (cond-> [#(if-let [publication (:global-installation handle)]
                                   ((:clear! publication))
                                   (clear-legacy-publication! handle))]
                          (:logging-installation handle)
                          (conj #(tools-logging/retire-installation! (:logging-installation handle)))
                          (and (nil? (:logging-installation handle)) (:previous-logger-factory handle))
                          (conj #(tools-logging/uninstall! (:previous-logger-factory handle)))
                          (:reader handle)
                          (conj #(export/shutdown! (:reader handle)))
                          (:logger-provider handle)
                          (conj #(sdk-logs/shutdown! (:logger-provider handle)))
                          (:meter-provider handle)
                          (conj #(sdk-metrics/shutdown! (:meter-provider handle)))
                          (:tracer-provider handle)
                          (conj #(sdk-tracer/shutdown! (:tracer-provider handle))))]
            ;; run-all! preserves its first Throwable and still executes EVERY
            ;; action. No registry CAS/lock spans any callback or worker join.
            (lifecycle/run-all! actions)))]
    (if-let [terminal (:terminal handle)]
      (lifecycle/run-terminal! terminal action)
      (if (component-handle? handle)
        (throw (ex-info "SDK shutdown requires the lifecycle handle returned by init!"
                        {:otel.sdk/error :invalid-shutdown-handle :missing :terminal}))
        true))))
