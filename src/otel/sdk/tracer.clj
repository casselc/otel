(ns otel.sdk.tracer
  "The SDK tracer provider: the object an application configures once at startup
  and the tracers it hands out.

  The provider owns everything that is a whole-process decision — the resource,
  the sampler, the span limits, the clock, and the processor pipeline — and a
  tracer is a thin handle that stamps its instrumentation scope onto the spans it
  creates. That split is why a library can take a tracer at load time without
  knowing or caring how the application configured export."
  (:require [otel.attributes :as attr]
            [otel.context :as ctx]
            [otel.id :as id]
            [otel.resource :as res]
            [otel.sdk.clock :as clock]
            [otel.sdk.export :as export]
            [otel.sdk.lifecycle :as lifecycle]
            [otel.sdk.sampler :as sampler]
            [otel.sdk.span :as span]
            [otel.trace :as trace]))

(defrecord SdkTracer [provider scope]
  trace/Tracer
  (start-span* [_ name opts]
    (let [{:keys [resource sampler limits clock processor shutdown?]} provider
          ;; Direct callers of the historical seven-field record constructor
          ;; have no extension-map entry. Preserve that source-compatible path.
          id-generator (if (nil? (:id-generator provider))
                         id/default-id-generator
                         (:id-generator provider))]
      (locking shutdown?
        (if @shutdown?
          ;; After shutdown nothing can be exported, so a span would only cost
          ;; memory. It still propagates: a non-recording span keeps the trace
          ;; intact for anything downstream that is still running.
          (trace/non-recording-span trace/invalid-span-context)
          (let [parent-ctx (or (:parent opts) (ctx/current))
              parent-sc (trace/span-context-of (trace/span-from-context parent-ctx))
              parent? (trace/valid? parent-sc)
              ;; A child stays in its parent's trace; a root starts a new one.
              generated-trace
              (when-not parent?
                (let [generated (id/generate-trace-id id-generator)
                      trace-id (:id generated)
                      random? (:random? generated)]
                  (when-not (id/valid-trace-id? trace-id)
                    (throw
                     (ex-info "OTel id generator returned an invalid trace id"
                              {:type ::invalid-generated-id
                               :id-kind :trace
                               :reason :invalid-id})))
                  (when-not (or (true? random?) (false? random?))
                    (throw
                     (ex-info "OTel trace id requires Boolean random provenance"
                              {:type ::invalid-generated-id
                               :id-kind :trace
                               :reason :invalid-random-provenance})))
                  {:id trace-id :random? random?}))
              trace-id (if parent? (:trace-id parent-sc) (:id generated-trace))
              span-id (let [generated (id/generate-span-id id-generator)]
                        (when-not (id/valid-span-id? generated)
                          (throw
                           (ex-info "OTel id generator returned an invalid span id"
                                    {:type ::invalid-generated-id
                                     :id-kind :span
                                     :reason :invalid-id})))
                        generated)
              kind (:kind opts :internal)
              links (vec (:links opts))
              decision (sampler/should-sample
                         sampler
                         {:parent-context parent-ctx
                          :trace-id trace-id
                          :name name
                          :kind kind
                          :attributes (:attributes opts)
                          :links links})
              sc (trace/span-context
                   {:trace-id trace-id
                    :span-id span-id
                    ;; The sampler owns bit 0. Bit 1 describes the trace id and
                    ;; therefore stays unchanged while a trace is continued.
                    ;; New trace ids come from otel.id's random generator.
                    :trace-flags
                    (bit-or (if (sampler/sampled? decision)
                              trace/flag-sampled
                              0)
                            (if parent?
                              (bit-and (or (:trace-flags parent-sc) 0)
                                       trace/flag-random)
                              (if (:random? generated-trace)
                                trace/flag-random
                                0)))
                    ;; The parent's trace state travels on unless the sampler
                    ;; replaced it — it is how vendors carry their own routing
                    ;; data along a trace.
                    :trace-state (or (:trace-state decision)
                                     (when parent? (:trace-state parent-sc))
                                     [])})]
          (if-not (sampler/recording? decision)
            (trace/non-recording-span sc)
            (let [sp (span/new-span
                       {:span-context sc
                        :parent-span-id (when parent? (:span-id parent-sc))
                        :name name
                        :kind kind
                        :scope scope
                        :resource resource
                        :start-time-unix-nano (or (:start-timestamp opts)
                                                  (clock/wall-nanos clock))
                        :clock clock
                        :limits limits
                        :processor processor
                        ;; A sampler may contribute attributes of its own; they
                        ;; are merged under the caller's, which win.
                        :attributes (merge (:attributes decision) (:attributes opts))
                        :links links})]
              (export/on-start processor sp parent-ctx)
              sp))))))))

(defrecord SdkTracerProvider [resource sampler limits clock processor shutdown? terminal])

(defn tracer-provider
  "Build a tracer provider.

  Options:
    :resource    what produced the telemetry (default `res/default-resource`)
    :sampler     sampling policy (default parent-based + always-on)
    :processors  a sequence of span processors (default none — spans are recorded
                 but go nowhere)
    :limits      per-span limits, see `otel.sdk.span/default-limits`
    :clock       the clock to time spans with (default the system clock)
    :id-generator  an `otel.id/IdGenerator` (default OS entropy). Deterministic
                 generators can make replay ids reproducible without falsely
                 setting the W3C random trace-id flag

  The clock is anchored: timestamps stay epoch-based, but every interval within
  the process comes from the monotonic clock, so a wall-clock step cannot produce
  a span that ends before it started."
  [{:keys [resource sampler processors limits clock id-generator]}]
  (let [id-generator (if (nil? id-generator)
                       id/default-id-generator
                       id-generator)]
    (when-not (satisfies? id/IdGenerator id-generator)
      (throw
       (ex-info "OTel tracer provider requires an IdGenerator"
                {:type ::invalid-id-generator
                 :value-class (str (class id-generator))})))
    (assoc
     (->SdkTracerProvider (or resource (res/default-resource))
                          (or sampler sampler/default-sampler)
                          (span/span-limits limits)
                          (clock/anchored (or clock clock/system))
                          (export/composite-processor (or processors []))
                          (atom false)
                          (lifecycle/terminal-action))
     :id-generator id-generator)))

(defn get-tracer
  "A tracer for one instrumentation scope. `:name` identifies the instrumenting
  library (not the application) and is required; `:version` and `:schema-url` are
  optional but recommended, since a backend uses them to tell versions of an
  instrumentation apart."
  [provider {:keys [name version schema-url attributes]}]
  (->SdkTracer provider
               (attr/normalize-scope
                {:name name
                 :version version
                 :schema-url schema-url
                 :attributes attributes})))

(defn force-flush!
  "Block until everything already ended has been handed to the exporters."
  [provider]
  (export/force-flush! (:processor provider)))

(defn shutdown!
  "Flush and stop. The provider stops recording; call this before the process
  exits or buffered spans are lost."
  [provider]
  (lifecycle/run-terminal!
    (:terminal provider)
    #(locking (:shutdown? provider)
       (reset! (:shutdown? provider) true)
       (export/shutdown! (:processor provider)))))
