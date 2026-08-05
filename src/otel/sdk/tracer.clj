(ns otel.sdk.tracer
  "The SDK tracer provider: the object an application configures once at startup
  and the tracers it hands out.

  The provider owns everything that is a whole-process decision — the resource,
  the sampler, the span limits, the clock, and the processor pipeline — and a
  tracer is a thin handle that stamps its instrumentation scope onto the spans it
  creates. That split is why a library can take a tracer at load time without
  knowing or caring how the application configured export."
  (:require [otel.context :as ctx]
            [otel.id :as id]
            [otel.resource :as res]
            [otel.sdk.clock :as clock]
            [otel.sdk.export :as export]
            [otel.sdk.sampler :as sampler]
            [otel.sdk.span :as span]
            [otel.trace :as trace]))

(defrecord SdkTracer [provider scope]
  trace/Tracer
  (start-span* [_ name opts]
    (let [{:keys [resource sampler limits clock processor shutdown?]} provider
          ;; A provider built directly through the historical six-argument
          ;; record constructor has no extension-map entry. Preserve that public
          ;; construction path by applying the production default at use time.
          id-generator (if (nil? (:id-generator provider))
                         id/default-id-generator
                         (:id-generator provider))]
      (if @shutdown?
        ;; After shutdown nothing can be exported, so a span would only cost
        ;; memory. It still propagates: a non-recording span keeps the trace
        ;; intact for anything downstream that is still running.
        (trace/non-recording-span trace/invalid-span-context)
        (let [parent-ctx (or (:parent opts) (ctx/current))
              parent-sc (trace/span-context-of (trace/span-from-context parent-ctx))
              parent? (trace/valid? parent-sc)
              ;; A child stays in its parent's trace; a root starts a new one.
              ;; Every span id comes from the provider's generator, so a
              ;; deterministic generator makes a whole run reproducible.
              trace-id (if parent?
                         (:trace-id parent-sc)
                         (let [generated (id/generate-trace-id id-generator)]
                           (when-not (id/valid-trace-id? generated)
                             (throw
                              (ex-info "OTel id generator returned an invalid trace id"
                                       {:type ::invalid-generated-id
                                        :id-kind :trace
                                        :value generated})))
                           generated))
              span-id (let [generated (id/generate-span-id id-generator)]
                        (when-not (id/valid-span-id? generated)
                          (throw
                           (ex-info "OTel id generator returned an invalid span id"
                                    {:type ::invalid-generated-id
                                     :id-kind :span
                                     :value generated})))
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
                    :sampled? (sampler/sampled? decision)
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
              sp)))))))

(defrecord SdkTracerProvider [resource sampler limits clock processor shutdown?])

(defn tracer-provider
  "Build a tracer provider.

  Options:
    :resource    what produced the telemetry (default `res/default-resource`)
    :sampler     sampling policy (default parent-based + always-on)
    :processors  a sequence of span processors (default none — spans are recorded
                 but go nowhere)
    :limits      per-span limits, see `otel.sdk.span/default-limits`
    :clock       the clock to time spans with (default the system clock)
    :id-generator  where trace and span ids come from (default
                 `id/default-id-generator`, OS entropy — pass a deterministic
                 `id/IdGenerator` to make a run reproducible)

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
    ;; Keep SdkTracerProvider's historical six-argument record constructor
    ;; source-compatible. The new optional field lives in the record extmap.
    (assoc
     (->SdkTracerProvider (or resource (res/default-resource))
                          (or sampler sampler/default-sampler)
                          (span/span-limits limits)
                          (clock/anchored (or clock clock/system))
                          (export/composite-processor (or processors []))
                          (atom false))
     :id-generator id-generator)))

(defn get-tracer
  "A tracer for one instrumentation scope. `:name` identifies the instrumenting
  library (not the application) and is required; `:version` and `:schema-url` are
  optional but recommended, since a backend uses them to tell versions of an
  instrumentation apart."
  [provider {:keys [name version schema-url attributes]}]
  (->SdkTracer provider {:name name
                         :version version
                         :schema-url schema-url
                         :attributes (or attributes {})}))

(defn force-flush!
  "Block until everything already ended has been handed to the exporters."
  [provider]
  (export/force-flush! (:processor provider)))

(defn shutdown!
  "Flush and stop. The provider stops recording; call this before the process
  exits or buffered spans are lost."
  [provider]
  (reset! (:shutdown? provider) true)
  (export/shutdown! (:processor provider)))
