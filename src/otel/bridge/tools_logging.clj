(ns otel.bridge.tools-logging
  "Bridge clojure.tools.logging into the OpenTelemetry logs signal.

  This is how an application gets the logs signal: it keeps writing
  `(log/info \"...\")` and the records also become OTLP log records, correlated
  with whatever span was active when the line was written. That correlation is
  the payoff — a backend can show the log lines for one request beside the trace
  for that same request, which is not something the log line alone can express.

  The bridge is *additive*. `install!` wraps the factory that is already there
  rather than replacing it, so stderr (or whatever else was configured) keeps
  working exactly as before. Telemetry that silences the existing logs would be a
  bad trade, and it is also the kind of change that goes unnoticed until an
  incident.

  tools.logging's levels map cleanly onto OTel severities — :trace/:debug/:info/
  :warn/:error/:fatal are exactly the spec's six ranges — so nothing is lost in
  translation."
  (:require [clojure.tools.logging :as log]
            [clojure.tools.logging.impl :as impl]
            [otel.logs :as logs]))

(defn- exception-attributes
  "Semantic-convention attributes for a throwable attached to a log call."
  [t]
  (let [msg (try (ex-message t) (catch :default _ nil))
        data (try (ex-data t) (catch :default _ nil))]
    (cond-> {:exception.type (try (.getName (class t)) (catch :default _ (str t)))}
      msg (assoc :exception.message msg)
      data (assoc :exception.data (pr-str data)))))

(defn otel-logger
  "A tools.logging Logger that writes to `delegate` and also emits an OTel record."
  [delegate otel-log logger-ns]
  (reify impl/Logger
    (enabled? [_ level] (impl/enabled? delegate level))
    (write! [_ level throwable message]
      ;; The delegate goes first, so the familiar output happens even if the
      ;; telemetry path is misconfigured or throws.
      (impl/write! delegate level throwable message)
      (try
        (logs/emit! otel-log
                    {:body (str message)
                     :severity level
                     :attributes (cond-> {:logger.name (str logger-ns)}
                                   throwable (merge (exception-attributes throwable)))})
        (catch :default _ nil)))))

(defrecord OtelLoggerFactory [delegate logger-provider]
  impl/LoggerFactory
  (name [_] (str "otel(" (impl/name delegate) ")"))
  (get-logger [_ logger-ns]
    (otel-logger (impl/get-logger delegate logger-ns)
                 (logs/get-logger* logger-provider {:name (str logger-ns)})
                 logger-ns)))

(defn factory
  "A LoggerFactory that tees `delegate`'s output into `logger-provider`."
  [delegate logger-provider]
  (->OtelLoggerFactory delegate logger-provider))

(defn install!
  "Install the bridge globally, wrapping whatever factory is currently active.
  Returns the previous factory so `uninstall!` can restore it."
  [logger-provider]
  (let [previous log/*logger-factory*]
    (alter-var-root #'log/*logger-factory* (fn [f] (factory f logger-provider)))
    previous))

(defn uninstall!
  "Restore a factory returned by `install!`."
  [previous]
  (alter-var-root #'log/*logger-factory* (constantly previous))
  previous)
