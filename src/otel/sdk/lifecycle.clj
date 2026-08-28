(ns otel.sdk.lifecycle
  "Small shared lifecycle primitives used by SDK owners.")

(defn run-all!
  "Run every zero-argument action, even after false or a throw. Return their
  aggregate truth value, or rethrow the first Throwable after all have run."
  [actions]
  (loop [remaining (seq actions)
         ok true
         failure nil]
    (if-let [action (first remaining)]
      (let [outcome (try
                      {:value (action)}
                      (catch :default throwable
                        {:throwable throwable}))]
        (recur (next remaining)
               (and (boolean (:value outcome)) ok)
               (or failure (:throwable outcome))))
      (if failure (throw failure) ok))))
