(ns otel.sdk.lifecycle
  "Small shared lifecycle primitives used by SDK owners.")

(defn terminal-action
  "Return the state cell for one exactly-once terminal action."
  []
  (atom nil))

(defn- observe!
  [outcome]
  (let [{:keys [value throwable]} @outcome]
    (if throwable
      (throw throwable)
      value)))

(defn run-terminal!
  "Run `action` at most once. Concurrent and later callers wait for and observe
  the same returned object, or rethrow the same Throwable object."
  [terminal action]
  (loop []
    (if-let [outcome @terminal]
      (observe! outcome)
      (let [outcome (promise)]
        (if (compare-and-set! terminal nil outcome)
          (do
            (try
              (deliver outcome {:value (action)})
              (catch :default throwable
                (deliver outcome {:throwable throwable})))
            (observe! outcome))
          (recur))))))

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
