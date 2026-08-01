(ns otel.context
  "The propagation context: an immutable key/value map carried alongside a unit of
  work, plus the thread-local slot naming the one that is currently active.

  Everything that has to travel with a logical operation — the active span,
  baggage, anything an instrumentation library wants to thread through — lives
  here rather than in separate globals, so a single `with-context` swaps all of it
  at once and a single capture carries all of it to another thread.

  The active context is a dynamic var, so it unwinds correctly on a normal return
  and on a throw, and nesting works without bookkeeping. Jolt conveys dynamic
  bindings into `future`, matching Clojure; a raw `Thread.` does not inherit them
  on any host, so work handed to an arbitrary thread or executor must be wrapped
  with `bind-fn`."
  (:refer-clojure :exclude [get remove]))

(def root
  "The empty context. Every propagation chain starts here."
  {})

(def ^:dynamic *current*
  "The active context on this thread. Read it with `current`, install one with
  `with-context`."
  root)

(defn current
  "The context active on this thread."
  []
  *current*)

(defn get-value
  "The value stored under `k`, or nil."
  [ctx k]
  (clojure.core/get ctx k))

(defn with-value
  "A new context with `k` bound to `v`. `ctx` is unchanged."
  [ctx k v]
  (assoc ctx k v))

(defn remove-value
  "A new context with `k` absent. `ctx` is unchanged."
  [ctx k]
  (dissoc ctx k))

(defmacro with-context
  "Run `body` with `ctx` as the active context, restoring the previous one on the
  way out — including when `body` throws."
  [ctx & body]
  `(binding [*current* ~ctx] ~@body))

(defn bind-fn*
  "Wrap `f` so it runs with `ctx` active, wherever and whenever it is later
  called. Use for work handed to a thread or executor that would otherwise start
  from the root context."
  [ctx f]
  (fn [& args]
    (binding [*current* ctx]
      (apply f args))))

(defn bind-fn
  "Wrap `f` so it runs with the context that is active *now*, wherever it is
  later called."
  [f]
  (bind-fn* (current) f))
