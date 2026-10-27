(ns otel.sorted-runtime-fixture
  "Deliberately malformed private inputs for both qualified sorted layouts.
  New scoll wrappers are immutable through the public host API; construct a
  separate corrupt wrapper for admission tests rather than bypassing that API."
  (:require [jolt.scheme :as scheme] [jolt.host :as host]))

(def native-nodes?
  ((scheme/eval-string "(lambda (m) (eq? (record-type-uid (record-rtd m)) 'chez-scoll-v1))")
   (sorted-map)))
(def ^:private native-copy
  (when native-nodes?
    (scheme/eval-string
      "(lambda (m field value)
         (make-scoll* #f #f (scoll-kind m) (scoll-tag m)
           (if (eq? field (keyword #f \"tree\")) value (scoll-tree m))
           (if (eq? field (keyword #f \"cnt\")) value (scoll-cnt m))
           (scoll-cmp m) (scoll-cmp-fn m)
           (if (eq? field (keyword #f \"ops\")) value (scoll-ops m))))")))
(defn altered-copy [m field value]
  (if native-nodes?
    (native-copy m field value)
    (let [copy (with-meta m {:test-copy true})]
      (host/ref-put! copy field value)
      copy)))
