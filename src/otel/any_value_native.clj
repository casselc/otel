(ns otel.any-value-native
  "Optional SDK-owned bounded primitive-map conversion on Jolt/Chez."
  (:require [clojure.java.io :as io] [jolt.scheme :as scheme]))

(defmacro ^:private source []
  (if-let [url (io/resource "otel/scalar_string_map.ss")]
    (str "(begin "
         "(unless (and "
         "(eq? (record-type-uid (record-type-descriptor pmap)) 'chez-pmap-v6) "
         "(equal? (record-type-field-names (record-type-descriptor pmap)) '#(root cnt hasheq meta)) "
         "(eq? (record-type-uid (record-type-descriptor hnode)) 'chez-hnode-v1) "
         "(equal? (record-type-field-names (record-type-descriptor hnode)) '#(bm arr))) "
         "(error 'scalar-string-map \"Unsupported map layout\")) "
         "(unless (for-all (lambda (entry) "
         "(and (top-level-bound? (car entry)) "
         "(procedure? (top-level-value (car entry))) "
         "(bitwise-bit-set? (procedure-arity-mask (top-level-value (car entry))) (cadr entry)))) "
         "'((make-pmap 2) (var-cell-deref 1) (jolt-fixnum->string 1))) "
         "(error 'scalar-string-map \"Unsupported runtime helpers\")) "
         "(unless (and (>= (most-negative-fixnum) -9223372036854775808) "
         "(<= (most-positive-fixnum) 9223372036854775807)) "
         "(error 'scalar-string-map \"Native fixnums exceed Int64\")) "
         (slurp url) ")")
    (throw (ex-info "Missing native scalar-map resource" {}))))

(def ^:private converter-source (source))

(defn load-string-map-converter!
  "Internal: policy anchors come from otel.any-value's defining namespace."
  [bindings maximum]
  (let [convert ((scheme/eval-string converter-source) bindings maximum)]
    (fn [value] (convert value))))
