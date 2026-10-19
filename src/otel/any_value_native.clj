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

(defmacro ^:private sorted-source []
  (if-let [url (io/resource "otel/sorted_scalar_string_map.ss")]
    (str "(begin "
         "(unless (and "
         "(eq? (record-type-uid (record-type-descriptor pmap)) 'chez-pmap-v6) "
         "(equal? (record-type-field-names (record-type-descriptor pmap)) '#(root cnt hasheq meta)) "
         "(eq? (record-type-uid (record-type-descriptor htable)) 'chez-htable-v2) "
         "(equal? (record-type-field-names (record-type-descriptor htable)) '#(h hc))) "
         "(error 'sorted-scalar-string-map \"Unsupported table layout\")) "
         "(unless (for-all (lambda (entry) "
         "(and (top-level-bound? (car entry)) (procedure? (top-level-value (car entry))) "
         "(bitwise-bit-set? (procedure-arity-mask (top-level-value (car entry))) (cadr entry)))) "
         "'((jolt-ref-get 2) (pvec-count 1) (pvec-nth! 2) (pmap-array? 1) (var-cell-deref 1) "
         "(jolt-fixnum->string 1) (jolt-transient-new 1) (tmap-put! 3) (jolt-persistent! 1))) "
         "(error 'sorted-scalar-string-map \"Unsupported runtime helpers\")) "
         "(unless (and (>= (most-negative-fixnum) -9223372036854775808) "
         "(<= (most-positive-fixnum) 9223372036854775807)) "
         "(error 'sorted-scalar-string-map \"Native fixnums exceed Int64\")) "
         (slurp url) ")")
    (throw (ex-info "Missing native sorted-map resource" {}))))

(def ^:private sorted-converter-source (sorted-source))

(defn load-sorted-string-map-converter!
  "Internal: defining SDK supplies policy anchors and the trusted core prototype."
  [bindings maximum prototype]
  (let [convert ((scheme/eval-string sorted-converter-source) bindings maximum prototype)]
    (fn [value] (convert value))))

(defmacro ^:private payload-cache-source []
  (if-let [url (io/resource "otel/payload_scalar_string_map_cache.ss")]
    (str "(begin "
         "(unless (and "
         "(eq? (record-type-uid (record-type-descriptor pmap)) 'chez-pmap-v6) "
         "(equal? (record-type-field-names (record-type-descriptor pmap)) '#(root cnt hasheq meta)) "
         "(eq? (record-type-uid (record-type-descriptor htable)) 'chez-htable-v2) "
         "(equal? (record-type-field-names (record-type-descriptor htable)) '#(h hc))) "
         "(error 'payload-scalar-string-map-cache \"Unsupported map layout\")) "
         "(unless (for-all (lambda (entry) "
         "(and (top-level-bound? (car entry)) (procedure? (top-level-value (car entry))) "
         "(bitwise-bit-set? (procedure-arity-mask (top-level-value (car entry))) (cadr entry)))) "
         ;; Record predicates/accessors are lexical syntax, not top-level
         ;; procedures. Their UID/field geometry is fenced above; evaluating
         ;; the source separately checks the actual accessor forms compile.
         "'((pvec-count 1) (pvec-nth! 2) (var-cell-deref 1) "
         "(make-pmap 2) (jolt-ref-get 2) (make-weak-eq-hashtable 0) "
         "(hashtable-ref 3) (hashtable-set! 3))) "
         "(error 'payload-scalar-string-map-cache \"Unsupported runtime helpers\")) "
         (slurp url) ")")
    (throw (ex-info "Missing native payload scalar-map cache resource" {}))))

(def ^:private payload-cache-converter-source (payload-cache-source))

(def ^:private scalar-string-map-payload-cache-factory
  (delay
    (try (scheme/eval-string payload-cache-converter-source)
         (catch Throwable _ (constantly nil)))))

(defn load-scalar-string-map-payload-cache!
  "Internal: policy anchors and budget come from otel.any-value's defining
  namespace. Returns a fresh {:lookup :store!} pair with independent cache
  state per call, or nil when the optional native capability is unavailable."
  [bindings max-insertions max-chars]
  (when-let [dispatch (@scalar-string-map-payload-cache-factory
                       bindings max-insertions max-chars)]
    {:lookup (fn [value] (dispatch 0 value nil nil))
     :store! (fn [value result weight] (dispatch 1 value result weight))}))
