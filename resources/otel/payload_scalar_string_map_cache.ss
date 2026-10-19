;; Policy anchors are supplied by the defining SDK namespace; this module
;; never samples them from caller input and does not reimplement HAMT/sorted
;; -map structural conversion. It only attests the identity of an already
;; -admitted source value, caches the already-converted result keyed by a
;; weak reference to that identity, and hands back a FRESH make-pmap wrapper
;; sharing the cached root on both the first success and any later hit.
;; Admission/conversion itself stays owned by the original
;; try-scalar-string-map / try-sorted-scalar-string-map helpers on every
;; miss; this module never calls them and never invokes a table operation or
;; comparator. The weak-eq table holds no strong reference to a source map's
;; root/tree pointer once that pointer becomes otherwise unreachable, so a
;; source map's own metadata is never retained through this cache.
(lambda (bindings max-insertions max-chars)
  (let ((tag (keyword "jolt" "type")) (sorted-tag (keyword "jolt" "sorted-map"))
        (ops-key (keyword #f "ops")) (tree-key (keyword #f "tree"))
        (count-key (keyword #f "cnt"))
        (table (make-weak-eq-hashtable))
        (insertions 0) (retained 0))
    (define (same-policy?)
      (let loop ((i 0))
        (or (= i (pvec-count bindings))
            (let ((entry (pvec-nth! bindings i)))
              (and (eq? (var-cell-deref (pvec-nth! entry 0)) (pvec-nth! entry 1))
                   (loop (+ i 1)))))))
    ;; Returns (key . stamp) for a supported shape, or #f. Reads opaque
    ;; identity fields only; never iterates entries or calls a table op.
    (define (stamp-of m)
      (cond
        ((pmap? m) (cons (pmap-root m) (vector 'hamt (pmap-cnt m))))
        ((and (htable? m) (eq? (jolt-ref-get m tag) sorted-tag))
         (cons (jolt-ref-get m tree-key)
               (vector 'sorted (jolt-ref-get m ops-key) (jolt-ref-get m count-key))))
        (else #f)))
    (define (stamp-matches? stamp live)
      (case (vector-ref stamp 0)
        ((hamt) (and (pmap? live) (eqv? (vector-ref stamp 1) (pmap-cnt live))))
        ((sorted) (and (htable? live) (eq? (jolt-ref-get live tag) sorted-tag)
                       (eq? (vector-ref stamp 1) (jolt-ref-get live ops-key))
                       (eqv? (vector-ref stamp 2) (jolt-ref-get live count-key))))
        (else #f)))
    (define (lookup m)
      (and (same-policy?)
           (let ((ks (stamp-of m)))
             (and ks
                  (let ((entry (hashtable-ref table (car ks) #f)))
                    (and entry (stamp-matches? (vector-ref entry 0) m)
                         (same-policy?)
                         (make-pmap (vector-ref entry 1) (vector-ref entry 2))))))))
    (define (store! m result weight)
      (and (same-policy?) (pmap? result)
           (let ((ks (stamp-of m)))
             (when (and ks (< insertions max-insertions)
                        (<= (+ retained weight) max-chars))
               (hashtable-set! table (car ks)
                 (vector (cdr ks) (pmap-root result) (pmap-cnt result)))
               (set! insertions (+ insertions 1))
               (set! retained (+ retained weight)))
             (and (same-policy?) (make-pmap (pmap-root result) (pmap-cnt result))))))
    (lambda (mode a b c)
      (if (eqv? mode 0) (lookup a) (store! a b c)))))
