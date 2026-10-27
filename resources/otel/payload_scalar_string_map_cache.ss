;; Payload-local weak root cache. Immutable source roots and ordinary runtime
;; internals are trusted; source headers must be stable throughout each call.
;; Admission stays in the defining SDK's original callback-free converters.
;; Runtime-owned folding reads only their admitted immutable output. No source
;; wrapper/metadata, comparator or caller operation is retained or invoked.
(lambda (bindings max-insertions max-chars)
  (let ((tag (keyword "jolt" "type")) (sorted-tag (keyword "jolt" "sorted-map"))
        (ops-key (keyword #f "ops")) (tree-key (keyword #f "tree"))
        (count-key (keyword #f "cnt"))
        (table (make-weak-eq-hashtable))
        (anchors (make-vector (* 2 (pvec-count bindings))))
        (insertions 0) (retained 0))
    (define (same-policy?)
      (let loop ((i 0))
        (or (= i (vector-length anchors))
            (and (eq? (var-cell-deref (vector-ref anchors i))
                      (vector-ref anchors (+ i 1)))
                 (loop (+ i 2))))))
    (define (fresh entry)
      (and (same-policy?) (make-pmap (vector-ref entry 3) (vector-ref entry 4))))
    ;; No cons/vector stamp allocation on either hits or ordinary misses.
    (define (lookup m)
      (and (same-policy?)
           (cond
             ((pmap? m)
              (let ((entry (hashtable-ref table (pmap-root m) #f)))
                (and entry (eq? (vector-ref entry 0) 'hamt)
                     (eqv? (vector-ref entry 2) (pmap-cnt m)) (fresh entry))))
             ((and (htable? m) (eq? (jolt-ref-get m tag) sorted-tag))
              (let ((entry (hashtable-ref table (jolt-ref-get m tree-key) #f)))
                (and entry (eq? (vector-ref entry 0) 'sorted)
                     ;; A tree/ops/count match does not attest the wrapper's
                     ;; representation. Do not replay an admitted scoll result
                     ;; through a forged legacy tagged htable sharing its tree.
                     (eq? (vector-ref entry 5) (record-rtd m))
                     (eq? (vector-ref entry 1) (jolt-ref-get m ops-key))
                     (eqv? (vector-ref entry 2) (jolt-ref-get m count-key))
                     (fresh entry))))
             (else #f))))
    (define (weight result)
      (pmap-fold-seq-order result
        (lambda (key value total)
          (+ total (string-length key) (string-length value))) 0))
    (define (store! m result)
      (and (same-policy?) (pmap? result)
           (begin
             (when (< insertions max-insertions)
               (let ((charge (weight result)))
                 (when (<= (+ retained charge) max-chars)
                   (cond
                     ((pmap? m)
                      (hashtable-set! table (pmap-root m)
                        (vector 'hamt #f (pmap-cnt m) (pmap-root result) (pmap-cnt result))))
                     ((and (htable? m) (eq? (jolt-ref-get m tag) sorted-tag))
                      (hashtable-set! table (jolt-ref-get m tree-key)
                        (vector 'sorted (jolt-ref-get m ops-key) (jolt-ref-get m count-key)
                                (pmap-root result) (pmap-cnt result) (record-rtd m)))))
                   (set! insertions (+ insertions 1))
                   (set! retained (+ retained charge)))))
             ;; Original helpers already return a fresh metadata-free wrapper.
             ;; Store its root only; hits construct another fresh wrapper.
             (and (same-policy?) result))))
    ;; Decode immutable binding vectors once, but dereference every live cell
    ;; on each check. This is not a startup-only policy snapshot.
    (do ((i 0 (+ i 1))) ((= i (pvec-count bindings)))
      (let ((entry (pvec-nth! bindings i)))
        (vector-set! anchors (* i 2) (pvec-nth! entry 0))
        (vector-set! anchors (+ (* i 2) 1) (pvec-nth! entry 1))))
    (lambda (mode a b c)
      (case mode ((0) (lookup a)) ((1) (store! a b)) (else (same-policy?))))))
