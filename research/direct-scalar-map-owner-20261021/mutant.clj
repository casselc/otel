;; Independent process: patch the codec BEFORE its lazy backend is initialized.
;; Expected exit1 must be a real test failure, not parse/startup failure.
(require '[clojure.string :as str] '[jolt.codec.experimental :as codec])
(let [kind (first *command-line-args*) source @#'codec/json-source
      start (str/index-of source (if (= kind "unquoted")
                                  "(define (direct-scalar-entry!" "(define (ffc-json-direct-scalar-map-box"))
      end (str/index-of source (if (= kind "unquoted")
                                "    (define (emit!" ";; Explicit row view:") start)
      fragment (subs source start end)
      needle (case kind "unquoted" "(byte! 34) (integer! value) (byte! 34)"
                        "metadata" "(make-pmap (pmap-root m) (pmap-cnt m))")
      replacement (case kind "unquoted" "(integer! value)" "metadata" "m")]
  (assert (= 2 (count (str/split fragment (re-pattern (java.util.regex.Pattern/quote needle)) -1))))
  (with-redefs-fn {#'codec/json-source
                  (str (subs source 0 start) (str/replace fragment needle replacement) (subs source end))}
    #(do (require 'otel.direct-scalar-map-view-test)
         ((ns-resolve 'otel.direct-scalar-map-view-test '-main)))))
