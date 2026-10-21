(ns otel.hamt-scalar-view-bench
  "Controlled component screen, not confirmed Durable throughput."
  (:require [otel.any-value :as any] [jolt.host :as host] [jolt.scheme :as scheme]
            [jolt.codec.experimental :as codec] [jolt.codec.scalar-text.experimental :as shapes]))

(defn check! [x label] (when-not x (throw (ex-info "HAMT component check failed" {:label label}))))
(defn counters [] {:nanos (System/nanoTime) :cpu (host/cpu-nanos)
                  :heap (host/bytes-allocated) :gc-bytes (host/gc-bytes)
                  :gc-nanos (host/gc-real-nanos)})
(defn quantiles [xs]
  (let [v (vec (sort xs)) n (count v)]
    (mapv #(nth v (dec (long (Math/ceil (* % n))))) [0.5 0.9 0.95 0.99])))

(defn -main [output & [mode rows-text rounds-text]]
  (let [n (parse-long (or rows-text "5000")) rounds (parse-long (or rounds-text "16"))
        _ (check! (and output (.isAbsolute (java.io.File. output))
                       (contains? #{"source" "release"} mode)
                       (<= 1 n 10000) (<= 2 rounds 32)) :arguments)
        base (into {} (for [i (range 16)]
                        [(str "key-" i) (case (mod i 3) 0 "quote\"λ😀" 1 false i)]))
        rows (mapv #(assoc base "key-0" %) (range n))
        c (codec/open-codec {:flat-map-cache? true})
        folder (codec/native-map-folder c)
        box (codec/native-scalar-text-view-box)
        factories {false (shapes/native-view-plan-factory box false)
                   true (shapes/native-view-plan-factory box true)}
        same-bytes? (scheme/eval-string
                      "(lambda (a b) (bytevector=? (na-bytearray->bv a) (na-bytearray->bv b)))")
        encode (fn [enabled?]
                 (let [project (any/scalar-text-payload-projector
                                 folder {:shape-plan-factory (get factories enabled?)})]
                   (check! (ifn? project) :projector-present)
                   (codec/encode-prefixed-records c "" rows
                     (fn [row] (let [value (project row)] (check! value :admitted) [value]))
                     (* 16 1024 1024))))
        expected (encode false)
        completed (atom [])]
    ;; One process, fixed rows/runtime, disabled/enabled/enabled/disabled order.
    ;; Both arms use the NEW SDK; this is not an old-SDK comparison.
    (doseq [[arm enabled?] (map-indexed vector [false true true false])]
      (dotimes [_ 2] (check! (same-bytes? expected (encode enabled?)) :warmup-wire))
      (let [samples
            (mapv (fn [round]
                    (let [before (counters) bytes (encode enabled?) after (counters)
                          sample {:round round :rows n :nanos (- (:nanos after) (:nanos before))
                                  :cpu-nanos (- (:cpu after) (:cpu before))
                                  :gc-nanos (- (:gc-nanos after) (:gc-nanos before))
                                  :estimated-bytes (- (+ (:heap after) (:gc-bytes after))
                                                      (+ (:heap before) (:gc-bytes before)))}]
                      ;; Consume/compare complete output OUTSIDE measured section.
                      ;; Comparison copies are identical in both arms and can
                      ;; affect later GC; this remains a component diagnostic.
                      (check! (same-bytes? expected bytes) :whole-batch-wire)
                      sample)) (range rounds))
            summary {:arm arm :hamt-views? enabled? :samples samples
                     :p50-p90-p95-p99-ms (mapv #(/ % 1e6) (quantiles (map :nanos samples)))
                     :estimated-bytes-per-row (/ (reduce + (map :estimated-bytes samples))
                                                  (double (* n rounds)))}]
        (swap! completed conj summary)
        (spit output (pr-str {:scope :projection-json-component :mode mode
                              :batch-size n :rounds rounds :warmups-per-arm 2
                              :cpu-scope :current-os-thread :memory-scope :process-wide-nonatomic
                              :comparison-copies-outside-window true
                              :tail-qualified false :arms @completed}))
        (println :hamt-component (dissoc summary :samples))
        (flush)))))
