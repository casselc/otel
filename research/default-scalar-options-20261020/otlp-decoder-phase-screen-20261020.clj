(require '[oscope.socket-ingest :as socket]
         '[clojure.java.io :as io]
         '[otel.otlp.trace-decode :as decode]
         '[jolt.host :as host])

(defn counters []
  {:heap (host/bytes-allocated) :collected (host/gc-bytes)
   :cpu (host/cpu-nanos) :gc (host/gc-real-nanos)})
(defn allocation [before after]
  (- (+ (:heap after) (:collected after)) (+ (:heap before) (:collected before))))

(let [base (socket/payload 512 1800000000000000000)
      alternate (update-in base ["resourceSpans" 0 "scopeSpans" 0 "spans"]
                  (fn [spans]
                    (mapv (fn [index span]
                            (assoc span "traceId" (str "abcdef01" (socket/hex 24 (inc index)))
                                        "spanId" (str "abcdef01" (socket/hex 8 (+ 100000 index)))))
                          (range (count spans)) spans)))
      inputs [[:padded base] [:nonzero-prefix alternate]]
      samples (atom {})]
  (prn {:runtime (host/scheme-version) :machine (host/machine-type)
        :decoder-source (str (io/resource "otel/otlp/trace_decode.clj"))
        :normalizer-source (str (io/resource "otel/any_value.clj"))
        :scope :preparsed-otlp-to-sdk-only :spans 512 :warmups 2 :samples 12})
  (doseq [round (range 14)
          [shape input] (if (even? round) inputs (reverse inputs))]
    (let [before (counters) start (System/nanoTime)
          result (decode/decode-request input)
          nanos (- (System/nanoTime) start) after (counters)]
      (assert (= 512 (count (:spans result))))
      (assert (zero? (:rejected-spans result))) (assert (empty? (:errors result)))
      (assert (false? (get-in result [:spans 1 :attributes "active"])))
      (assert (= (get-in input ["resourceSpans" 0 "scopeSpans" 0 "spans" 0 "traceId"])
                 (get-in result [:spans 0 :span-context :trace-id])))
      (when (>= round 2)
        (swap! samples update shape (fnil conj [])
               {:ms (/ nanos 1e6) :cpu-ms (/ (- (:cpu after) (:cpu before)) 1e6)
                :gc-ms (/ (- (:gc after) (:gc before)) 1e6)
                :estimated-bytes-per-span (/ (double (allocation before after)) 512)}))))
  (doseq [[shape _] inputs]
    (let [raw (get @samples shape) ordered (vec (sort (map :ms raw)))
          q (fn [p] (nth ordered (dec (long (Math/ceil (* p (count ordered)))))))]
      (prn {:shape shape :samples raw :p50-ms (q 0.5) :p90-ms (q 0.9)
            :p95-ms (q 0.95) :p99-ms (q 0.99)
            :estimated-mean-bytes-per-span
            (/ (reduce + (map :estimated-bytes-per-span raw)) (count raw))}))))
