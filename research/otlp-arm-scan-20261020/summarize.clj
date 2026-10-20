(require '[clojure.edn :as edn] '[clojure.java.io :as io])

(let [root (or (first *command-line-args*) "research/otlp-arm-scan-20261020")]
  (doseq [[order arm] [["forward" "baseline"] ["forward" "candidate"]
                      ["reverse" "candidate"] ["reverse" "baseline"]]]
    (with-open [r (java.io.PushbackReader. (io/reader (str root "/" order "/" arm ".edn")))]
      (let [metadata (edn/read r)
            results [(edn/read r) (edn/read r)]]
        (assert (= :preparsed-otlp-to-sdk-only (:scope metadata)))
        (assert (= 512 (:spans metadata)))
        (assert (= ::eof (edn/read {:eof ::eof} r)))
        (doseq [result results]
          (assert (= 12 (count (:samples result))))
          (prn order arm (select-keys result [:shape :p50-ms :p90-ms :p95-ms :p99-ms
                                              :estimated-mean-bytes-per-span])))))))
