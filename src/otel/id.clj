(ns otel.id
  "Trace and span identifier generation.

  Per the W3C Trace Context spec a trace id is 16 random bytes and a span id is 8,
  each carried on the wire as lowercase hex with no separators — 32 and 16
  characters respectively. The all-zero value of each is reserved to mean
  \"absent\" and is the only value that is invalid; generation must never produce
  it.

  These ids need to be unpredictable enough to avoid collisions across a fleet,
  but they are not a security boundary — nothing authenticates or authorizes on a
  trace id — so Chez's `random` (reached through clojure.core/rand-int) is the
  right tool and a CSPRNG would only cost throughput on a per-span path.")

(def invalid-trace-id "00000000000000000000000000000000")
(def invalid-span-id  "0000000000000000")

(def ^:private hex-chars "0123456789abcdef")

(defn- hex-digits
  "A string of `n` random lowercase hex digits."
  [n]
  (let [sb (StringBuilder.)]
    (dotimes [_ n]
      (.append sb (.charAt hex-chars (rand-int 16))))
    (.toString sb)))

(defn- hex-string?
  "True when `s` is exactly `n` lowercase hex characters."
  [s n]
  (and (string? s)
       (= n (count s))
       (every? (fn [c] (>= (.indexOf hex-chars (str c)) 0)) s)))

(defn valid-trace-id?
  "True for 32 lowercase hex characters other than the reserved all-zero id."
  [s]
  (and (hex-string? s 32) (not= s invalid-trace-id)))

(defn valid-span-id?
  "True for 16 lowercase hex characters other than the reserved all-zero id."
  [s]
  (and (hex-string? s 16) (not= s invalid-span-id)))

(defn trace-id
  "A fresh random trace id: 32 lowercase hex characters, never all-zero."
  []
  ;; The retry can only fire on a 1-in-16^32 draw, but the spec makes all-zero
  ;; mean \"no trace\", so emitting it would silently detach the span from its trace.
  (loop []
    (let [t (hex-digits 32)]
      (if (= t invalid-trace-id) (recur) t))))

(defn span-id
  "A fresh random span id: 16 lowercase hex characters, never all-zero."
  []
  (loop []
    (let [s (hex-digits 16)]
      (if (= s invalid-span-id) (recur) s))))
