(ns otel.otlp.http
  "A minimal HTTP/1.1 client, sized for posting OTLP payloads.

  Chez has no socket API and jolt's own HTTPS client is private to dependency
  resolution, so the transport is built here directly on the BSD socket calls
  through jolt's FFI — the same approach jolt.mvn-http takes.

  Two deliberate simplifications. Every request sends `Connection: close` and
  reads the response until EOF, which removes keep-alive and chunked framing
  from the problem entirely; for an exporter that posts a batch every few seconds
  the extra handshake is irrelevant next to not having to parse two more framing
  layers correctly. And only cleartext HTTP is implemented — the overwhelmingly
  common OTLP deployment is a collector on localhost or a sidecar. See
  `supports-scheme?`: an https endpoint is rejected up front with a clear error
  rather than failing obscurely at connect time."
  (:require [clojure.string :as str]
            [jolt.ffi :as ffi]))

;; --- socket layer -----------------------------------------------------------

(def ^:private os-name (str/lower-case (or (System/getProperty "os.name") "")))
(def ^:private macos? (str/includes? os-name "mac"))
(def ^:private windows? (str/includes? os-name "windows"))

(ffi/defcfn c-socket       "socket"       [:int :int :int] :int)
(ffi/defcfn c-connect      "connect"      [:int :pointer :int] :int :blocking)
(ffi/defcfn c-close        "close"        [:int] :int)
(ffi/defcfn c-closesocket  "closesocket"  [:int] :int)
(ffi/defcfn c-recv         "recv"         [:int :pointer :size_t :int] :ssize_t :blocking)
(ffi/defcfn c-send         "send"         [:int :pointer :size_t :int] :ssize_t :blocking)
(ffi/defcfn c-getaddrinfo  "getaddrinfo"  [:pointer :pointer :pointer :pointer] :int :blocking)
(ffi/defcfn c-freeaddrinfo "freeaddrinfo" [:pointer] :void)
(ffi/defcfn c-setsockopt   "setsockopt"   [:int :int :int :pointer :int] :int)

;; SO_RCVTIMEO/SO_SNDTIMEO bound every blocking call, so an unreachable or wedged
;; collector cannot stall the exporter thread forever.
(def ^:private sol-socket  (if (or macos? windows?) 0xffff 1))
(def ^:private so-rcvtimeo (if (or macos? windows?) 0x1006 20))
(def ^:private so-sndtimeo (if (or macos? windows?) 0x1005 21))

;; struct addrinfo offsets: macOS and Win64 place ai_addr at 32, Linux packs it at 24.
(def ^:private O-ai-family 4)
(def ^:private O-ai-socktype 8)
(def ^:private O-ai-protocol 12)
(def ^:private O-ai-addrlen 16)
(def ^:private O-ai-addr (if (or macos? windows?) 32 24))
(def ^:private O-ai-next 40)

(defn- set-timeouts! [fd ms]
  (if windows?
    (let [buf (ffi/alloc 4)]
      (try (ffi/write buf :int 0 ms)
           (c-setsockopt fd sol-socket so-rcvtimeo buf 4)
           (c-setsockopt fd sol-socket so-sndtimeo buf 4)
           (finally (ffi/free buf))))
    (let [tv (ffi/alloc 16)]
      (try (ffi/write tv :long 0 (quot ms 1000))
           (ffi/write tv :long 8 (* (rem ms 1000) 1000))
           (c-setsockopt fd sol-socket so-rcvtimeo tv 16)
           (c-setsockopt fd sol-socket so-sndtimeo tv 16)
           (finally (ffi/free tv))))))

(defn- close-sock [fd]
  (if windows? (c-closesocket fd) (c-close fd))
  nil)

(defn- connect-socket
  "Resolve host:port and return a connected TCP socket fd."
  [host port timeout-ms]
  (let [node (ffi/string->ptr (str host))
        service (ffi/string->ptr (str port))
        respp (ffi/alloc (ffi/sizeof :pointer))
        hints (ffi/alloc 48)]
    (dotimes [i 48] (ffi/write hints :uint8 i 0))
    ;; SOCK_STREAM, else getaddrinfo also returns UDP entries and connect()
    ;; spuriously succeeds on a datagram socket.
    (ffi/write hints :int O-ai-socktype 1)
    (try
      (when-not (zero? (c-getaddrinfo node service hints respp))
        (throw (ex-info (str "otlp: cannot resolve " host) {:host host})))
      (let [res (ffi/read respp :pointer)]
        (try
          (loop [ai res]
            (if (ffi/null? ai)
              (throw (ex-info (str "otlp: cannot connect to " host ":" port)
                              {:host host :port port}))
              (let [fd (c-socket (ffi/read ai :int O-ai-family)
                                 (ffi/read ai :int O-ai-socktype)
                                 (ffi/read ai :int O-ai-protocol))]
                (cond
                  (neg? fd) (recur (ffi/read ai :pointer O-ai-next))
                  (zero? (c-connect fd (ffi/read ai :pointer O-ai-addr)
                                    (ffi/read ai :int O-ai-addrlen)))
                  (do (set-timeouts! fd timeout-ms) fd)
                  :else (do (close-sock fd) (recur (ffi/read ai :pointer O-ai-next)))))))
          (finally (c-freeaddrinfo res))))
      (finally
        (ffi/free node) (ffi/free service) (ffi/free respp) (ffi/free hints)))))

(defn- send-all! [fd ^bytes data]
  (let [n (alength data)
        buf (ffi/alloc (max 1 n))]
    (try
      (ffi/write-array buf data)
      (loop [off 0]
        (when (< off n)
          (let [sent (c-send fd (+ buf off) (- n off) 0)]
            (if (pos? sent)
              (recur (+ off sent))
              (throw (ex-info "otlp: send failed" {}))))))
      (finally (ffi/free buf)))))

(def ^:private recv-bufsize 65536)

(defn- recv-all
  "Read the whole response, until the peer closes. Returns a string."
  [fd]
  (let [buf (ffi/alloc recv-bufsize)
        out (java.io.ByteArrayOutputStream.)]
    (try
      (loop []
        (let [got (c-recv fd buf recv-bufsize 0)]
          (cond
            (pos? got) (do (.write out (ffi/read-array buf got) 0 got) (recur))
            ;; 0 is a clean EOF; negative is a timeout or error, and whatever was
            ;; already read is still worth parsing for a status line.
            :else nil)))
      (finally (ffi/free buf)))
    (String. (.toByteArray out) "UTF-8")))

;; --- URL handling -----------------------------------------------------------

(defn parse-url
  "Split an http URL into {:scheme :host :port :path}."
  [url]
  (let [[_ scheme rest*] (re-matches #"(?i)^(https?)://(.*)$" (str url))]
    (when-not scheme
      (throw (ex-info (str "otlp: not an http(s) url: " url) {:url url})))
    (let [scheme (str/lower-case scheme)
          slash (str/index-of rest* "/")
          authority (if slash (subs rest* 0 slash) rest*)
          path (if slash (subs rest* slash) "/")
          colon (str/last-index-of authority ":")
          ;; Only a colon after the last ']' is a port, so an IPv6 literal's own
          ;; colons are not mistaken for one.
          bracket (str/last-index-of authority "]")
          port-colon (when (and colon (or (nil? bracket) (> colon bracket))) colon)]
      {:scheme scheme
       :host (if port-colon (subs authority 0 port-colon) authority)
       :port (if port-colon
               (Long/parseLong (subs authority (inc port-colon)))
               (if (= "https" scheme) 443 80))
       :path path})))

(defn supports-scheme?
  "Whether this transport can reach `url`. Only cleartext http is implemented."
  [url]
  (= "http" (:scheme (parse-url url))))

;; --- request/response -------------------------------------------------------

(defn- parse-response
  "Pull the status code and body out of a raw HTTP response."
  [raw]
  (let [split (str/index-of raw "\r\n\r\n")
        head (if split (subs raw 0 split) raw)
        body (if split (subs raw (+ split 4)) "")
        status (when-let [m (re-find #"^HTTP/\d\.\d\s+(\d{3})" head)]
                 (Long/parseLong (second m)))
        header (fn [n]
                 (second (re-find (re-pattern (str "(?i)\r?\n" n ":\\s*([^\r\n]*)")) (str "\n" head))))]
    {:status status
     :body body
     :retry-after (header "Retry-After")
     :headers head}))

(defn post
  "POST `body` (a byte array) to `url`. Returns {:status :body}, or throws when
  the request could not be made at all."
  [url body {:keys [headers timeout-ms] :or {timeout-ms 10000}}]
  (let [{:keys [scheme host port path]} (parse-url url)]
    (when-not (= "http" scheme)
      (throw (ex-info (str "otlp: the " scheme " scheme is not supported by this transport; "
                           "use an http:// endpoint (a local collector or sidecar)")
                      {:url url :scheme scheme})))
    (let [fd (connect-socket host port timeout-ms)]
      (try
        (let [head (str "POST " path " HTTP/1.1\r\n"
                        "Host: " host ":" port "\r\n"
                        "Content-Length: " (alength body) "\r\n"
                        ;; close-delimited: no keep-alive or chunked framing to parse
                        "Connection: close\r\n"
                        (str/join "" (map (fn [[k v]] (str k ": " v "\r\n")) headers))
                        "\r\n")
              out (java.io.ByteArrayOutputStream.)]
          (.write out (.getBytes head "UTF-8"))
          (.write out body)
          (send-all! fd (.toByteArray out))
          (parse-response (recv-all fd)))
        (finally (close-sock fd))))))
