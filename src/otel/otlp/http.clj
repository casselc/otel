(ns otel.otlp.http
  "The OTLP transport: an HTTP POST, and the endpoint parsing around it.

  The request itself goes through casselc/http-client (clj-http-lite on jolt),
  which carries TLS via the system OpenSSL. That is the whole reason this
  namespace is thin: an exporter has no business owning a socket, a TLS
  handshake, or HTTP framing, and an earlier version of this file that did own
  them could only speak cleartext.

  `:throw-exceptions false` is essential rather than incidental — the exporter
  has to *see* a 429 or a 503 to decide whether to retry, and an exception would
  turn a retryable response into a dropped batch."
  (:require [clojure.string :as str]
            [jolt.http-client :as http]))

(defn parse-url
  "Split an http(s) URL into {:scheme :host :port :path}. Used to resolve and
  validate endpoints; the request itself takes the URL whole."
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
  "Whether this transport can reach `url`. Both http and https are supported;
  https needs the system OpenSSL, which jolt-crypto declares."
  [url]
  (contains? #{"http" "https"} (:scheme (parse-url url))))

(defn post
  "POST `body` (a string or byte array) to `url`. Returns {:status :body
  :retry-after}. Throws only when the request could not be made at all — a
  connection failure or a DNS miss — never for an unhappy status code.

  `:insecure?` skips certificate verification. It exists for a collector using a
  self-signed cert on a trusted network and should not be used across one."
  [url body {:keys [headers timeout-ms insecure?] :or {timeout-ms 10000}}]
  (let [resp (http/post url {:body body
                             :headers headers
                             :socket-timeout timeout-ms
                             :connection-timeout timeout-ms
                             :insecure? (boolean insecure?)
                             :throw-exceptions false})
        ;; clj-http-lite lower-cases response header names.
        hdrs (:headers resp)]
    {:status (:status resp)
     :body (:body resp)
     :retry-after (or (get hdrs "retry-after") (get hdrs "Retry-After"))}))
