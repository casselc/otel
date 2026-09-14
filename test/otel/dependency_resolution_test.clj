(ns otel.dependency-resolution-test
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [jolt.process :as process]))

(def ^:private expected-resolutions
  [{:coordinate "io.github.casselc/http-client"
    :repository "https___github.com_casselc_http-client.git"
    :sha "eab6b78d5957f88690faf6768360572a3f185341"}
   {:coordinate "io.github.chucklehead-dev/jolt-hegel"
    :repository "https___github.com_chucklehead-dev_jolt-hegel.git"
    :sha "b214f769983211431c74e427f0f35553cfba7b34"}])

(def ^:private crypto-sha
  "44da69bad08a2fd7631bd4061e3fb53938dafff6")

(def ^:private http-sha
  "eab6b78d5957f88690faf6768360572a3f185341")

(def ^:private divergent-upstream-sha
  "ccce992d6e3d0035a5ffd1d4364cdb39df4af2f0")

(def ^:private expected-crypto-dependency
  {:git/url "https://github.com/jolt-lang/jolt-crypto"
   :git/sha crypto-sha})

(def ^:private canonical-crypto-suffixes
  ;; A coordinate whose name agrees with its repository can use Clojure's
  ;; ~/.gitlibs layout locally. An explicit JOLT_GITLIBS_DIR, as used in hosted
  ;; CI, uses Jolt's URL-derived checkout key instead.
  [(str "/jolt-lang/jolt-crypto/" crypto-sha)
   (str "/https___github.com_jolt-lang_jolt-crypto/" crypto-sha)])

(defn- crypto-checkouts
  [classpath]
  (->> (str/split classpath #":")
       (filter #(str/includes? % "jolt-crypto"))
       (map #(str/replace % #"/(?:src|resources)$" ""))
       distinct
       vec))

(defn- one-canonical-crypto?
  [entries]
  (and (= 1 (count entries))
       (some #(str/ends-with? (first entries) %)
             canonical-crypto-suffixes)))

(def ^:private http-source-files
  ["jolt/http_client.clj"
   "jolt/http/core.clj"
   "jolt/http/net.clj"
   "jolt/http/platform.clj"
   "jolt/http/tls.clj"
   "jolt/http/zlib.clj"])

(defn- roots-providing [classpath relative-path]
  (->> (str/split classpath #":")
       (filter #(.exists (io/file % relative-path)))
       distinct
       vec))

(defn- one-shared-provider? [roots-by-source]
  (and (every? #(= 1 (count %)) (vals roots-by-source))
       (= 1 (count (distinct (map first (vals roots-by-source)))))))

(defn- resolved-classpath []
  (let [wrapper (System/getenv "OTEL_TEST_JOLT_WRAPPER")
        command (cond-> []
                  wrapper (conj wrapper "jolt")
                  (not wrapper) (conj "jolt")
                  true (into ["-Srepro" "-A:test:consumer-resolution" "-Spath"]))
        child (process/process
               command
               {:out :string :err :string})
        result (deref child 60000 ::timeout)]
    (when (= ::timeout result)
      (try (process/destroy-tree child) (catch Throwable _ nil)))
    (is (map? result) "dependency-resolution child must complete")
    (when (map? result)
      (is (zero? (:exit result))
          (str "jolt -Spath failed: " (:err result))))
    (when (and (map? result) (zero? (:exit result)))
      (str/replace (str (:out result)) "\\" "/"))))

(deftest reconciled-dependencies-resolve-to-exact-reviewed-revisions
  (when-let [classpath (resolved-classpath)]
    (doseq [{:keys [coordinate repository sha]} expected-resolutions]
      (testing coordinate
        (is (str/includes? classpath (str "/" repository "/" sha "/"))
            (str coordinate " did not resolve from " repository " at " sha))))
    (is (one-canonical-crypto? (crypto-checkouts classpath))
        (str "expected one canonical jolt-crypto at " crypto-sha
             ", selected " (pr-str (crypto-checkouts classpath))))
    (let [http-entries (->> (str/split classpath #":")
                            (filter #(str/includes? % "http-client"))
                            (map #(str/replace % #"/(?:src|resources)$" ""))
                            distinct
                            vec)
          roots-by-source (into {}
                                (map (fn [source]
                                       [source (roots-providing classpath source)]))
                                http-source-files)]
      (is (= 1 (count http-entries))
          (str "Samizdat-style graph selected multiple HTTP roots: "
               (pr-str http-entries)))
      (is (str/includes? (first http-entries) http-sha)
          (str "selected HTTP root is not the converged revision: "
               (first http-entries)))
      (is (not (str/includes? classpath divergent-upstream-sha))
          "the divergent upstream v0.0.8 checkout must not remain selected")
      (is (one-shared-provider? roots-by-source)
          (str "jolt.http namespaces do not share exactly one source root: "
               (pr-str roots-by-source))))))

(deftest selected-provider-manifest-is-discoverable
  (let [resource-name "META-INF/jolt/aspects/http-client-core.edn"
        resource (io/resource resource-name)
        manifest (some-> resource slurp edn/read-string)]
    (is (some? resource)
        "the selected provider must put its aspect manifest on the classpath")
    (is (= :http-client.core/request
           (get-in manifest [:aspects 0 :id])))
    (is (= 'clj-http.lite.core/request
           (get-in manifest [:aspects 0 :match :entry])))
    (is (= 1 (get-in manifest [:aspects 0 :expect :matches])))))

(deftest provider-cardinality-guard-rejects-the-old-mixed-graph
  (let [one-root (zipmap http-source-files (repeat ["/provider/src"]))
        duplicate (assoc one-root "jolt/http/net.clj"
                         ["/provider/src" "/upstream-v0.0.8/src"])
        split-root (assoc one-root "jolt/http/tls.clj"
                          ["/upstream-v0.0.8/src"])]
    (is (one-shared-provider? one-root))
    (is (not (one-shared-provider? duplicate)))
    (is (not (one-shared-provider? split-root)))))

(deftest direct-crypto-declaration-is-canonical
  (let [declared (get-in (edn/read-string (slurp "deps.edn"))
                         [:deps 'jolt-lang/jolt-crypto])]
    (is (= expected-crypto-dependency declared))
    (is (not= expected-crypto-dependency
              (assoc declared :git/url
                     "https://github.com/casselc/jolt-crypto.git")))
    (is (not= expected-crypto-dependency
              (assoc declared :git/sha
                     "0000000000000000000000000000000000000000")))))

(deftest canonical-crypto-guard-covers-cache-layouts-and-mutations
  (let [[local-suffix hosted-suffix] canonical-crypto-suffixes
        local (str "/home/test/.gitlibs/libs" local-suffix)
        hosted (str "/work/.jolt-gitlibs" hosted-suffix)
        fork (str "/work/.jolt-gitlibs/"
                  "https___github.com_casselc_jolt-crypto.git/"
                  crypto-sha)
        wrong-sha (str "/home/test/.gitlibs/libs/jolt-lang/jolt-crypto/"
                       "0000000000000000000000000000000000000000")]
    (testing "both known canonical cache encodings are accepted"
      (is (one-canonical-crypto? [local]))
      (is (one-canonical-crypto? [hosted])))
    (testing "fork identity, wrong revision, and duplicate identities are red"
      (is (not (one-canonical-crypto? [])))
      (is (not (one-canonical-crypto? [fork])))
      (is (not (one-canonical-crypto? [wrong-sha])))
      (is (not (one-canonical-crypto? [local hosted]))))))
