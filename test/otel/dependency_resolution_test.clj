(ns otel.dependency-resolution-test
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [jolt.process :as process]))

(def ^:private expected-resolutions
  [{:coordinate "io.github.casselc/http-client"
    :repository "https___github.com_casselc_http-client.git"
    :sha "9cb5801e8c5929387715aa6713c33b2c21fd9a2a"}
   {:coordinate "io.github.chucklehead-dev/jolt-hegel"
    :repository "https___github.com_chucklehead-dev_jolt-hegel.git"
    :sha "b214f769983211431c74e427f0f35553cfba7b34"}])

(def ^:private crypto-sha
  "5effcc89a3258499a79a2a3d69edad9e7800d1bf")

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

(defn- resolved-classpath []
  (let [wrapper (System/getenv "OTEL_TEST_JOLT_WRAPPER")
        command (cond-> []
                  wrapper (conj wrapper "jolt")
                  (not wrapper) (conj "jolt")
                  true (into ["-Srepro" "-A:test" "-Spath"]))
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
             ", selected " (pr-str (crypto-checkouts classpath))))))

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
