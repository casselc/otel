(ns otel.dependency-resolution-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [jolt.process :as process]))

(def ^:private expected-resolutions
  [{:coordinate "io.github.casselc/http-client"
    :repository "https___github.com_casselc_http-client.git"
    :sha "a42592a690000fcb749f608a94053695e467c836"}
   {:coordinate "jolt-lang/jolt-crypto"
    :repository "https___github.com_casselc_jolt-crypto.git"
    :sha "8bd234142d56dd75d36d58065a311f29fa08611e"}
   {:coordinate "io.github.chucklehead-dev/jolt-hegel"
    :repository "https___github.com_chucklehead-dev_jolt-hegel.git"
    :sha "b214f769983211431c74e427f0f35553cfba7b34"}])

(defn- resolved-classpath []
  (let [child (process/process
               ["jolt" "-Srepro" "-A:test" "-Spath"]
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
            (str coordinate " did not resolve from " repository " at " sha))))))
