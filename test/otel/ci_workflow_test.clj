(ns otel.ci-workflow-test
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]))

(def ^:private workflow-path ".github/workflows/tests.yml")
(def ^:private toolchain-path "resources/otel/ci-toolchain.edn")

(defn- workflow-matches-toolchain?
  [toolchain workflow]
  (let [{:keys [version source-commit installer-sha256
                linux-x86-64-archive-sha256]} (:jolt toolchain)
        checkout-revision (get-in toolchain [:checkout :revision])]
    (and
      (every? #(str/includes? workflow %)
              [(str "JOLT_VERSION: " version)
               (str "JOLT_SOURCE_COMMIT: " source-commit)
               (str "JOLT_INSTALLER_SHA256: " installer-sha256)
               (str "JOLT_ARCHIVE_SHA256: " linux-x86-64-archive-sha256)])
      (str/includes? workflow
                     (str "https://raw.githubusercontent.com/jolt-lang/jolt/"
                          "$JOLT_SOURCE_COMMIT/install"))
      (str/includes? workflow "--checksum \"$JOLT_ARCHIVE_SHA256\"")
      (str/includes? workflow
                     (str "uses: actions/checkout@" checkout-revision))
      (not (str/includes? workflow "actions/checkout@v"))
      (not (str/includes? workflow
                          "raw.githubusercontent.com/jolt-lang/jolt/v0.8.3")))))

(defn- least-privilege-reproducible-workflow?
  [workflow]
  (and (str/includes? workflow "permissions:\n  contents: read")
       (not (str/includes? workflow "permissions: write-all"))
       (not (re-find #"(?m)^\s+[^#\s]+:\s+write\s*$" workflow))
       (str/includes? workflow "persist-credentials: false")
       (str/includes? workflow "timeout-minutes: 15")
       (str/includes? workflow "jolt -Srepro -Sdescribe")
       (str/includes? workflow "jolt -Srepro -A:test -m hegel.install")
       (str/includes? workflow
                      "jolt -Srepro -M:test -m otel.test-runner")
       (not (str/includes? workflow "joltc"))))

(deftest hosted-workflow-is-pinned-and-reproducible
  (let [toolchain (edn/read-string (slurp toolchain-path))
        workflow (slurp workflow-path)]
    (is (workflow-matches-toolchain? toolchain workflow))
    (is (least-privilege-reproducible-workflow? workflow))
    (testing "pin, action, and reproducibility drift each turn the guard red"
      (is (not (workflow-matches-toolchain?
                 (assoc-in toolchain [:jolt :linux-x86-64-archive-sha256]
                           (apply str (repeat 64 "0")))
                 workflow)))
      (is (not (workflow-matches-toolchain?
                 (assoc-in toolchain [:checkout :revision] "v4")
                 workflow)))
      (is (not (least-privilege-reproducible-workflow?
                 (str/replace workflow "jolt -Srepro -M:test"
                              "jolt -M:test"))))
      (is (not (least-privilege-reproducible-workflow?
                 (str/replace workflow
                              "permissions:\n  contents: read"
                              "permissions: write-all"))))
      (is (not (least-privilege-reproducible-workflow?
                 (str/replace workflow
                              "          persist-credentials: false\n"
                              "")))))))
