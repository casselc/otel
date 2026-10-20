#!/usr/bin/env bash
set -euo pipefail
task_out=${1:-/home/chuck/ai-src/evidence/otlp-arm-scan-pair-20261020}
task_order=${2:-forward}
case "$task_order" in
  forward) task_arms=(baseline candidate);;
  reverse) task_arms=(candidate baseline);;
  *) exit 90;;
esac
test ! -e "$task_out"
task_bin=/home/chuck/.cache/jolt-stock-v0.8.23-codec/jolt-v0.8.23-x86_64-linux/jolt
task_wrapper=/home/chuck/ai-src/tools/jolt-with-chez-10.4.1
task_script=/home/chuck/ai-src/evidence/otlp-arm-scan-phase-screen-20261020.clj
task_base=/home/chuck/ai-src/worktrees/otel-default-scalar-main-20261020
task_candidate=/home/chuck/ai-src/worktrees/otel-otlp-arm-scan-20261020
export JOLT_AOT_CACHE=0 JOLT_NO_USER_DEPS=1
export JOLT_CHDB_LIB=/home/chuck/.cache/jolt-chdb/26.9.0/linux-amd64/libchdb.so
printf '%s  %s\n' 7bf7acae4b4bcb5c27b3651fa4a94d2d4e6a7fc43b5b821def2f8be3d6f00e94 "$task_bin" | sha256sum -c -
mkdir -p "$task_out"
sha256sum "$task_script" "$task_base/src/otel/any_value.clj" "$task_base/src/otel/otlp/any_value.clj" "$task_candidate/src/otel/any_value.clj" "$task_candidate/src/otel/otlp/any_value.clj" > "$task_out/sources.sha256"
cmp "$task_base/src/otel/any_value.clj" "$task_candidate/src/otel/any_value.clj"
cmp "$task_base/src/otel/otlp/trace_decode.clj" "$task_candidate/src/otel/otlp/trace_decode.clj"
for task_arm in "${task_arms[@]}"; do
  task_root=$task_base
  if [[ "$task_arm" == candidate ]]; then task_root=$task_candidate; fi
  task_deps='{:paths ["src" "benchmark"] :deps {io.github.casselc/otel {:local/root "'"$task_root"'"}}}'
  timeout --signal=TERM --kill-after=5s 180s "$task_wrapper" "$task_bin" -Srepro -Sdeps "$task_deps" "$task_script" > "$task_out/$task_arm.edn" 2> "$task_out/$task_arm.stderr"
  bb -e '(with-open [r (java.io.PushbackReader. (clojure.java.io/reader (first *command-line-args*)))] (let [m (clojure.edn/read r)] (doseq [k [:decoder-source :normalizer-source :wire-any-value-source]] (assert (clojure.string/starts-with? (get m k) (str "file:" (second *command-line-args*) "/"))))))' "$task_out/$task_arm.edn" "$task_root"
done
sha256sum -c "$task_out/sources.sha256" > "$task_out/source-after.log"
printf '%s\n' 'terminal0: source-checked decoder pair complete'
