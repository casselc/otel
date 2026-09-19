#!/usr/bin/env bash
# Source-only contract tests for the exact-revision shutdown model selector.
# They neither invoke Quint nor run either 10k sampled campaign.
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
classifier="$repo_root/scripts/classify-shutdown-lifecycle-quint-paths.sh"

check() {
  local label=$1 expected_lifecycle=$2 expected_settlement=$3
  shift 3
  local actual
  actual=$("$classifier" "$@")
  [[ $(sed -n 's/^lifecycle=//p' <<<"$actual") == "$expected_lifecycle" ]] || {
    echo "$label: unexpected lifecycle decision" >&2; echo "$actual" >&2; exit 1;
  }
  [[ $(sed -n 's/^settlement=//p' <<<"$actual") == "$expected_settlement" ]] || {
    echo "$label: unexpected settlement decision" >&2; echo "$actual" >&2; exit 1;
  }
  echo "ok $label"
}

python3 - "$repo_root" <<'PY'
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
checker = (root / "scripts/check-shutdown-lifecycle-quint.sh").read_text()
classifier = (root / "scripts/classify-shutdown-lifecycle-quint-paths.sh").read_text()
fingerprinter = (root / "scripts/fingerprint-shutdown-lifecycle-quint-inputs.sh").read_text()
workflow = (root / ".github/workflows/shutdown-lifecycle-quint.yml").read_text()

for command in ("--deterministic", "--sample lifecycle|settlement", "--all"):
    assert command in checker, f"missing checker command {command}"
assert checker.count("--max-samples 10000") == 2, "sample count leaked into controls or was dropped"
assert "run_deterministic" in checker and "run_lifecycle_sample" in checker and "run_settlement_sample" in checker
for output in ("shutdownLifecycle.qnt", "shutdownLifecycleTest.qnt",
               "shutdownSettlement.qnt", "shutdownSettlementTest.qnt"):
    assert output in checker and output in fingerprinter, f"unclosed generated output {output}"
for shared in ("shutdown-lifecycle-quint.yml", "check-shutdown-lifecycle-quint.sh",
               "classify-shutdown-lifecycle-quint-paths.sh",
               "shutdown-lifecycle-model-path-classifier.sh"):
    assert shared in fingerprinter, f"shared effective input absent: {shared}"
assert "effective_document_only" in classifier and "compare_campaign lifecycle" in classifier
for path in ("src/otel/sdk.clj", "src/otel/sdk/lifecycle.clj", "src/otel/sdk/export.clj",
             "src/otel/sdk/logs.clj", "src/otel/sdk/metrics.clj",
             "test/otel/sdk/lifecycle_events.clj", "test/otel/sdk/lifecycle_test.clj"):
    assert path in classifier, f"correspondence closure omits {path}"
for control in ("force_both:", "MANUAL_FORCE_BOTH", "manual-force-both",
                "shutdown-model-classification.receipt", "sample-lifecycle",
                "sample-settlement", "deterministic-model-controls"):
    assert control in workflow, f"workflow control/receipt missing {control}"
def has_manual_force_both(source):
    return ("force_both:" in source and "MANUAL_FORCE_BOTH" in source and
            '[[ "$MANUAL_FORCE_BOTH" == true ]]' in source and
            "lifecycle=true\\nsettlement=true\\nreason=manual-force-both" in source)
assert has_manual_force_both(workflow), "manual force-both no longer selects both"
assert not has_manual_force_both(workflow.replace("MANUAL_FORCE_BOTH", "MANUAL_DISABLED")), \
    "manual dispatch mutation control is vacuous"

# Classification compares the immutable PR head (not GitHub's synthetic merge
# revision), so every executor must checkout and attest that same classifier
# output. This is deliberately structural: a checkout action's default ref on
# pull_request is the merge revision, and would silently invalidate a decision
# calculated from the head object.
classifier_ref = "ref: ${{ github.event.pull_request.head.sha || github.sha }}"
executor_ref = "ref: ${{ needs.classify-model-inputs.outputs.revision }}"
executor_attestation = 'test "$(git rev-parse HEAD)" = "${{ needs.classify-model-inputs.outputs.revision }}"'
assert classifier_ref in workflow, "classifier does not checkout its exact input revision"
assert "revision: ${{ steps.decision.outputs.revision }}" in workflow, "classifier does not publish execution revision"
assert "test \"$HEAD_SHA\" = \"$(git rev-parse HEAD)\"" in workflow, "classifier does not attest checkout revision"
assert "printf 'revision=%s\\n' \"$HEAD_SHA\" >> \"$GITHUB_OUTPUT\"" in workflow, "classifier revision output is not exact head"
for job in ("deterministic-model-controls", "sample-lifecycle", "sample-settlement"):
    start = workflow.index(f"  {job}:")
    following = re.search(r"\n  [a-z][a-z0-9-]*:", workflow[start + len(job) + 4:])
    end = len(workflow) if following is None else start + len(job) + 4 + following.start()
    body = workflow[start:end]
    assert executor_ref in body, f"{job} defaults to a synthetic merge checkout"
    assert executor_attestation in body, f"{job} does not attest classified checkout"
assert executor_ref not in workflow.replace(executor_ref, "", 3), "executor-ref count control is vacuous"
assert executor_attestation not in workflow.replace(executor_attestation, "", 3), "executor-attestation count control is vacuous"
print("ok static checker/fingerprint/workflow contract")
PY

# Cheap direct classification checks include the shared and correspondence
# closures. The doc itself is conservative without two exact revisions.
check "unrelated path" false false --paths README.md
check "shared checker input" true true --paths scripts/check-shutdown-lifecycle-quint.sh
check "lifecycle correspondence input" true true --paths src/otel/sdk/export.clj
check "document without provenance" true true --paths formal/quint/shutdown-lifecycle.md
check "missing provenance fails closed" true true --diff 0000000000000000000000000000000000000000 HEAD

fixture=$(mktemp -d)
cleanup() { rm -rf -- "$fixture"; }
trap cleanup EXIT HUP INT TERM
fixture_repo="$fixture/repo"
mkdir -p "$fixture_repo/.github/workflows" "$fixture_repo/formal/quint" "$fixture_repo/scripts" "$fixture_repo/test"
cp "$repo_root/formal/quint/shutdown-lifecycle.md" "$fixture_repo/formal/quint/"
cp "$repo_root/.github/workflows/shutdown-lifecycle-quint.yml" "$fixture_repo/.github/workflows/"
cp "$repo_root/scripts/check-shutdown-lifecycle-quint.sh" "$fixture_repo/scripts/"
cp "$repo_root/scripts/classify-shutdown-lifecycle-quint-paths.sh" "$fixture_repo/scripts/"
cp "$repo_root/scripts/fingerprint-shutdown-lifecycle-quint-inputs.sh" "$fixture_repo/scripts/"
cp "$repo_root/test/shutdown-lifecycle-model-path-classifier.sh" "$fixture_repo/test/"
git -C "$fixture_repo" init -q
git -C "$fixture_repo" config user.name "shutdown selector test"
git -C "$fixture_repo" config user.email "shutdown-selector@example.invalid"
git -C "$fixture_repo" add .
git -C "$fixture_repo" commit -qm base
base=$(git -C "$fixture_repo" rev-parse HEAD)

# Identical exact revisions never invoke a sample.
OTEL_SHUTDOWN_MODEL_CLASSIFIER_REPO="$fixture_repo" \
  check "unchanged exact revisions" false false --diff "$base" "$base"

# Narrative text changes the literate source but not either tangled campaign.
printf '\n<!-- selector prose fixture -->\n' >> "$fixture_repo/formal/quint/shutdown-lifecycle.md"
git -C "$fixture_repo" add formal/quint/shutdown-lifecycle.md
git -C "$fixture_repo" commit -qm prose
prose=$(git -C "$fixture_repo" rev-parse HEAD)
OTEL_SHUTDOWN_MODEL_CLASSIFIER_REPO="$fixture_repo" \
  check "prose-only literate change" false false --diff "$base" "$prose"

# A fenced lifecycle change selects lifecycle alone; it proves the two output
# closures are compared independently rather than treating the document byte
# as a monolithic sample trigger.
python3 - "$fixture_repo/formal/quint/shutdown-lifecycle.md" <<'PY'
import pathlib
import sys
p = pathlib.Path(sys.argv[1])
s = p.read_text()
needle = "module shutdownLifecycle {"
assert needle in s
p.write_text(s.replace(needle, needle + "\n  // selector fixture lifecycle-only semantic edit", 1))
PY
git -C "$fixture_repo" add formal/quint/shutdown-lifecycle.md
git -C "$fixture_repo" commit -qm lifecycle
lifecycle=$(git -C "$fixture_repo" rev-parse HEAD)
OTEL_SHUTDOWN_MODEL_CLASSIFIER_REPO="$fixture_repo" \
  check "lifecycle campaign-only generated change" true false --diff "$prose" "$lifecycle"

python3 - "$fixture_repo/formal/quint/shutdown-lifecycle.md" <<'PY'
import pathlib
import sys
p = pathlib.Path(sys.argv[1])
s = p.read_text()
needle = "module shutdownSettlement {"
assert needle in s
p.write_text(s.replace(needle, needle + "\n  // selector fixture settlement-only semantic edit", 1))
PY
git -C "$fixture_repo" add formal/quint/shutdown-lifecycle.md
git -C "$fixture_repo" commit -qm settlement
settlement=$(git -C "$fixture_repo" rev-parse HEAD)
OTEL_SHUTDOWN_MODEL_CLASSIFIER_REPO="$fixture_repo" \
  check "settlement campaign-only generated change" false true --diff "$lifecycle" "$settlement"

echo "shutdown lifecycle model classifier source-only tests passed"
