#!/usr/bin/env bash
set -euo pipefail

# The deterministic model controls and the two sampled campaigns deliberately
# have separate entry points. A selected sample still tangles and typechecks
# its own closure, so it cannot consume generated files left by another job.

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
literate_spec="$repo_root/formal/quint/shutdown-lifecycle.md"
target="$repo_root/target/formal/quint"
lifecycle_model="$target/shutdownLifecycle.qnt"
lifecycle_tests="$target/shutdownLifecycleTest.qnt"
settlement_model="$target/shutdownSettlement.qnt"
settlement_tests="$target/shutdownSettlementTest.qnt"
required_quint_version=0.32.0
lmt_revision=62fe18f2f6a6e11c158ff2b2209e1082a4fcd59c

usage() {
  echo "usage: $0 --deterministic | --sample lifecycle|settlement | --all" >&2
  exit 2
}

[[ $# -ge 1 && $# -le 2 ]] || usage
case "$1" in
  --deterministic) [[ $# -eq 1 ]] || usage; mode=deterministic ;;
  --sample)
    [[ $# -eq 2 ]] || usage
    case "$2" in lifecycle|settlement) mode="sample-$2" ;; *) usage ;; esac
    ;;
  --all) [[ $# -eq 1 ]] || usage; mode=all ;;
  *) usage ;;
esac

if ! command -v lmt >/dev/null 2>&1
then
  echo "lmt is required; install the pinned extractor with:" >&2
  echo "  go install github.com/driusan/lmt@$lmt_revision" >&2
  exit 1
fi

if ! command -v quint >/dev/null 2>&1
then
  echo "Quint $required_quint_version is required" >&2
  exit 1
fi

actual_quint_version=$(quint --version)
if [[ "$actual_quint_version" != "$required_quint_version" ]]
then
  echo "expected Quint $required_quint_version, found $actual_quint_version" >&2
  exit 1
fi

tangle() {
  mkdir -p "$target"
  (
    cd "$repo_root"
    lmt "${literate_spec#$repo_root/}"
  )
  # lmt has no manifest. Require the maintained generated closure; the
  # source-only classifier test independently pins this same inventory.
  for generated in "$lifecycle_model" "$lifecycle_tests" \
                   "$settlement_model" "$settlement_tests"
  do
    test -f "$generated" || {
      echo "missing tangled shutdown model output: $generated" >&2
      exit 1
    }
  done
}

typecheck_lifecycle() {
  quint typecheck "$lifecycle_model"
  quint typecheck "$lifecycle_tests"
}

typecheck_settlement() {
  quint typecheck "$settlement_model"
  quint typecheck "$settlement_tests"
}

run_deterministic() {
  tangle
  typecheck_lifecycle
  typecheck_settlement

  for module in \
    shutdownLifecycleCorrectedTest \
    shutdownLifecycleTimedJoinMutantTest \
    shutdownLifecycleSwallowedWaitMutantTest \
    shutdownLifecycleLateExportMutantTest \
    shutdownLifecycleDoubleCloseMutantTest
  do
    quint test "$lifecycle_tests" \
      --main "$module" \
      --match '.*Test' \
      --backend typescript \
      --verbosity 1
  done

  # ITF files preserve each concrete forbidden-cleanup path. These are
  # deterministic red controls, not the sampled settlement campaign.
  for module in \
    shutdownSettlementCorrectedTest \
    shutdownSettlementSDKOnlyMutantTest \
    shutdownSettlementFalseReleaseMutantTest \
    shutdownSettlementOtherReleaseMutantTest \
    shutdownSettlementThrownReleaseMutantTest \
    shutdownSettlementHostileWitnessMutantTest
  do
    quint test "$settlement_tests" \
      --main "$module" \
      --match '.*Test' \
      --backend typescript \
      --seed 0x114 \
      --out-itf "$target/${module}_{test}_{seq}.itf.json" \
      --verbosity 2
  done
}

run_settlement_sample() {
  tangle
  typecheck_settlement
  local witnesses=(
    callerAdmittedReached callerCompletedReached admissionRetiredReached
    drainAcceptedReached exportBeganReached exportEndedReached workerExitedReached
    joinSucceededReached joinFailedReached releaseCalledReached releaseTrueReached
    releaseFalseReached releaseOtherReached releaseThrewReached ownerPublishedReached
    sdkPublishedReached exporterRetiredReached internalUserBeganReached internalUserEndedReached
    witnessConfirmedReached witnessUnknownReached deliveryObservedReached proofRefreshedReached
    sourceRetiredReached persistedReached connectionClosedReached
  )
  local log="$target/shutdown-settlement-sampled.log"
  quint run "$settlement_model" \
    --main shutdownSettlementCorrected \
    --init sampleInit \
    --invariants trustContract cleanupOwnsNoUsers cleanupOrder proofFactorsAgree releaseExactlyOnce cachedDeliveryStable \
    --witnesses "${witnesses[@]}" \
    --max-steps 100 \
    --max-samples 10000 \
    --backend rust \
    --seed 0x114 \
    --verbosity 1 | tee "$log"
  for witness in "${witnesses[@]}"
  do
    if ! grep -Eq "^${witness} was witnessed in [1-9][0-9]* trace" "$log"
    then
      echo "required settlement witness was not reached: $witness" >&2
      exit 1
    fi
  done
}

run_lifecycle_sample() {
  tangle
  typecheck_lifecycle
  local witnesses=(
    exportStartedReached exportFinishedReached shutdownRequestedReached
    ownerWorkRejectedReached workerTerminalReached waitSucceededReached
    waitFailedReached exporterCloseCalledReached exporterCloseReturnedReached
    exporterCloseFailedReached terminalReturnCompletedReached
    terminalFailureCompletedReached terminalReturnObservedReached
    terminalFailureObservedReached repeatedTerminalObservationReached
  )
  local log="$target/shutdown-lifecycle-sampled.log"
  quint run "$lifecycle_model" \
    --main shutdownLifecycleCorrected \
    --invariants exporterCloseAfterWorkerTerminal exporterCloseAfterSuccessfulWait exporterCloseExactlyOnce noExportAfterOwnershipRelease failedWaitRetainsExporter closeCallStateIsConsistent terminalReturnRequiresCloseReturn terminalFailureHasCause terminalObserversAgree \
    --witnesses "${witnesses[@]}" \
    --max-steps 18 \
    --max-samples 10000 \
    --backend typescript \
    --verbosity 1 | tee "$log"
  for witness in "${witnesses[@]}"
  do
    if ! grep -Eq "^${witness} was witnessed in [1-9][0-9]* trace" "$log"
    then
      echo "required lifecycle witness was not reached: $witness" >&2
      exit 1
    fi
  done
}

case "$mode" in
  deterministic) run_deterministic ;;
  sample-lifecycle) run_lifecycle_sample ;;
  sample-settlement) run_settlement_sample ;;
  all)
    run_deterministic
    run_lifecycle_sample
    run_settlement_sample
    ;;
esac
