#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
literate_spec="$repo_root/formal/quint/shutdown-lifecycle.md"
target="$repo_root/target/formal/quint"
model="$target/shutdownLifecycle.qnt"
tests="$target/shutdownLifecycleTest.qnt"
settlement_model="$target/shutdownSettlement.qnt"
settlement_tests="$target/shutdownSettlementTest.qnt"
required_quint_version=0.32.0
lmt_revision=62fe18f2f6a6e11c158ff2b2209e1082a4fcd59c

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

mkdir -p "$target"
(
  cd "$repo_root"
  lmt "${literate_spec#$repo_root/}"
)

quint typecheck "$model"
quint typecheck "$tests"
quint typecheck "$settlement_model"
quint typecheck "$settlement_tests"

for module in \
  shutdownLifecycleCorrectedTest \
  shutdownLifecycleTimedJoinMutantTest \
  shutdownLifecycleSwallowedWaitMutantTest \
  shutdownLifecycleLateExportMutantTest \
  shutdownLifecycleDoubleCloseMutantTest
do
  quint test "$tests" \
    --main "$module" \
    --match '.*Test' \
    --backend typescript \
    --verbosity 1
done

# Keep settlement controls connected to the maintained literate CI gate.
# ITF files preserve the concrete forbidden-cleanup path of every mutant.
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

settlement_witnesses=(
  callerAdmittedReached callerCompletedReached admissionRetiredReached
  drainAcceptedReached exportBeganReached exportEndedReached workerExitedReached
  joinSucceededReached joinFailedReached releaseCalledReached releaseTrueReached
  releaseFalseReached releaseOtherReached releaseThrewReached ownerPublishedReached
  sdkPublishedReached exporterRetiredReached internalUserBeganReached internalUserEndedReached
  witnessConfirmedReached witnessUnknownReached deliveryObservedReached proofRefreshedReached
  sourceRetiredReached persistedReached connectionClosedReached
)
settlement_log="$target/shutdown-settlement-sampled.log"
# sampleInit includes synchronous (absent-worker) and background-worker owners.
# Rust simulation avoids turning this safety gate into a TypeScript CPU benchmark.
quint run "$settlement_model" \
  --main shutdownSettlementCorrected \
  --init sampleInit \
  --invariants trustContract cleanupOwnsNoUsers cleanupOrder proofFactorsAgree releaseExactlyOnce cachedDeliveryStable \
  --witnesses "${settlement_witnesses[@]}" \
  --max-steps 100 \
  --max-samples 10000 \
  --backend rust \
  --seed 0x114 \
  --verbosity 1 | tee "$settlement_log"

for witness in "${settlement_witnesses[@]}"
do
  if ! grep -Eq "^${witness} was witnessed in [1-9][0-9]* trace" "$settlement_log"
  then
    echo "required settlement witness was not reached: $witness" >&2
    exit 1
  fi
done

sample_log="$target/shutdown-lifecycle-sampled.log"
quint run "$model" \
  --main shutdownLifecycleCorrected \
  --invariants exporterCloseAfterWorkerTerminal exporterCloseAfterSuccessfulWait exporterCloseExactlyOnce noExportAfterOwnershipRelease failedWaitRetainsExporter closeCallStateIsConsistent terminalReturnRequiresCloseReturn terminalFailureHasCause terminalObserversAgree \
  --witnesses exportStartedReached exportFinishedReached shutdownRequestedReached ownerWorkRejectedReached workerTerminalReached waitSucceededReached waitFailedReached exporterCloseCalledReached exporterCloseReturnedReached exporterCloseFailedReached terminalReturnCompletedReached terminalFailureCompletedReached terminalReturnObservedReached terminalFailureObservedReached repeatedTerminalObservationReached \
  --max-steps 18 \
  --max-samples 10000 \
  --backend typescript \
  --verbosity 1 | tee "$sample_log"

for witness in \
  exportStartedReached exportFinishedReached shutdownRequestedReached \
  ownerWorkRejectedReached workerTerminalReached waitSucceededReached \
  waitFailedReached exporterCloseCalledReached exporterCloseReturnedReached \
  exporterCloseFailedReached terminalReturnCompletedReached \
  terminalFailureCompletedReached terminalReturnObservedReached \
  terminalFailureObservedReached repeatedTerminalObservationReached
do
  if ! grep -Eq "^${witness} was witnessed in [1-9][0-9]* trace" "$sample_log"
  then
    echo "required witness was not reached: $witness" >&2
    exit 1
  fi
done
