# SDK shutdown ownership model

This is the authoritative executable model for the worker/exporter ownership
contract shared by the batch span processor, periodic metric reader, and batch
log processor. The `lmt` tangler extracts the Quint fences into ignored files
under `target/formal/quint/`; generated `.qnt` files are never edited directly.

## Scope and correspondence

The model follows these implementation seams:

| Model transition | Runtime correspondence |
| --- | --- |
| stop accepting owner work | the processor/reader's `:shutdown?` state transition |
| export starts/finishes | exporter I/O performed by the background worker |
| worker becomes terminal | `otel.sdk.lifecycle/await-owned-worker!` observes actual termination after cooperative exit or destination-owned cancellation |
| worker wait fails | `await-owned-worker!` throws, including caller interruption or failure to terminate within the documented bound |
| exporter close call and outcome | the signal-specific exporter shutdown protocol method |
| shared terminal observation | `otel.sdk.lifecycle/run-terminal!` publishes one value or Throwable |

The production correspondence is
`src/otel/sdk/{lifecycle,export,metrics,logs}.clj`. The shared runtime event
vocabulary and trace validator live in `test/otel/sdk/lifecycle_events.clj` and
are exercised through all three owners in `test/otel/sdk/lifecycle_test.clj`.
The two representations use this one-for-one vocabulary:

| Quint event | Runtime keyword |
| --- | --- |
| `ExportStarted` / `ExportFinished` | `:export-started` / `:export-finished` |
| `ShutdownRequested` / `OwnerWorkRejected` | `:shutdown-requested` / `:owner-work-rejected` |
| `WorkerTerminal` | `:worker-terminal` |
| `WorkerWaitSucceeded` / `WorkerWaitFailed` | `:worker-wait-succeeded` / `:worker-wait-failed` |
| `ExporterCloseCalled` | `:exporter-close-called` |
| `ExporterCloseReturned` / `ExporterCloseFailed` | `:exporter-close-returned` / `:exporter-close-failed` |
| `TerminalCompletedReturn` / `TerminalCompletedFailure` | `:terminal-completed-return` / `:terminal-completed-failure` |
| `TerminalObservedReturn` / `TerminalObservedFailure` | `:terminal-observed-return` / `:terminal-observed-failure` |

## System model and abstraction

There is one processor owner, its one worker, one exporter, and three symbolic
shutdown callers. They coordinate through shared state, so this is plain Quint
rather than a message-passing Choreo model. Scheduling is asynchronous and any
enabled transition may interleave with another caller. A transition is one
observable ownership seam; notably worker termination, join outcome, exporter
close invocation, close outcome, terminal-result publication, and caller
observation remain separate so the property cannot be satisfied vacuously by
one atomic shutdown action.

The model hides queues, drain batching, locks, wall-clock intervals, exporter
payloads, cancellation grace, interrupt delivery, and Throwable identity.
Runtime tests retain responsibility for those mechanics, for proving that only
the exact worker recorded as exporter owner is interrupted after admission
retires, and for asserting that every caller receives the identical returned
object or Throwable. `TerminalFailed` may follow either an interrupted/failed
worker wait or an exporter-close failure, but the two causes remain distinct in
`waitOutcome` and `closeOutcome`. The model remains a safety model: the runtime
gate, rather than Quint, checks the 250 ms grace plus 2,000 ms terminal bound.

“Owner work” is deliberately narrower than producer-side telemetry recording.
For batch spans and logs, rejection means `on-end` cannot enqueue a new item.
For metrics, an existing counter may still record locally; the periodic reader
rejects collection/export, including `force-flush!`, after its shutdown
boundary. The metric runtime fixture records a fresh value after shutdown and
checks both the false flush result and unchanged exporter call count. Its causal
control executes the same collect/export body without the reader guard and must
both reach the exporter and invalidate the ownership trace.

The runtime event trace is a test-side observation, not production lifecycle
instrumentation. Exporter calls and worker joins are observed at their direct
protocol seams. `:terminal-completed-*` is reconstructed immediately after
`shutdown!` returns or throws, then ordered before test-side caller observation;
the stronger claim that every caller gets the same terminal object or Throwable
comes from direct identity assertions, not from those reconstructed events.
Successful coverage also includes each real SDK background worker blocked in
its exporter. The synthetic one-shot worker substitution remains only for
deterministically forcing the interrupted-join and concurrent/repeated-caller
histories; it is not described as the SDK drain loop.

Update this document and run its gate before changing the named lifecycle
seams. Do not weaken the model to accommodate an implementation defect.

## Properties and causal controls

The corrected model requires:

- an exporter-close call only after the worker is terminal and ownership join
  succeeded;
- at most one exporter-close call;
- shutdown rejects new owner work while already-accepted work may still begin
  export during draining, but no export starts after worker termination or the
  exporter-close call releases ownership;
- a failed/interrupted wait never releases exporter ownership;
- terminal observers all see the one published outcome; and
- owner work attempted after the acceptance boundary is rejected.

The runtime cancellation companion additionally requires admission retirement
before interruption, exact worker/export-owner identity, no sibling interrupt,
no exporter close after the bounded wait fails, and explicit span-delivery
failed accounting without a new export for accepted batches behind a cancelled
operation. Other batch pipelines likewise start no new export.
Force-flush is deliberately outside that cancellation transition and retains
transport timeout semantics. Real stalled-socket companion tests cover the
signal-specific span, log, and metric result propagation omitted by this model.
Truthy-after-interrupt exporter controls enforce the runtime policy that a sent
owned interrupt is an ambiguous delivery and therefore a failed destination.
The metric scheduled-snapshot/retirement race is likewise a runtime-layer
collection boundary and is covered by a deterministic exporter barrier test,
not by the signal-agnostic lifecycle abstraction below.

Four mutation modules isolate the assumptions that previously escaped the
external-call-only Hegel history:

- `shutdownLifecycleTimedJoinMutant` treats a timed join as worker quiescence;
- `shutdownLifecycleSwallowedWaitMutant` closes after a failed wait;
- `shutdownLifecycleLateExportMutant` begins export after ownership release;
- `shutdownLifecycleDoubleCloseMutant` invokes exporter close twice.

The deterministic tests make each mutant red for its corresponding invariant,
while corrected success, interrupted-wait, close-failure, concurrent-caller,
repeated-caller, and post-shutdown-rejection traces remain green.

## Command

The deterministic control gate tangles this document, typechecks all four
generated files, and runs the corrected and mutation scenarios:

```sh
scripts/check-shutdown-lifecycle-quint.sh --deterministic
```

The two 10,000-sample campaigns are intentionally independent. Each tangles
this document and typechecks its own generated closure before sampling, but
does not rerun unrelated mutation controls:

```sh
scripts/check-shutdown-lifecycle-quint.sh --sample lifecycle
scripts/check-shutdown-lifecycle-quint.sh --sample settlement
```

`--all` is the local convenience command for the deterministic controls
followed by both samples. CI first records an exact base/head classifier
receipt, always runs deterministic controls, and selects each sample only when
its effective tangled closure or a conservative runtime-correspondence input
changed. Missing revisions, extractor failures, checker/workflow changes, and
manual `force_both` dispatches select both campaigns. Prose-only changes to
this document can skip both only when exact-revision extraction proves both
generated closures and shared pinned inputs byte-identical.

It requires Quint 0.32.0 and `lmt` at commit
`62fe18f2f6a6e11c158ff2b2209e1082a4fcd59c`. Simulation is sampled evidence,
not exhaustive proof. The dedicated CI workflow is path-filtered to the model,
checker, classifier, implementation, and runtime-test correspondence closure.

## Executable model

```quint target/formal/quint/shutdownLifecycle.qnt +=
// Worker/exporter ownership model for the three background SDK owners.

module shutdownLifecycle {
  const ALLOW_TIMED_JOIN: bool
  const ALLOW_CLOSE_AFTER_FAILED_WAIT: bool
  const ALLOW_LATE_EXPORT: bool
  const ALLOW_DOUBLE_CLOSE: bool

  type CallerId = Caller1 | Caller2 | Caller3
  type CallerPhase =
    | CallerIdle
    | CallerWaiting
    | CallerObservedReturn
    | CallerObservedFailure

  type TerminalOwner = NoOwner | OwnedBy(CallerId)
  type WaitOutcome = NotWaiting | WaitingForWorker | WaitSucceeded | WaitFailed
  type CloseOutcome =
    | CloseNotCalled
    | CloseInFlight
    | CloseReturned
    | CloseFailed
  type TerminalResult = TerminalPending | TerminalReturned | TerminalFailed
  type LifecycleEvent =
    | ExportStarted
    | ExportFinished
    | ShutdownRequested
    | OwnerWorkRejected
    | WorkerTerminal
    | WorkerWaitSucceeded
    | WorkerWaitFailed
    | ExporterCloseCalled
    | ExporterCloseReturned
    | ExporterCloseFailed
    | TerminalCompletedReturn
    | TerminalCompletedFailure
    | TerminalObservedReturn
    | TerminalObservedFailure

  type LifecycleState = {
    accepting: bool,
    exportActive: bool,
    workerTerminal: bool,
    waitOutcome: WaitOutcome,
    exporterCloseCalled: bool,
    closeOutcome: CloseOutcome,
    closeCount: int,
    terminalResult: TerminalResult,
    owner: TerminalOwner,
    callers: CallerId -> CallerPhase,
    ownerWorkRejectedCount: int,
    exportStartedAfterOwnershipRelease: bool,
    repeatObservationCount: int,
    seen: Set[LifecycleEvent],
  }

  pure val CALLERS: Set[CallerId] = Set(Caller1, Caller2, Caller3)

  var state: LifecycleState

  pure def note(s: LifecycleState, event: LifecycleEvent): Set[LifecycleEvent] =
    s.seen.union(Set(event))

  pure def canBeginExport(s: LifecycleState): bool =
    not(s.exportActive) and
      ((not(s.workerTerminal) and not(s.exporterCloseCalled)) or
        ALLOW_LATE_EXPORT)

  pure def applyBeginExport(s: LifecycleState): LifecycleState =
    { ...s,
      exportActive: true,
      exportStartedAfterOwnershipRelease:
        s.exportStartedAfterOwnershipRelease or s.workerTerminal or
          s.exporterCloseCalled,
      seen: note(s, ExportStarted),
    }

  pure def canFinishExport(s: LifecycleState): bool = s.exportActive

  pure def applyFinishExport(s: LifecycleState): LifecycleState =
    { ...s, exportActive: false, seen: note(s, ExportFinished) }

  pure def canRequestShutdown(s: LifecycleState, caller: CallerId): bool =
    s.callers.get(caller) == CallerIdle

  pure def applyRequestShutdown(s: LifecycleState, caller: CallerId): LifecycleState = {
    val first = s.owner == NoOwner
    { ...s,
      accepting: false,
      owner: if (first) OwnedBy(caller) else s.owner,
      waitOutcome: if (first) WaitingForWorker else s.waitOutcome,
      callers: s.callers.put(caller, CallerWaiting),
      seen: note(s, ShutdownRequested),
    }
  }

  pure def canRejectOwnerWork(s: LifecycleState): bool = not(s.accepting)

  pure def applyRejectOwnerWork(s: LifecycleState): LifecycleState =
    { ...s,
      ownerWorkRejectedCount: s.ownerWorkRejectedCount + 1,
      seen: note(s, OwnerWorkRejected),
    }

  pure def canTerminateWorker(s: LifecycleState): bool =
    s.owner != NoOwner and not(s.accepting) and not(s.exportActive) and
      not(s.workerTerminal)

  pure def applyTerminateWorker(s: LifecycleState): LifecycleState =
    { ...s, workerTerminal: true, seen: note(s, WorkerTerminal) }

  pure def canSucceedWait(s: LifecycleState): bool =
    s.waitOutcome == WaitingForWorker and s.terminalResult == TerminalPending and
      (s.workerTerminal or ALLOW_TIMED_JOIN)

  pure def applySucceedWait(s: LifecycleState): LifecycleState =
    { ...s,
      waitOutcome: WaitSucceeded,
      seen: note(s, WorkerWaitSucceeded),
    }

  pure def canFailWait(s: LifecycleState): bool =
    s.waitOutcome == WaitingForWorker and s.terminalResult == TerminalPending

  pure def applyFailWait(s: LifecycleState): LifecycleState =
    { ...s,
      waitOutcome: WaitFailed,
      seen: note(s, WorkerWaitFailed),
    }

  pure def canCallExporterClose(s: LifecycleState): bool =
    not(s.exportActive) and
      (s.waitOutcome == WaitSucceeded or
        (ALLOW_CLOSE_AFTER_FAILED_WAIT and s.waitOutcome == WaitFailed)) and
      ((s.closeCount == 0 and s.closeOutcome == CloseNotCalled and
          s.terminalResult == TerminalPending) or
        (ALLOW_DOUBLE_CLOSE and s.closeCount == 1 and
          s.closeOutcome == CloseReturned))

  pure def applyCallExporterClose(s: LifecycleState): LifecycleState =
    { ...s,
      exporterCloseCalled: true,
      closeCount: s.closeCount + 1,
      closeOutcome: CloseInFlight,
      seen: note(s, ExporterCloseCalled),
    }

  pure def canCompleteExporterClose(s: LifecycleState): bool =
    s.closeOutcome == CloseInFlight

  pure def applyCompleteExporterClose(
      s: LifecycleState, outcome: CloseOutcome,
      event: LifecycleEvent): LifecycleState =
    { ...s, closeOutcome: outcome, seen: note(s, event) }

  pure def canCompleteTerminalReturn(s: LifecycleState): bool =
    s.terminalResult == TerminalPending and s.closeOutcome == CloseReturned

  pure def canCompleteTerminalFailure(s: LifecycleState): bool =
    s.terminalResult == TerminalPending and
      (s.waitOutcome == WaitFailed or s.closeOutcome == CloseFailed)

  pure def applyCompleteTerminal(
      s: LifecycleState, result: TerminalResult,
      event: LifecycleEvent): LifecycleState =
    { ...s, terminalResult: result, seen: note(s, event) }

  pure def canObserveTerminal(s: LifecycleState, caller: CallerId): bool =
    s.callers.get(caller) == CallerWaiting and
      s.terminalResult != TerminalPending

  pure def applyObserveTerminal(s: LifecycleState, caller: CallerId): LifecycleState = {
    val returned = s.terminalResult == TerminalReturned
    { ...s,
      callers: s.callers.put(
        caller,
        if (returned) CallerObservedReturn else CallerObservedFailure),
      seen: note(
        s,
        if (returned) TerminalObservedReturn else TerminalObservedFailure),
    }
  }

  pure def canRepeatTerminalObservation(
      s: LifecycleState, caller: CallerId): bool =
    (s.callers.get(caller) == CallerObservedReturn and
      s.terminalResult == TerminalReturned) or
    (s.callers.get(caller) == CallerObservedFailure and
      s.terminalResult == TerminalFailed)

  pure def applyRepeatTerminalObservation(s: LifecycleState): LifecycleState = {
    val returned = s.terminalResult == TerminalReturned
    val requested = { ...s, seen: note(s, ShutdownRequested) }
    { ...requested,
      repeatObservationCount: s.repeatObservationCount + 1,
      seen: note(
        requested,
        if (returned) TerminalObservedReturn else TerminalObservedFailure),
    }
  }

  action init: bool = all {
    state' = {
      accepting: true,
      exportActive: false,
      workerTerminal: false,
      waitOutcome: NotWaiting,
      exporterCloseCalled: false,
      closeOutcome: CloseNotCalled,
      closeCount: 0,
      terminalResult: TerminalPending,
      owner: NoOwner,
      callers: CALLERS.mapBy(_ => CallerIdle),
      ownerWorkRejectedCount: 0,
      exportStartedAfterOwnershipRelease: false,
      repeatObservationCount: 0,
      seen: Set(),
    },
  }

  action beginExport: bool = all {
    canBeginExport(state),
    state' = applyBeginExport(state),
  }

  action finishExport: bool = all {
    canFinishExport(state),
    state' = applyFinishExport(state),
  }

  action requestShutdown(caller: CallerId): bool = all {
    canRequestShutdown(state, caller),
    state' = applyRequestShutdown(state, caller),
  }

  action rejectOwnerWork: bool = all {
    canRejectOwnerWork(state),
    state' = applyRejectOwnerWork(state),
  }

  action terminateWorker: bool = all {
    canTerminateWorker(state),
    state' = applyTerminateWorker(state),
  }

  action succeedWait: bool = all {
    canSucceedWait(state),
    state' = applySucceedWait(state),
  }

  action failWait: bool = all {
    canFailWait(state),
    state' = applyFailWait(state),
  }

  action callExporterClose: bool = all {
    canCallExporterClose(state),
    state' = applyCallExporterClose(state),
  }

  action returnExporterClose: bool = all {
    canCompleteExporterClose(state),
    state' = applyCompleteExporterClose(
      state, CloseReturned, ExporterCloseReturned),
  }

  action failExporterClose: bool = all {
    canCompleteExporterClose(state),
    state' = applyCompleteExporterClose(
      state, CloseFailed, ExporterCloseFailed),
  }

  action completeTerminalReturn: bool = all {
    canCompleteTerminalReturn(state),
    state' = applyCompleteTerminal(
      state, TerminalReturned, TerminalCompletedReturn),
  }

  action completeTerminalFailure: bool = all {
    canCompleteTerminalFailure(state),
    state' = applyCompleteTerminal(
      state, TerminalFailed, TerminalCompletedFailure),
  }

  action observeTerminal(caller: CallerId): bool = all {
    canObserveTerminal(state, caller),
    state' = applyObserveTerminal(state, caller),
  }

  action repeatTerminalObservation(caller: CallerId): bool = all {
    canRepeatTerminalObservation(state, caller),
    state' = applyRepeatTerminalObservation(state),
  }

  action step: bool = {
    nondet caller = CALLERS.oneOf()
    any {
      beginExport,
      finishExport,
      requestShutdown(caller),
      rejectOwnerWork,
      terminateWorker,
      succeedWait,
      failWait,
      callExporterClose,
      returnExporterClose,
      failExporterClose,
      completeTerminalReturn,
      completeTerminalFailure,
      observeTerminal(caller),
      repeatTerminalObservation(caller),
    }
  }

  val exporterCloseAfterWorkerTerminal: bool =
    state.closeCount == 0 or state.workerTerminal

  val exporterCloseAfterSuccessfulWait: bool =
    state.closeCount == 0 or state.waitOutcome == WaitSucceeded

  val exporterCloseExactlyOnce: bool = state.closeCount <= 1

  val noExportAfterOwnershipRelease: bool =
    not(state.exportStartedAfterOwnershipRelease)

  val failedWaitRetainsExporter: bool =
    state.waitOutcome != WaitFailed or
      (state.closeCount == 0 and not(state.exporterCloseCalled))

  val closeCallStateIsConsistent: bool =
    state.exporterCloseCalled == (state.closeCount > 0)

  val terminalReturnRequiresCloseReturn: bool =
    state.terminalResult != TerminalReturned or
      (state.closeCount == 1 and state.closeOutcome == CloseReturned)

  val terminalFailureHasCause: bool =
    state.terminalResult != TerminalFailed or
      state.waitOutcome == WaitFailed or state.closeOutcome == CloseFailed

  val terminalObserversAgree: bool = and {
    state.callers.keys().forall(caller =>
      state.callers.get(caller) != CallerObservedReturn or
        state.terminalResult == TerminalReturned),
    state.callers.keys().forall(caller =>
      state.callers.get(caller) != CallerObservedFailure or
        state.terminalResult == TerminalFailed),
  }

  val exportStartedReached: bool = state.seen.contains(ExportStarted)
  val exportFinishedReached: bool = state.seen.contains(ExportFinished)
  val shutdownRequestedReached: bool = state.seen.contains(ShutdownRequested)
  val ownerWorkRejectedReached: bool = state.seen.contains(OwnerWorkRejected)
  val workerTerminalReached: bool = state.seen.contains(WorkerTerminal)
  val waitSucceededReached: bool = state.seen.contains(WorkerWaitSucceeded)
  val waitFailedReached: bool = state.seen.contains(WorkerWaitFailed)
  val exporterCloseCalledReached: bool =
    state.seen.contains(ExporterCloseCalled)
  val exporterCloseReturnedReached: bool =
    state.seen.contains(ExporterCloseReturned)
  val exporterCloseFailedReached: bool =
    state.seen.contains(ExporterCloseFailed)
  val terminalReturnCompletedReached: bool =
    state.seen.contains(TerminalCompletedReturn)
  val terminalFailureCompletedReached: bool =
    state.seen.contains(TerminalCompletedFailure)
  val terminalReturnObservedReached: bool =
    state.seen.contains(TerminalObservedReturn)
  val terminalFailureObservedReached: bool =
    state.seen.contains(TerminalObservedFailure)
  val repeatedTerminalObservationReached: bool =
    state.repeatObservationCount > 0
}

module shutdownLifecycleCorrected {
  import shutdownLifecycle(
    ALLOW_TIMED_JOIN = false,
    ALLOW_CLOSE_AFTER_FAILED_WAIT = false,
    ALLOW_LATE_EXPORT = false,
    ALLOW_DOUBLE_CLOSE = false
  ).*
}

module shutdownLifecycleTimedJoinMutant {
  import shutdownLifecycle(
    ALLOW_TIMED_JOIN = true,
    ALLOW_CLOSE_AFTER_FAILED_WAIT = false,
    ALLOW_LATE_EXPORT = false,
    ALLOW_DOUBLE_CLOSE = false
  ).*
}

module shutdownLifecycleSwallowedWaitMutant {
  import shutdownLifecycle(
    ALLOW_TIMED_JOIN = false,
    ALLOW_CLOSE_AFTER_FAILED_WAIT = true,
    ALLOW_LATE_EXPORT = false,
    ALLOW_DOUBLE_CLOSE = false
  ).*
}

module shutdownLifecycleLateExportMutant {
  import shutdownLifecycle(
    ALLOW_TIMED_JOIN = false,
    ALLOW_CLOSE_AFTER_FAILED_WAIT = false,
    ALLOW_LATE_EXPORT = true,
    ALLOW_DOUBLE_CLOSE = false
  ).*
}

module shutdownLifecycleDoubleCloseMutant {
  import shutdownLifecycle(
    ALLOW_TIMED_JOIN = false,
    ALLOW_CLOSE_AFTER_FAILED_WAIT = false,
    ALLOW_LATE_EXPORT = false,
    ALLOW_DOUBLE_CLOSE = true
  ).*
}
```

## Executable scenarios

```quint target/formal/quint/shutdownLifecycleTest.qnt +=
// Deterministic corrected boundaries and causal mutation controls.

module shutdownLifecycleCorrectedTest {
  import shutdownLifecycle(
    ALLOW_TIMED_JOIN = false,
    ALLOW_CLOSE_AFTER_FAILED_WAIT = false,
    ALLOW_LATE_EXPORT = false,
    ALLOW_DOUBLE_CLOSE = false
  ).* from "./shutdownLifecycle"

  run normalConcurrentShutdownTest =
    init
      .then(beginExport)
      .then(requestShutdown(Caller1))
      .then(requestShutdown(Caller2))
      .then(rejectOwnerWork)
      .then(finishExport)
      .then(terminateWorker)
      .then(succeedWait)
      .then(callExporterClose)
      .then(returnExporterClose)
      .then(completeTerminalReturn)
      .then(observeTerminal(Caller2))
      .then(observeTerminal(Caller1))
      .then(repeatTerminalObservation(Caller1))
      .then(requestShutdown(Caller3))
      .then(observeTerminal(Caller3))
      .expect(and {
        exporterCloseAfterWorkerTerminal,
        exporterCloseAfterSuccessfulWait,
        exporterCloseExactlyOnce,
        noExportAfterOwnershipRelease,
        terminalReturnRequiresCloseReturn,
        terminalObserversAgree,
        state.ownerWorkRejectedCount == 1,
        state.closeCount == 1,
        state.repeatObservationCount == 1,
      })

  run interruptedWaitRetainsOwnershipTest =
    init
      .then(beginExport)
      .then(requestShutdown(Caller1))
      .then(requestShutdown(Caller2))
      .then(failWait)
      .then(completeTerminalFailure)
      .then(observeTerminal(Caller1))
      .then(observeTerminal(Caller2))
      .then(finishExport)
      .then(terminateWorker)
      .expect(and {
        failedWaitRetainsExporter,
        state.terminalResult == TerminalFailed,
        state.closeCount == 0,
        not(state.exporterCloseCalled),
        terminalObserversAgree,
      })

  run acceptedDrainMayStartAfterShutdownTest =
    init
      .then(requestShutdown(Caller1))
      .then(beginExport)
      .then(finishExport)
      .then(terminateWorker)
      .then(succeedWait)
      .then(callExporterClose)
      .then(returnExporterClose)
      .then(completeTerminalReturn)
      .expect(and {
        noExportAfterOwnershipRelease,
        state.closeCount == 1,
        state.terminalResult == TerminalReturned,
      })

  run exporterCloseFailureIsSharedTest =
    init
      .then(requestShutdown(Caller1))
      .then(terminateWorker)
      .then(succeedWait)
      .then(callExporterClose)
      .then(failExporterClose)
      .then(completeTerminalFailure)
      .then(observeTerminal(Caller1))
      .then(requestShutdown(Caller2))
      .then(observeTerminal(Caller2))
      .expect(and {
        state.closeCount == 1,
        state.terminalResult == TerminalFailed,
        terminalObserversAgree,
      })

  run correctedRejectsTimedJoinTest =
    init
      .then(requestShutdown(Caller1))
      .then(succeedWait.fail())

  run correctedRejectsCloseAfterFailedWaitTest =
    init
      .then(requestShutdown(Caller1))
      .then(failWait)
      .then(callExporterClose.fail())

  run correctedRejectsLateExportTest =
    init
      .then(requestShutdown(Caller1))
      .then(terminateWorker)
      .then(beginExport.fail())

  run correctedRejectsDoubleCloseTest =
    init
      .then(requestShutdown(Caller1))
      .then(terminateWorker)
      .then(succeedWait)
      .then(callExporterClose)
      .then(returnExporterClose)
      .then(callExporterClose.fail())
}

module shutdownLifecycleTimedJoinMutantTest {
  import shutdownLifecycle(
    ALLOW_TIMED_JOIN = true,
    ALLOW_CLOSE_AFTER_FAILED_WAIT = false,
    ALLOW_LATE_EXPORT = false,
    ALLOW_DOUBLE_CLOSE = false
  ).* from "./shutdownLifecycle"

  run timedJoinViolatesQuiescenceTest =
    init
      .then(requestShutdown(Caller1))
      .then(succeedWait)
      .then(callExporterClose)
      .expect(not(exporterCloseAfterWorkerTerminal))
}

module shutdownLifecycleSwallowedWaitMutantTest {
  import shutdownLifecycle(
    ALLOW_TIMED_JOIN = false,
    ALLOW_CLOSE_AFTER_FAILED_WAIT = true,
    ALLOW_LATE_EXPORT = false,
    ALLOW_DOUBLE_CLOSE = false
  ).* from "./shutdownLifecycle"

  run swallowedWaitViolatesOwnershipTest =
    init
      .then(requestShutdown(Caller1))
      .then(failWait)
      .then(callExporterClose)
      .expect(not(failedWaitRetainsExporter))
}

module shutdownLifecycleLateExportMutantTest {
  import shutdownLifecycle(
    ALLOW_TIMED_JOIN = false,
    ALLOW_CLOSE_AFTER_FAILED_WAIT = false,
    ALLOW_LATE_EXPORT = true,
    ALLOW_DOUBLE_CLOSE = false
  ).* from "./shutdownLifecycle"

  run lateExportViolatesOwnershipReleaseTest =
    init
      .then(requestShutdown(Caller1))
      .then(terminateWorker)
      .then(beginExport)
      .expect(not(noExportAfterOwnershipRelease))
}

module shutdownLifecycleDoubleCloseMutantTest {
  import shutdownLifecycle(
    ALLOW_TIMED_JOIN = false,
    ALLOW_CLOSE_AFTER_FAILED_WAIT = false,
    ALLOW_LATE_EXPORT = false,
    ALLOW_DOUBLE_CLOSE = true
  ).* from "./shutdownLifecycle"

  run doubleCloseViolatesExactlyOnceTest =
    init
      .then(requestShutdown(Caller1))
      .then(terminateWorker)
      .then(succeedWait)
      .then(callExporterClose)
      .then(returnExporterClose)
      .then(callExporterClose)
      .expect(not(exporterCloseExactlyOnce))
}
```

## SDK settlement and consumer cleanup

A finished shutdown call is not permission to close the shared database.
An admitted caller can still be using an exporter after the background worker
exits. Conversely, a failed delivery result can remain cached even after all
resource users eventually settle. This extension models those facts separately.
It does not replace the worker/join controls above.

There are two symbolic component owners and two caller operation IDs per owner.
`SpanOwner` and `MetricOwner` are opaque component labels, not separate signal
algorithms: logs use the same ownership seams. Sampled initialization includes
absent workers for synchronous owners; an absent worker needs no exit transition.
Two components exercise the all-components conjunction, not a proof about every
SDK registration graph or the behavior of arbitrary custom component owners.
Each owner groups admission, callers, accepted drains, worker, export operations,
join, release and terminal publication. The consumer groups its cached delivery
result, fresh versioned proof factors, source retirement, persistence and close.
The aggregate SDK proof is computed from **every** component and its own terminal
publication; it cannot be independently assigned `confirmed`.

The exporter has its own admission and internal users. Literal-true release is
a trusted resource-release contract. False, nonliteral/void and thrown release
do not prove settlement. A custom witness is also a trusted contract: confirmation
must mean permanent admission retirement and no resource users, not momentary
idleness. The checked `trustContract` property expresses that premise; there is
no unenforced `assume`. The hostile-witness module intentionally violates the
premise and demonstrates why the SDK cannot guarantee safety against a lying
implementation. Arbitrary custom-witness latency is not modeled or enforced.

Each admission CAS, caller completion, drain admission/start/finish, worker exit,
join outcome, release invocation/outcome, component/SDK publication, witness
observation, consumer proof snapshot and cleanup seam is a separate transition.
Exporter retirement is permanent, making a confirmed snapshot stable despite
later scheduling. Unknown/throwing/partial witnesses remain unconfirmed.
`persist` is only an ordering marker, **not** a claim about native persistence.
No delivery, wall-clock, scheduling fairness or transport guarantees follow.

Correspondence extends `lifecycle.clj`'s `admitted-operation!`, `release-exporter!`,
`owned-settlement`, `component-settlement`, and `combined-settlement`, and
`sdk.clj`'s `shutdown-status`. The consumer corresponds to Oscope's embedded stop
source/persist/connection-close sequence. Caller/worker operations carry distinct
symbolic IDs in the model; a future runtime trace must retain owner and operation
identity rather than reconstructing admission from exporter calls. New seam names
below are **model observations**, not a claim that production emits them.
The existing reconstructed terminal events still do not prove publication
linearization, result/Throwable identity, or actual native resource settlement.
Real blocked-exporter tests remain necessary for those boundaries.

### Settlement state and transitions

```quint target/formal/quint/shutdownSettlement.qnt +=
module shutdownSettlement {
  const SDK_ONLY_CLEANUP: bool
  const ACCEPT_FALSE_RELEASE: bool
  const ACCEPT_OTHER_RELEASE: bool
  const ACCEPT_THROWN_RELEASE: bool
  const LYING_WITNESS: bool

  type OwnerId = SpanOwner | MetricOwner
  type OperationId = CallerOperation1 | CallerOperation2 | DrainOperation
  type Admission = Open | Retired
  type Worker = Absent | Running | Exited
  type Wait = UnstartedWait | SuccessfulWait | FailedWait
  type Release = UnstartedRelease | RunningRelease | TrueRelease |
    FalseRelease | OtherRelease | ThrownRelease
  type Terminal = PendingTerminal | TrueTerminal | FalseTerminal | ThrownTerminal
  type Witness = UnconfirmedWitness | ConfirmedWitness
  type Seam = CallerAdmitted | CallerCompleted | AdmissionRetired |
    DrainAccepted | ExportBegan | ExportEnded | WorkerExited | JoinSucceeded |
    JoinFailed | ReleaseCalled | ReleaseTrue | ReleaseFalse | ReleaseOther |
    ReleaseThrew | OwnerPublished | SDKPublished | ExporterRetired |
    InternalUserBegan | InternalUserEnded | WitnessConfirmed | WitnessUnknown |
    DeliveryObserved | ProofRefreshed | SourceRetired | Persisted | ConnectionClosed

  type OwnerState = {
    admission: Admission, callers: Set[OperationId], drains: Set[OperationId],
    worker: Worker, exports: Set[OperationId], wait: Wait, release: Release,
    terminal: Terminal, closeCount: int,
    exporterAdmission: Admission, internalUsers: Set[OperationId], witness: Witness,
  }
  type Factors = {
    retired: bool, callers: bool, worker: bool, terminal: bool,
    sdk: bool, exporter: bool, confirmed: bool,
  }
  type ConsumerState = {
    delivery: Terminal, proofVersion: int, factors: OwnerId -> Factors,
    aggregate: bool, sourceRetired: bool, persisted: bool, closed: bool,
    refreshes: int, sdkTerminalObserved: bool,
  }
  type State = {
    owners: OwnerId -> OwnerState, sdkTerminal: Terminal,
    consumer: ConsumerState, seen: Set[Seam],
  }

  pure val OWNERS = Set(SpanOwner, MetricOwner)
  pure val CALLER_OPERATIONS = Set(CallerOperation1, CallerOperation2)
  pure def terminalSettled(t: Terminal): bool = t != PendingTerminal
  pure def sdkSettled(o: OwnerState): bool =
    o.admission == Retired and o.callers == Set() and o.worker != Running and
      terminalSettled(o.terminal)
  pure def actualExporterSettled(o: OwnerState): bool =
    o.exporterAdmission == Retired and o.exports == Set() and o.internalUsers == Set()
  pure def releaseProof(o: OwnerState): bool =
    o.release == TrueRelease or o.witness == ConfirmedWitness or
      (ACCEPT_FALSE_RELEASE and o.release == FalseRelease) or
      (ACCEPT_OTHER_RELEASE and o.release == OtherRelease) or
      (ACCEPT_THROWN_RELEASE and o.release == ThrownRelease)
  pure def observeFactors(o: OwnerState): Factors = {
    retired: o.admission == Retired, callers: o.callers == Set(),
    worker: o.worker != Running, terminal: terminalSettled(o.terminal),
    sdk: sdkSettled(o), exporter: releaseProof(o),
    confirmed: sdkSettled(o) and releaseProof(o),
  }
  pure def putOwner(s: State, id: OwnerId, o: OwnerState, seam: Seam): State =
    { ...s, owners: s.owners.put(id, o), seen: s.seen.union(Set(seam)) }
  pure def putConsumer(s: State, c: ConsumerState, seam: Seam): State =
    { ...s, consumer: c, seen: s.seen.union(Set(seam)) }

  pure val initialOwner: OwnerState = {
    admission: Open, callers: Set(), drains: Set(), worker: Running,
    exports: Set(), wait: UnstartedWait, release: UnstartedRelease,
    terminal: PendingTerminal, closeCount: 0,
    exporterAdmission: Open, internalUsers: Set(), witness: UnconfirmedWitness,
  }
  pure val emptyFactors: Factors = {
    retired: false, callers: false, worker: false, terminal: false,
    sdk: false, exporter: false, confirmed: false,
  }
  action initWithAbsent(absent: Set[OwnerId]): bool = all {
    absent.subseteq(OWNERS),
    state' = {
    owners: OWNERS.mapBy(id => { ...initialOwner,
      worker: if (absent.contains(id)) Absent else Running }), sdkTerminal: PendingTerminal,
    consumer: {
      delivery: PendingTerminal, proofVersion: 0,
      factors: OWNERS.mapBy(_ => emptyFactors), aggregate: false,
      sourceRetired: false, persisted: false, closed: false, refreshes: 0,
      sdkTerminalObserved: false,
    }, seen: Set(),
    },
  }
  action init = initWithAbsent(Set())
  action sampleInit = {
    nondet absent = OWNERS.powerset().oneOf()
    initWithAbsent(absent)
  }
  var state: State

  pure def canAdmit(o: OwnerState, op: OperationId): bool =
    o.admission == Open and CALLER_OPERATIONS.contains(op) and not(o.callers.contains(op))
  action admit(id: OwnerId, op: OperationId): bool = {
    val o = state.owners.get(id)
    all { canAdmit(o, op),
      state' = putOwner(state, id, { ...o, callers: o.callers.union(Set(op)) }, CallerAdmitted) }
  }
  pure def canComplete(o: OwnerState, op: OperationId): bool =
    o.callers.contains(op) and not(o.exports.contains(op))
  action complete(id: OwnerId, op: OperationId): bool = {
    val o = state.owners.get(id)
    all { canComplete(o, op),
      state' = putOwner(state, id, { ...o, callers: o.callers.exclude(Set(op)) }, CallerCompleted) }
  }
  action retire(id: OwnerId): bool = {
    val o = state.owners.get(id)
    all { o.admission == Open,
      state' = putOwner(state, id, { ...o, admission: Retired }, AdmissionRetired) }
  }
  pure def canAcceptDrain(o: OwnerState): bool =
    o.admission == Open and o.worker == Running and o.drains == Set()
  action acceptDrain(id: OwnerId): bool = {
    val o = state.owners.get(id)
    all { canAcceptDrain(o),
      state' = putOwner(state, id, { ...o, drains: Set(DrainOperation) }, DrainAccepted) }
  }
  pure def canBegin(o: OwnerState, op: OperationId): bool =
    o.exporterAdmission == Open and not(o.exports.contains(op)) and
      (o.callers.contains(op) or (o.drains.contains(op) and o.worker == Running))
  action beginExport(id: OwnerId, op: OperationId): bool = {
    val o = state.owners.get(id)
    all { canBegin(o, op),
      state' = putOwner(state, id, { ...o, exports: o.exports.union(Set(op)) }, ExportBegan) }
  }
  action finishExport(id: OwnerId, op: OperationId): bool = {
    val o = state.owners.get(id)
    all { o.exports.contains(op),
      state' = putOwner(state, id, { ...o, exports: o.exports.exclude(Set(op)),
        drains: o.drains.exclude(Set(op)) }, ExportEnded) }
  }
  pure def canExit(o: OwnerState): bool = o.admission == Retired and
    o.worker == Running and o.drains == Set() and not(o.exports.contains(DrainOperation))
  action exitWorker(id: OwnerId): bool = {
    val o = state.owners.get(id)
    all { canExit(o),
      state' = putOwner(state, id, { ...o, worker: Exited }, WorkerExited) }
  }
  action join(id: OwnerId, success: bool): bool = {
    val o = state.owners.get(id)
    all { o.admission == Retired, o.wait == UnstartedWait,
      not(success) or o.worker != Running,
      state' = putOwner(state, id, { ...o, wait: if (success) SuccessfulWait else FailedWait },
        if (success) JoinSucceeded else JoinFailed) }
  }
  pure def canCallRelease(o: OwnerState): bool =
    o.wait == SuccessfulWait and o.worker != Running and o.release == UnstartedRelease
  action callRelease(id: OwnerId): bool = {
    val o = state.owners.get(id)
    all { canCallRelease(o),
      state' = putOwner(state, id, { ...o, release: RunningRelease, closeCount: o.closeCount + 1 }, ReleaseCalled) }
  }
  pure def canEndRelease(o: OwnerState, outcome: Release): bool =
    o.release == RunningRelease and
      Set(TrueRelease, FalseRelease, OtherRelease, ThrownRelease).contains(outcome) and
      (outcome != TrueRelease or (o.exports == Set() and o.internalUsers == Set()))
  action endRelease(id: OwnerId, outcome: Release): bool = {
    val o = state.owners.get(id)
    val seam = if (outcome == TrueRelease) ReleaseTrue else
      if (outcome == FalseRelease) ReleaseFalse else
      if (outcome == OtherRelease) ReleaseOther else ReleaseThrew
    all { canEndRelease(o, outcome),
      state' = putOwner(state, id, { ...o, release: outcome,
        exporterAdmission: if (outcome == TrueRelease) Retired else o.exporterAdmission }, seam) }
  }
  pure def canPublish(o: OwnerState): bool =
    o.terminal == PendingTerminal and
      (o.wait == FailedWait or Set(TrueRelease, FalseRelease, OtherRelease, ThrownRelease).contains(o.release))
  pure def canPublishResult(o: OwnerState, result: Terminal): bool =
    canPublish(o) and
      (if (o.wait == FailedWait or o.release == ThrownRelease) result == ThrownTerminal
       else if (o.release == FalseRelease) result == FalseTerminal
       else Set(TrueTerminal, FalseTerminal).contains(result))
  action publishOwnerResult(id: OwnerId, outcome: Terminal): bool = {
    val o = state.owners.get(id)
    all { canPublishResult(o, outcome),
      state' = putOwner(state, id, { ...o, terminal: outcome }, OwnerPublished) }
  }
  action publishOwner(id: OwnerId): bool = {
    val o = state.owners.get(id)
    val outcome = if (o.wait == FailedWait or o.release == ThrownRelease) ThrownTerminal else
      if (o.release == FalseRelease) FalseTerminal else TrueTerminal
    publishOwnerResult(id, outcome)
  }
  pure def canPublishSDK(s: State): bool = s.sdkTerminal == PendingTerminal and
    OWNERS.forall(id => terminalSettled(s.owners.get(id).terminal))
  action publishSDK = {
    val failure = OWNERS.exists(id => state.owners.get(id).terminal == ThrownTerminal)
    val failedDelivery = OWNERS.exists(id => state.owners.get(id).terminal == FalseTerminal)
    all { canPublishSDK(state),
      state' = { ...state,
        sdkTerminal: if (failure) ThrownTerminal else if (failedDelivery) FalseTerminal else TrueTerminal,
        seen: state.seen.union(Set(SDKPublished)) } }
  }
  // Exporter-internal users are not SDK caller or worker users.
  pure def canBeginInternal(o: OwnerState): bool =
    o.exporterAdmission == Open and o.internalUsers == Set()
  action beginInternal(id: OwnerId): bool = {
    val o = state.owners.get(id)
    all { canBeginInternal(o),
      state' = putOwner(state, id, { ...o, internalUsers: Set(CallerOperation1) }, InternalUserBegan) }
  }
  action endInternal(id: OwnerId): bool = {
    val o = state.owners.get(id)
    all { o.internalUsers != Set(),
      state' = putOwner(state, id, { ...o, internalUsers: Set() }, InternalUserEnded) }
  }
  action retireExporter(id: OwnerId): bool = {
    val o = state.owners.get(id)
    all { o.exporterAdmission == Open,
      state' = putOwner(state, id, { ...o, exporterAdmission: Retired }, ExporterRetired) }
  }
  pure def canObserveWitness(o: OwnerState, confirmed: bool): bool =
    o.witness != ConfirmedWitness and
      (not(confirmed) or actualExporterSettled(o) or LYING_WITNESS)
  action observeWitness(id: OwnerId, confirmed: bool): bool = {
    val o = state.owners.get(id)
    all { canObserveWitness(o, confirmed),
      state' = putOwner(state, id,
        { ...o, witness: if (confirmed) ConfirmedWitness else UnconfirmedWitness },
        if (confirmed) WitnessConfirmed else WitnessUnknown) }
  }
  action observeDelivery = all {
    state.consumer.delivery == PendingTerminal, terminalSettled(state.sdkTerminal),
    state' = putConsumer(state, { ...state.consumer, delivery: state.sdkTerminal }, DeliveryObserved),
  }
  pure def refresh(s: State): ConsumerState = {
    val fs = OWNERS.mapBy(id => observeFactors(s.owners.get(id)))
    { ...s.consumer, proofVersion: 1, factors: fs,
      aggregate: terminalSettled(s.sdkTerminal) and OWNERS.forall(id => fs.get(id).confirmed),
      sdkTerminalObserved: terminalSettled(s.sdkTerminal),
      refreshes: s.consumer.refreshes + 1 }
  }
  action refreshProof = state' = putConsumer(state, refresh(state), ProofRefreshed)
  pure def cleanupAllowed(s: State): bool =
    s.consumer.proofVersion == 1 and terminalSettled(s.sdkTerminal) and
      (if (SDK_ONLY_CLEANUP) OWNERS.forall(id => s.consumer.factors.get(id).sdk)
       else s.consumer.aggregate)
  action retireSource = all {
    not(state.consumer.sourceRetired), cleanupAllowed(state),
    state' = putConsumer(state, { ...state.consumer, sourceRetired: true }, SourceRetired),
  }
  action persist = all {
    state.consumer.sourceRetired, not(state.consumer.persisted), cleanupAllowed(state),
    state' = putConsumer(state, { ...state.consumer, persisted: true }, Persisted),
  }
  action closeConnection = all {
    state.consumer.persisted, not(state.consumer.closed), cleanupAllowed(state),
    state' = putConsumer(state, { ...state.consumer, closed: true }, ConnectionClosed),
  }
  action step = {
    nondet id = OWNERS.oneOf()
    nondet op = Set(CallerOperation1, CallerOperation2, DrainOperation).oneOf()
    nondet success = Set(true, false).oneOf()
    nondet outcome = Set(TrueRelease, FalseRelease, OtherRelease, ThrownRelease).oneOf()
    nondet delivery = Set(TrueTerminal, FalseTerminal, ThrownTerminal).oneOf()
    any { admit(id, op), complete(id, op), retire(id), acceptDrain(id),
      beginExport(id, op), finishExport(id, op), exitWorker(id), join(id, success),
      callRelease(id), endRelease(id, outcome), publishOwnerResult(id, delivery), publishSDK,
      beginInternal(id), endInternal(id), retireExporter(id), observeWitness(id, success),
      observeDelivery, refreshProof, retireSource, persist, closeConnection }
  }
  val trustContract = OWNERS.forall(id => {
    val o = state.owners.get(id)
    (o.witness == ConfirmedWitness or o.release == TrueRelease) implies actualExporterSettled(o)
  })
  val cleanupOwnsNoUsers =
    (state.consumer.sourceRetired or state.consumer.persisted or state.consumer.closed) implies
      (terminalSettled(state.sdkTerminal) and OWNERS.forall(id =>
        sdkSettled(state.owners.get(id)) and actualExporterSettled(state.owners.get(id))))
  val cleanupOrder = (state.consumer.closed implies state.consumer.persisted) and
    (state.consumer.persisted implies state.consumer.sourceRetired)
  val proofFactorsAgree = and {
    state.consumer.factors.keys() == OWNERS,
    OWNERS.forall(id => {
      val f = state.consumer.factors.get(id)
      (f.sdk == (f.retired and f.callers and f.worker and f.terminal)) and
        (f.confirmed == (f.sdk and f.exporter))
    }),
    state.consumer.aggregate == (state.consumer.proofVersion == 1 and
      state.consumer.sdkTerminalObserved and
      OWNERS.forall(id => state.consumer.factors.get(id).confirmed)),
  }
  val releaseExactlyOnce = OWNERS.forall(id => state.owners.get(id).closeCount <= 1)
  val cachedDeliveryStable = state.consumer.delivery == PendingTerminal or
    state.consumer.delivery == state.sdkTerminal
  def reached(seam: Seam): bool = state.seen.contains(seam)
  val callerAdmittedReached = reached(CallerAdmitted)
  val callerCompletedReached = reached(CallerCompleted)
  val admissionRetiredReached = reached(AdmissionRetired)
  val drainAcceptedReached = reached(DrainAccepted)
  val exportBeganReached = reached(ExportBegan)
  val exportEndedReached = reached(ExportEnded)
  val workerExitedReached = reached(WorkerExited)
  val joinSucceededReached = reached(JoinSucceeded)
  val joinFailedReached = reached(JoinFailed)
  val releaseCalledReached = reached(ReleaseCalled)
  val releaseTrueReached = reached(ReleaseTrue)
  val releaseFalseReached = reached(ReleaseFalse)
  val releaseOtherReached = reached(ReleaseOther)
  val releaseThrewReached = reached(ReleaseThrew)
  val ownerPublishedReached = reached(OwnerPublished)
  val sdkPublishedReached = reached(SDKPublished)
  val exporterRetiredReached = reached(ExporterRetired)
  val internalUserBeganReached = reached(InternalUserBegan)
  val internalUserEndedReached = reached(InternalUserEnded)
  val witnessConfirmedReached = reached(WitnessConfirmed)
  val witnessUnknownReached = reached(WitnessUnknown)
  val deliveryObservedReached = reached(DeliveryObserved)
  val proofRefreshedReached = reached(ProofRefreshed)
  val sourceRetiredReached = reached(SourceRetired)
  val persistedReached = reached(Persisted)
  val connectionClosedReached = reached(ConnectionClosed)
}
module shutdownSettlementCorrected {
  import shutdownSettlement(SDK_ONLY_CLEANUP = false, ACCEPT_FALSE_RELEASE = false,
    ACCEPT_OTHER_RELEASE = false, ACCEPT_THROWN_RELEASE = false, LYING_WITNESS = false).*
}
```

### Deterministic settlement controls

These scenarios check the trusted premise and the permission boundary directly.
Mutation tests deliberately reach a state where `cleanupOwnsNoUsers` is false;
their green test result means the causal **red control** was detected, not that
the mutant satisfies the contract. The hostile-witness control also requires
`trustContract` to be false. The sampled corrected module checks that premise
alongside the resource-ownership invariant.

```quint target/formal/quint/shutdownSettlementTest.qnt +=
module shutdownSettlementCorrectedTest {
  import shutdownSettlement(SDK_ONLY_CLEANUP = false,
    ACCEPT_FALSE_RELEASE = false, ACCEPT_OTHER_RELEASE = false,
    ACCEPT_THROWN_RELEASE = false, LYING_WITNESS = false).*
    from "./shutdownSettlement"

  action settleOwner(id: OwnerId, outcome: Release): bool =
    retire(id).then(exitWorker(id)).then(join(id, true)).then(callRelease(id))
      .then(endRelease(id, outcome)).then(publishOwner(id))
  action prepareBoth(outcome: Release): bool =
    settleOwner(SpanOwner, outcome).then(settleOwner(MetricOwner, TrueRelease))
      .then(publishSDK).then(observeDelivery).then(refreshProof)

  run trustedPremiseAndSuccessfulCleanupTest = init
    .then(prepareBoth(TrueRelease)).expect(trustContract)
    .then(retireSource).then(persist).then(closeConnection)
    .expect(and { cleanupOwnsNoUsers, cleanupOrder, proofFactorsAgree,
      releaseExactlyOnce, cachedDeliveryStable, state.consumer.closed })

  run admittedCallerBlocksProofAfterWorkerExitTest = init
    .then(admit(SpanOwner, CallerOperation1))
    .then(beginExport(SpanOwner, CallerOperation1))
    .then(retire(SpanOwner)).then(exitWorker(SpanOwner)).then(join(SpanOwner, true))
    .then(callRelease(SpanOwner))
    .expect(not(canEndRelease(state.owners.get(SpanOwner), TrueRelease)))
    .then(finishExport(SpanOwner, CallerOperation1))
    .then(endRelease(SpanOwner, TrueRelease)).then(publishOwner(SpanOwner))
    .then(settleOwner(MetricOwner, TrueRelease)).then(publishSDK)
    .then(refreshProof).expect(not(cleanupAllowed(state)))
    .expect(not(canAdmit(state.owners.get(SpanOwner), CallerOperation2)))
    .then(complete(SpanOwner, CallerOperation1))
    // Completion does not magically update the consumer's old observation.
    .expect(not(cleanupAllowed(state))).then(refreshProof)
    .then(retireSource).then(persist).then(closeConnection)
    .expect(and { trustContract, cleanupOwnsNoUsers, state.consumer.refreshes == 2 })

  run acceptedDrainStartsAfterRetirementTest = init
    .then(acceptDrain(SpanOwner)).then(retire(SpanOwner))
    .expect(not(canExit(state.owners.get(SpanOwner)))).then(beginExport(SpanOwner, DrainOperation))
    .then(finishExport(SpanOwner, DrainOperation)).then(exitWorker(SpanOwner))
    .then(join(SpanOwner, true)).then(callRelease(SpanOwner))
    .then(endRelease(SpanOwner, TrueRelease)).then(publishOwner(SpanOwner))
    .expect(not(canBegin(state.owners.get(SpanOwner), DrainOperation)))
    .then(settleOwner(MetricOwner, TrueRelease)).then(publishSDK).then(refreshProof)
    .then(retireSource).then(persist).then(closeConnection)
    .expect(and { trustContract, cleanupOwnsNoUsers })

  run allComponentsAndSDKPublicationRequiredTest = init
    .then(settleOwner(SpanOwner, TrueRelease)).then(refreshProof)
    .expect(not(cleanupAllowed(state))).expect(not(canPublishSDK(state)))
    .then(settleOwner(MetricOwner, TrueRelease)).then(refreshProof)
    .expect(not(cleanupAllowed(state))).then(publishSDK).then(refreshProof)
    .then(retireSource).then(persist).then(closeConnection)
    .expect(cleanupOwnsNoUsers)

  run nonTrueReleaseBlocksCleanupTest = {
    nondet outcome = Set(FalseRelease, OtherRelease, ThrownRelease).oneOf()
    init.then(prepareBoth(outcome)).expect(not(cleanupAllowed(state)))
      .then(observeWitness(SpanOwner, false)).then(refreshProof).expect(not(cleanupAllowed(state)))
      .expect(and { trustContract, not(state.consumer.aggregate),
        state.owners.get(SpanOwner).closeCount == 1 })
  }

  run failedDeliveryLaterCleanupWithoutRepeatedShutdownTest = init
    .then(beginInternal(SpanOwner)).then(prepareBoth(FalseRelease))
    .expect(not(cleanupAllowed(state))).expect(not(canObserveWitness(state.owners.get(SpanOwner), true)))
    .then(retireExporter(SpanOwner)).expect(not(canObserveWitness(state.owners.get(SpanOwner), true)))
    .then(endInternal(SpanOwner)).then(observeWitness(SpanOwner, true))
    .then(refreshProof).then(retireSource).then(persist).then(closeConnection)
    .then(refreshProof)
    .expect(and { trustContract, cleanupOwnsNoUsers, cachedDeliveryStable,
      state.consumer.delivery == FalseTerminal,
      state.owners.get(SpanOwner).closeCount == 1,
      state.consumer.refreshes == 3 })

  run failedWaitCanSettleLaterWithTrustedWitnessTest = init
    .then(admit(SpanOwner, CallerOperation1)).then(retire(SpanOwner))
    .then(join(SpanOwner, false)).expect(not(canCallRelease(state.owners.get(SpanOwner))))
    .then(publishOwner(SpanOwner)).then(settleOwner(MetricOwner, TrueRelease))
    .then(publishSDK).then(observeDelivery).then(refreshProof).expect(not(cleanupAllowed(state)))
    .then(complete(SpanOwner, CallerOperation1)).then(exitWorker(SpanOwner))
    .then(retireExporter(SpanOwner)).then(observeWitness(SpanOwner, true))
    .then(refreshProof).then(retireSource).then(persist).then(closeConnection)
    .expect(and { trustContract, cleanupOwnsNoUsers, cachedDeliveryStable,
      state.consumer.delivery == ThrownTerminal,
      state.owners.get(SpanOwner).closeCount == 0 })

  run momentaryIdleIsNotStableWitnessTest = init
    .then(prepareBoth(OtherRelease)).expect(not(canObserveWitness(state.owners.get(SpanOwner), true)))
    .expect(not(cleanupAllowed(state))).then(beginInternal(SpanOwner)).then(endInternal(SpanOwner))
    .expect(not(canObserveWitness(state.owners.get(SpanOwner), true))).then(retireExporter(SpanOwner))
    .then(observeWitness(SpanOwner, true)).expect(not(canBeginInternal(state.owners.get(SpanOwner))))
    .then(refreshProof).then(retireSource).then(persist).then(closeConnection)
    .expect(and { trustContract, cleanupOwnsNoUsers })

  // A blocked action's .fail() is terminal: Quint does not carry state through
  // a failed action. Continued scenarios above inspect the action's same guard.
  run retiredAdmissionActuallyDisabledTest = init.then(retire(SpanOwner))
    .then(admit(SpanOwner, CallerOperation1).fail())
  run idleWitnessActuallyDisabledTest = init
    .then(observeWitness(SpanOwner, true).fail())
  run falseReleaseCleanupActuallyDisabledTest = init.then(prepareBoth(FalseRelease))
    .then(retireSource.fail())
  run otherReleaseCleanupActuallyDisabledTest = init.then(prepareBoth(OtherRelease))
    .then(retireSource.fail())
  run thrownReleaseCleanupActuallyDisabledTest = init.then(prepareBoth(ThrownRelease))
    .then(retireSource.fail())
  run pendingDrainWorkerExitActuallyDisabledTest = init
    .then(acceptDrain(SpanOwner)).then(retire(SpanOwner)).then(exitWorker(SpanOwner).fail())

  run absentWorkerDrainAdmissionActuallyDisabledTest = initWithAbsent(Set(SpanOwner))
    .expect(not(canAcceptDrain(state.owners.get(SpanOwner))))
    .then(acceptDrain(SpanOwner).fail())

  run absentWorkerStillTracksAdmittedCallerTest = initWithAbsent(Set(SpanOwner))
    .then(admit(SpanOwner, CallerOperation1)).then(retire(SpanOwner))
    .then(join(SpanOwner, true)).then(callRelease(SpanOwner))
    .then(endRelease(SpanOwner, TrueRelease)).then(publishOwner(SpanOwner))
    .then(settleOwner(MetricOwner, TrueRelease)).then(publishSDK).then(refreshProof)
    .expect(not(cleanupAllowed(state))).then(complete(SpanOwner, CallerOperation1))
    .then(refreshProof).then(retireSource).then(persist).then(closeConnection)
    .expect(and { trustContract, cleanupOwnsNoUsers, proofFactorsAgree,
      state.owners.get(SpanOwner).worker == Absent })

  run falseDeliveryWithTrueReleaseStillAllowsCleanupTest = init
    .then(retire(SpanOwner)).then(exitWorker(SpanOwner)).then(join(SpanOwner, true))
    .then(callRelease(SpanOwner)).then(endRelease(SpanOwner, TrueRelease))
    .then(publishOwnerResult(SpanOwner, FalseTerminal))
    .then(settleOwner(MetricOwner, TrueRelease)).then(publishSDK).then(observeDelivery)
    .then(refreshProof).then(retireSource).then(persist).then(closeConnection)
    .expect(and { trustContract, cleanupOwnsNoUsers, cachedDeliveryStable,
      state.consumer.delivery == FalseTerminal, state.consumer.closed })
}
module shutdownSettlementSDKOnlyMutantTest {
  import shutdownSettlement(SDK_ONLY_CLEANUP = true,
    ACCEPT_FALSE_RELEASE = false, ACCEPT_OTHER_RELEASE = false,
    ACCEPT_THROWN_RELEASE = false, LYING_WITNESS = false).* from "./shutdownSettlement"
  action settleOwner(id: OwnerId, outcome: Release): bool =
    retire(id).then(exitWorker(id)).then(join(id, true)).then(callRelease(id))
      .then(endRelease(id, outcome)).then(publishOwner(id))
  run forbiddenCleanupControlTest = init
    .then(settleOwner(SpanOwner, FalseRelease)).then(settleOwner(MetricOwner, TrueRelease))
    .then(publishSDK).then(refreshProof).then(retireSource).then(persist).then(closeConnection)
    .expect(and { not(cleanupOwnsNoUsers), state.consumer.closed, trustContract })
}
module shutdownSettlementFalseReleaseMutantTest {
  import shutdownSettlement(SDK_ONLY_CLEANUP = false,
    ACCEPT_FALSE_RELEASE = true, ACCEPT_OTHER_RELEASE = false,
    ACCEPT_THROWN_RELEASE = false, LYING_WITNESS = false).* from "./shutdownSettlement"
  action settleOwner(id: OwnerId, outcome: Release): bool =
    retire(id).then(exitWorker(id)).then(join(id, true)).then(callRelease(id))
      .then(endRelease(id, outcome)).then(publishOwner(id))
  run forbiddenCleanupControlTest = init
    .then(settleOwner(SpanOwner, FalseRelease)).then(settleOwner(MetricOwner, TrueRelease))
    .then(publishSDK).then(refreshProof).then(retireSource).then(persist).then(closeConnection)
    .expect(and { not(cleanupOwnsNoUsers), state.consumer.closed, trustContract })
}
module shutdownSettlementOtherReleaseMutantTest {
  import shutdownSettlement(SDK_ONLY_CLEANUP = false,
    ACCEPT_FALSE_RELEASE = false, ACCEPT_OTHER_RELEASE = true,
    ACCEPT_THROWN_RELEASE = false, LYING_WITNESS = false).* from "./shutdownSettlement"
  action settleOwner(id: OwnerId, outcome: Release): bool =
    retire(id).then(exitWorker(id)).then(join(id, true)).then(callRelease(id))
      .then(endRelease(id, outcome)).then(publishOwner(id))
  run forbiddenCleanupControlTest = init
    .then(settleOwner(SpanOwner, OtherRelease)).then(settleOwner(MetricOwner, TrueRelease))
    .then(publishSDK).then(refreshProof).then(retireSource).then(persist).then(closeConnection)
    .expect(and { not(cleanupOwnsNoUsers), state.consumer.closed, trustContract })
}
module shutdownSettlementThrownReleaseMutantTest {
  import shutdownSettlement(SDK_ONLY_CLEANUP = false,
    ACCEPT_FALSE_RELEASE = false, ACCEPT_OTHER_RELEASE = false,
    ACCEPT_THROWN_RELEASE = true, LYING_WITNESS = false).* from "./shutdownSettlement"
  action settleOwner(id: OwnerId, outcome: Release): bool =
    retire(id).then(exitWorker(id)).then(join(id, true)).then(callRelease(id))
      .then(endRelease(id, outcome)).then(publishOwner(id))
  run forbiddenCleanupControlTest = init
    .then(settleOwner(SpanOwner, ThrownRelease)).then(settleOwner(MetricOwner, TrueRelease))
    .then(publishSDK).then(refreshProof).then(retireSource).then(persist).then(closeConnection)
    .expect(and { not(cleanupOwnsNoUsers), state.consumer.closed, trustContract })
}
module shutdownSettlementHostileWitnessMutantTest {
  import shutdownSettlement(SDK_ONLY_CLEANUP = false,
    ACCEPT_FALSE_RELEASE = false, ACCEPT_OTHER_RELEASE = false,
    ACCEPT_THROWN_RELEASE = false, LYING_WITNESS = true).* from "./shutdownSettlement"
  action settleOwner(id: OwnerId, outcome: Release): bool =
    retire(id).then(exitWorker(id)).then(join(id, true)).then(callRelease(id))
      .then(endRelease(id, outcome)).then(publishOwner(id))
  run forbiddenCleanupControlTest = init
    .then(settleOwner(SpanOwner, FalseRelease)).then(settleOwner(MetricOwner, TrueRelease))
    .then(publishSDK).then(beginInternal(SpanOwner)).then(observeWitness(SpanOwner, true))
    .then(refreshProof).then(retireSource).then(persist).then(closeConnection)
    .expect(and { not(cleanupOwnsNoUsers), state.consumer.closed, not(trustContract) })
}
```
