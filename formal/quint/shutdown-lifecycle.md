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

The fast gate tangles this document, typechecks both generated files, runs the
deterministic corrected and mutation scenarios, and samples the corrected
invariants and reachability witnesses:

```sh
scripts/check-shutdown-lifecycle-quint.sh
```

It requires Quint 0.32.0 and `lmt` at commit
`62fe18f2f6a6e11c158ff2b2209e1082a4fcd59c`. Simulation is sampled evidence,
not exhaustive proof. The dedicated CI workflow is path-filtered to lifecycle
model, implementation, and runtime-test artifacts.

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
