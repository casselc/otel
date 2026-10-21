# Guarded ordinary-map view prototype

Prototype SDK source `4451a4c07cb40ba5ccb06f8020370f230a11c9f2`, codec source
`4650e3542705e7698d8895ff95cf9b43e8f2288a` in the owned exporter repository.
No production pin, default or Durable contract has changed. Full-pipeline
qualification remains pending; this is not an adopted throughput fix.

## Mechanism and correctness

The encoder-only projector now tries a learned view after the existing source
root cache but before copying a new ordinary scalar map. It learns only after
the original SDK helper accepts. The codec matches a key-only template against
the current HAMT's bitmap/array/key geometry and fills fresh value slots.
No input roots, wrappers or values are retained in plans. Scalar limits and all
eight original SDK policy fences remain; schemas/characters remain bounded.
Collision nodes and maps of eight or fewer entries retain the old fallback.
Mismatched geometry declines; this does not assume that every map construction
history produces identical geometry. Map-returning conversion is unchanged.

Local stock Jolt 0.8.23 / Chez 10.4.1 gates:

- Codec: **16 tests / 393 assertions**, including unchanged sorted behavior,
  reversed insertion, metadata, changed values, collision rejection, budgets
  and policy changes. Equal-hash `Aa`→`BB` replacement cannot reuse a stale
  template. A deliberate key-guard removal emits the wrong key and is detected.
- SDK projector: **7 / 110**, including all eight policy anchors, fallback,
  fresh output and copying-helper count **3 disabled versus 1 enabled** for
  three distinct admitted maps. Same input-root hits still precede plans.
- SDK aggregate: **486 / 5575**, zero failures/errors. Expected fault-control
  worker diagnostics are not test failures. The test alias alone opens a REPL;
  the actual gate is `jolt -Srepro -M:test -m otel.test-runner`.
- New SDK with actual older published sorted-only codec `68ee1ca8…` passes
  explicit construction/sorted-view/ordinary-map fallback checks.
- The same copying-helper feature test against the actual canonical old SDK
  is a red control: the enabled path still copies three times, not once.

The isolated mechanism test now rejects native `false` as an observed result:
`some? false` alone could previously pass a zero-allocation observation. Existing
wire parity and activation gates were independent; this is not evidence that
prior full-pipeline qualification was invalid.

Independent Claude source review found no correctness blocker. Codex owns the
executed gates; this is not a security/DoS proof, and the reviewer's broader
claim that HAMT geometry is necessarily independent of construction history
is not relied on. The actual matcher checks geometry and may decline safely.

## Component measurement, not Durable throughput

The source and default-release screens each use one process and the same
binary/rows for disabled→enabled→enabled→disabled. Two warmups and 16 measured
5,000-map batches per arm. Each batch creates a fresh payload projector and
encodes actual output. Every complete byte array matches the disabled control.
No redefinitions/private probes are used in the benchmark. Both arms run the
**new SDK**; this is not a direct old-SDK timing comparison.

| Release arm | HAMT views | p50 ms | p90 ms | p95 / p99 ms | Estimated B/row |
|---|---|---:|---:|---:|---:|
| A1 | disabled |24.53|43.83|99.03|1686.34|
| B1 | enabled |27.88|37.37|41.80|976.15|
| B2 | enabled |14.84|24.03|25.65|976.96|
| A2 | disabled |20.03|33.96|34.44|1685.62|

Estimated allocation falls ~710B/map (~42%). Total release CPU time across
the two enabled arms was ~704ms versus ~837ms disabled, but medians are mixed:
the first enabled median regresses. Disabled-arm observed GC wall time was
~64ms versus ~2.5ms enabled; counters have different scopes, so do not subtract
process GC wall time from thread CPU to manufacture an exclusive CPU result.
Source-mode medians improved in both pairs; they do not override the mixed
release result. No favorable-sample reruns or stable wall/tail win is claimed.

CPU is current OS-thread time; memory/GC are process-wide, separate non-atomic
snapshots. Equality comparison copies arrays outside the window in both arms,
which can affect later GC. At 16 samples p95/p99 are maxima, not population
tails. There is no native insert, writer queue, persistence, recovery, paced
application or Langfuse in this component screen. Raw samples/hashes are in
`results/`; reports are saved after each completed arm.

Run the component with the explicitly selected codec dependency, SDK resource
path and benchmark path (`src`, `resources`, `bench`). For standalone release,
use a small packaging project declaring those paths/dependencies and invoke
`jolt build` first under the mandatory Chez wrapper. The workspace packaging
receipt records exact local roots; no compiler fork is required.

## Next gate

Qualify the rich real SDK/Durable pipeline with exact source pins and independent
full-row recovery before adoption. Preserve the old matched graph and compare
controlled HAMT selection, not different runtimes or silently changed fixtures.
The ~25k p50 / ~20k p99 confirmed-throughput targets remain unmet.

Tracked in https://github.com/casselc/otel/issues/73 and
https://github.com/chucklehead-dev/jolt-otel-clickhouse/issues/141.
