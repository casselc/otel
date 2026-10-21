# SDK-owned ordinary-map encoder view

UNADOPTED prototype based on the exporter's actual pinned SDK6cd57c86, not SDK
maincd55981f. The latter does not contain the stacked map-cache/shape/view API;
this work must not implicitly merge its ancestors. No new dependency pins,
default capture/conversion behavior, delivery contract or model changes.

`scalar-text-payload-projector` optionally accepts `:ordinary-map-view-box` from
a trusted native codec. Its constructor supplies all eight ORIGINAL SDK policy
anchors and the original maximum; the existing native admission body is shared
with the map-returning helper, not reimplemented in the codec. The old helper
explicitly selects its unchanged copying path. Ordinary-map admission remains
9..128entries, String keys, bounded Strings/Booleans/native fixnums, no collision
buckets. Unsupported shapes retain nil/fallback behavior. Only the opt-in view
skips replacement map/numeric String construction; it is NOT a normal map.

The codec's separate native box creates a metadata-free header sharing the
input root. It retains no source wrapper/metadata and returns fresh wire output,
but DOES retain immutable input nodes/values through the view lifetime. This is
an explicit immutable-input/trusted-codec contract, NOT an enforced linear loan,
automatic release, thread-safe policy lock or proof against deliberate mutation
of SDK internals. Views are not stored in the existing bounded root cache or
in any new global/per-codec value cache. Sorted shape plans remain unchanged.
Repeated ordinary-root workloads can lose their previous normalized-root/wire
reuse because opted-in direct results bypass store; qualify that before adoption.

Qualified locally on official stock Jolt0.8.23/Chez10.4.1 with codec candidate
`/home/chuck/ai-src/worktrees/exporter-direct-scalar-map-codec-20261021`.
The component itself has no SDK dependency; its full SDK/exporter consumer
still resolves ecosystem forks. No compiler/data.json patch is required here.

Tests:10/172 direct+existing view;27/389 combined direct/view/plain/sorted/shape;
19/162 cache+provided-folder through its REQUIRED stock runner, all pass.
All eight anchors are tested both after and before new projector construction,
without calling replacements. Collision, limits, fresh values, metadata/header
identity, raw-root sharing, output mutation/depth/budget and failure-stop controls
are included. The actual exporter also passes11 equivalent-output/fallback
assertions, including custom key/value policies.

The initial combined test command incorrectly bypassed the stock cache
runner's constructor/folder injection, producing8failures/59errors. Its complete
log is preserved as `sdk-controls-unwrapped-attempt.log`; no product repair or
gate weakening was used to hide that setup error. The existing official runner
passes19/162 on the same sources. The standalone codec source gate passes43/596.
These are source-mode gates, not a new standalone/exporter/native qualification.

Original mutation controls:unquoted integers cause21 failures/0errors; retaining
the input wrapper causes4failures/0errors at the real identity/metadata oracle.
The first unquoted log printed large synthetic boundary values (~6MiB); it is
compressed losslessly to~20KiB, not committed uncompressed. Comparisons now
report Boolean equality, keeping failures bounded without weakening predicates.
The timing and product sources did not change for that report-only adjustment.

See the exporter research README for the complete rich screen:estimated
allocation8983.04→8464.59B/row(-5.77%), p50178.13→172.94ms(-2.91%), but sample
p99/max196.93→317.63ms WORSE. Every complete8647784-byte output matches; all raw
p50/p90/p95/p99/counters retained. This is NOT a Durable win or adoption gate.
The worst candidate sample's1.84ms GC pause does not explain its317.63ms wall
time; observing-threadCPU317.64ms is not process-wide CPU or a root-cause proof.
No repeated favorable run, native sweep, queue/recovery/S3/model claim is made.

## Read-only review and adjudication

Claude found no blocker to publishing this as unadopted source/prototype work;
its unedited report is `claude-review.txt`. No execution or publication by the
reviewer. The missing codec43/596 log it identified was subsequently captured
at exporter `research/direct-scalar-map-owner-20261021/codec-source-controls.log`.
That is a SOURCE run of a test namespace named standalone-smoke-test, not a new
compiled standalone gate. No timing/product source was rerun/changed for it.

Clarification to the advisory review:the pre-existing scalar-text projector
without the new option can ALREADY return sorted-map opaque views. Only the
ordinary-map path and separate legacy map-returning API remain map-returning.
Do not generalize its "no-option arity returns a plain map" wording to every
input. The preserved incorrect-runner log does contain failures/errors; its
absence from the review's positive-log file fence is not evidence of no failed
attempts. Trust/loan/retention and repeated-root caveats remain adoption gates.
