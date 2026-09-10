# Attribute-schema artifact discovery

`otel.attribute-schema.discovery` merges schema fragments published by libraries
and local advice packs into one deterministic build artifact. Discovery is
explicit: an application names every classpath index resource it approves. The
implementation never scans a resource directory and never treats whichever
classpath entry happens to appear first as authoritative.

Each publisher writes a closed `otel.attribute-schema.index/v1` document. One
index makes a single artifact claim and lists its fragment resources:

```clojure
{:schema "otel.attribute-schema.index/v1"
 :artifact {:package "io.github.example/checkout-lib"
            :repository "https://github.com/example/checkout-lib"
            :revision "0123456789abcdef0123456789abcdef01234567"}
 :fragments
 [{:path "META-INF/otel/attribute-schema/checkout-lib.edn"
   :sha256 "4f...64-lowercase-hex-characters...9a"}]}
```

`:revision` is a full lowercase 40-character revision. A publisher may instead
use an immutable semantic `:version`, but never both. The canonical identity of
one fragment is the artifact's `:package`, `:repository`, revision or version,
plus the fragment `:path` and SHA-256. The digest covers the exact UTF-8 resource
text, including its final newline. Fragment paths are normalized relative paths
under `META-INF/otel/attribute-schema/` and must be globally unique among the
selected indexes.

A build can read explicitly named classpath indexes:

```clojure
(require '[otel.attribute-schema.discovery :as discovery])

(discovery/discover-resources
 ["META-INF/otel/indexes/checkout-lib.edn"
  "META-INF/otel/indexes/checkout-advice.edn"]
 {:include #{"io.github.example/checkout-lib" "local/checkout-advice"}
  :exclude #{"local/disabled-advice"}})
```

Exclusion wins when a package appears in both sets. A non-empty include set
fails if an included package has no supplied index. Build systems that already
own resource loading can call `discover` with the raw index texts and an
injected resource reader; this is the shared merge implementation.

Every index and fragment is structurally validated before merging. Missing or
unreadable resources, digest drift, duplicate locators, different digests for
one locator, conflicting repository claims, and resource-path collisions fail
closed. An index or fragment resource must contain exactly one EDN value; a
valid first value followed by another form or junk is rejected. Valid identities
and every map in the output are canonicalized before rendering, so shuffled
index order or input map insertion order renders byte-identical bundle EDN.
Each bundle retains artifact identity beside that fragment's project-relative
source list, even when two artifacts both contain `src/shared.clj`. The merged
`otel.attribute-schema/v1` view is checked against the pinned
`otel.semantic-conventions` registry both during discovery and again at the
public rendering boundary. Unknown and dynamic evidence remains visible on the
fallback path.

`discover-resources` enumerates all matches for each explicitly named index and
fragment through the active Clojure/Jolt class loader and requires exactly one.
A shadowed or duplicate resource therefore fails closed instead of silently
using the first classpath match. This checks resource uniqueness and content;
portable classpath APIs do not attest that an index and fragment came from the
same physical JAR or checkout. The artifact identity remains a validated
publisher claim selected by the consuming build, not container-signing proof.

Diagnostics contain only bounded error classifications (or the pinned
semantic-convention mismatch fields). They do not copy index text, fragment
contents, absolute paths, environment values, resolver exceptions, or observed
telemetry into CI logs.

## Ownership boundaries

- Source inference reads only explicitly listed source as data and produces one
  publisher's `otel.attribute-schema/v1` fragment. It does not expand arbitrary
  macros.
- Artifact discovery verifies publisher identity, resource provenance and
  digests, then produces a pure `otel.attribute-schema.bundle/v1` build value.
  It does not execute dependency or advice code.
- An operator-approved storage manifest is a downstream projection with its own
  review and policy. Discovery does not generate DDL, select a database type,
  install a table or mutate a schema.
- Runtime attribute validation continues to enforce the AnyValue contract.
  Discovery never observes runtime values and runtime observations never alter
  the build artifact.
