#!/usr/bin/env bash
# Extract data at an exact Git revision; never execute that revision's scripts.
set -euo pipefail
[[ $# == 3 ]] || {
  echo "usage: $0 REPOSITORY REVISION lifecycle|settlement" >&2
  exit 2
}

exec python3 - "$1" "$2" "$3" <<'PY'
import hashlib
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile

repo, revision, campaign = sys.argv[1:]
if campaign not in {"lifecycle", "settlement"}:
    raise RuntimeError("unknown shutdown model campaign")

def git(*args):
    return subprocess.check_output(["git", "-C", repo, *args], timeout=10)

document = "formal/quint/shutdown-lifecycle.md"
closures = {
    "lifecycle": {
        "target/formal/quint/shutdownLifecycle.qnt",
        "target/formal/quint/shutdownLifecycleTest.qnt",
    },
    "settlement": {
        "target/formal/quint/shutdownSettlement.qnt",
        "target/formal/quint/shutdownSettlementTest.qnt",
    },
}
all_outputs = set().union(*closures.values())
required = [
    ".github/workflows/shutdown-lifecycle-quint.yml",
    "scripts/check-shutdown-lifecycle-quint.sh",
    "scripts/classify-shutdown-lifecycle-quint-paths.sh",
    "test/shutdown-lifecycle-model-path-classifier.sh",
]
# A missing fingerprint helper is accepted only at the pre-introduction
# revision. Adding it changes the inventory and therefore selects both samples.
optional = "scripts/fingerprint-shutdown-lifecycle-quint-inputs.sh"
tool = shutil.which("lmt")
if not tool:
    raise RuntimeError("missing pinned tangler")
metadata = subprocess.check_output(["go", "version", "-m", tool], timeout=10).decode()
if not re.search(r"(?m)^\s*mod\s+github.com/driusan/lmt\s+v0\.0\.0-20210421124901-62fe18f2f6a6\s", metadata):
    raise RuntimeError("tangler version mismatch")
tool_hash = hashlib.sha256(pathlib.Path(tool).read_bytes()).hexdigest()

tree = git("ls-tree", "-rz", "--full-tree", revision).split(b"\0")
blobs = {}
for entry in tree:
    if not entry:
        continue
    description, path = entry.split(b"\t", 1)
    mode, kind, oid = description.decode().split()
    path = path.decode()
    if path == document or path in required or path == optional:
        if mode not in {"100644", "100755"} or kind != "blob":
            raise RuntimeError("nonregular effective input")
        blobs[path] = git("cat-file", "blob", oid)
if any(path not in blobs for path in [document, *required]):
    raise RuntimeError("missing effective input")

fence = re.compile(r"^`{3,}\s?([\w\+]+)\s+([\w\.\-/]+)\s*([+][=])?$")
document_data = blobs[document]
if len(document_data) > 4 * 1024 * 1024:
    raise RuntimeError("oversized literate source")
for line in document_data.decode().splitlines():
    match = fence.fullmatch(line.strip())
    if match and (match[1] != "quint" or match[2] not in all_outputs or match[3] != "+="):
        raise RuntimeError("unknown extraction output")

fingerprint = hashlib.sha256()
def include(path, data):
    fingerprint.update(path.encode() + b"\0" + str(len(data)).encode() + b"\0" + data)

include("fingerprint-schema", b"1")
include("campaign", campaign.encode())
include("pinned-lmt-binary", tool_hash.encode())
with tempfile.TemporaryDirectory(prefix="otel-shutdown-effective-model.") as directory:
    root = pathlib.Path(directory)
    destination = root / document
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(document_data)
    (root / "target/formal/quint").mkdir(parents=True)
    subprocess.run([tool, document], cwd=root, timeout=20, check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    actual = set()
    for path in (root / "target").rglob("*"):
        if path.is_symlink():
            raise RuntimeError("symlink extraction output")
        if path.is_file():
            actual.add(path.relative_to(root).as_posix())
    if actual != all_outputs:
        raise RuntimeError("extraction inventory mismatch")
    for path in sorted(closures[campaign]):
        data = (root / path).read_bytes()
        if len(data) > 4 * 1024 * 1024:
            raise RuntimeError("oversized extraction output")
        include(path, data)
for path in sorted(blobs):
    if path != document:
        include(path, blobs[path])
print(fingerprint.hexdigest())
PY
