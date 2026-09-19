#!/usr/bin/env bash
set -euo pipefail

# Decide the two independently sampled Quint campaigns from exact Git objects.
# Any absent provenance or failed effective-input comparison selects both.

default_repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
repo_root=${OTEL_SHUTDOWN_MODEL_CLASSIFIER_REPO:-$default_repo}

usage() {
  echo "usage: $0 --paths [PATH ...] | --diff BASE HEAD [--merge-base]" >&2
  exit 2
}

is_zero_revision() { [[ $1 =~ ^0+$ ]]; }

emit_decision() {
  local lifecycle=$1 settlement=$2 reason=$3 changed_count=$4
  printf 'lifecycle=%s\n' "$lifecycle"
  printf 'settlement=%s\n' "$settlement"
  printf 'reason=%s\n' "$reason"
  printf 'changed_count=%s\n' "$changed_count"
}

fail_closed() {
  emit_decision true true diff-command-failed 0
  exit 0
}

is_shared_input() {
  case "$1" in
    .github/workflows/shutdown-lifecycle-quint.yml | \
    scripts/check-shutdown-lifecycle-quint.sh | \
    scripts/classify-shutdown-lifecycle-quint-paths.sh | \
    scripts/fingerprint-shutdown-lifecycle-quint-inputs.sh | \
    test/shutdown-lifecycle-model-path-classifier.sh)
      return 0 ;;
    *) return 1 ;;
  esac
}

is_correspondence_input() {
  # The document maps both campaigns onto these SDK seams and their shared
  # trace tests. Keep this closure deliberately broad until settlement gains a
  # separate runtime trace vocabulary.
  case "$1" in
    src/otel/sdk.clj | \
    src/otel/sdk/lifecycle.clj | \
    src/otel/sdk/export.clj | \
    src/otel/sdk/logs.clj | \
    src/otel/sdk/metrics.clj | \
    test/otel/sdk/lifecycle_events.clj | \
    test/otel/sdk/lifecycle_test.clj)
      return 0 ;;
    *) return 1 ;;
  esac
}

classify_paths() {
  local path count=0 lifecycle=false settlement=false first=""
  for path in "$@"; do
    count=$((count + 1))
    if is_shared_input "$path" || is_correspondence_input "$path" || \
       [[ $path == formal/quint/shutdown-lifecycle.md ]]; then
      lifecycle=true
      settlement=true
      [[ -n $first ]] || first=$path
    fi
  done
  if [[ -n $first ]]; then
    printf -v first '%q' "$first"
    emit_decision "$lifecycle" "$settlement" "model-input:$first" "$count"
  else
    emit_decision false false model-inputs-byte-identical "$count"
  fi
}

effective_document_only() {
  local path saw_document=false
  (( $# > 0 )) || return 1
  for path in "$@"; do
    case "$path" in
      formal/quint/shutdown-lifecycle.md) saw_document=true ;;
      README.md | CHANGELOG.md) ;;
      *) return 1 ;;
    esac
  done
  [[ $saw_document == true ]]
}

compare_campaign() {
  local campaign=$1 base=$2 head=$3 helper="$default_repo/scripts/fingerprint-shutdown-lifecycle-quint-inputs.sh"
  local base_fingerprint head_fingerprint
  if base_fingerprint=$(bash "$helper" "$repo_root" "$base" "$campaign" 2>/dev/null) && \
     head_fingerprint=$(bash "$helper" "$repo_root" "$head" "$campaign" 2>/dev/null) && \
     [[ $base_fingerprint =~ ^[a-f0-9]{64}$ && $head_fingerprint =~ ^[a-f0-9]{64}$ ]]; then
    [[ $base_fingerprint != "$head_fingerprint" ]]
    return
  fi
  return 0
}

[[ $# -ge 1 ]] || usage
mode=$1
shift
case "$mode" in
  --paths)
    classify_paths "$@"
    ;;
  --diff)
    [[ $# -ge 2 && $# -le 3 ]] || usage
    if [[ $# -eq 3 && ${3:-} != --merge-base ]]; then usage; fi
    base=$1
    head=$2
    use_merge_base=false
    [[ ${3:-} == --merge-base ]] && use_merge_base=true
    if [[ -z $base || -z $head ]] || is_zero_revision "$base" || is_zero_revision "$head"; then
      emit_decision true true missing-or-zero-diff-boundary 0
      exit 0
    fi
    git -C "$repo_root" cat-file -e "$base^{commit}" 2>/dev/null || fail_closed
    git -C "$repo_root" cat-file -e "$head^{commit}" 2>/dev/null || fail_closed
    diff_base=$base
    if [[ $use_merge_base == true ]]; then
      if ! diff_base=$(git -C "$repo_root" merge-base "$base" "$head" 2>/dev/null); then fail_closed; fi
      [[ -n $diff_base ]] || fail_closed
      git -C "$repo_root" cat-file -e "$diff_base^{commit}" 2>/dev/null || fail_closed
    fi
    diff_file=$(mktemp)
    cleanup() { rm -f -- "$diff_file"; }
    trap cleanup EXIT HUP INT TERM
    if ! git -C "$repo_root" diff --no-renames --name-only \
         --diff-filter=ACDMRTUXB -z "$diff_base" "$head" -- > "$diff_file"; then
      fail_closed
    fi
    paths=()
    while IFS= read -r -d '' path; do paths+=("$path"); done < "$diff_file"
    if effective_document_only "${paths[@]}"; then
      lifecycle=false
      settlement=false
      if compare_campaign lifecycle "$diff_base" "$head"; then lifecycle=true; fi
      if compare_campaign settlement "$diff_base" "$head"; then settlement=true; fi
      if [[ $lifecycle == false && $settlement == false ]]; then
        emit_decision false false effective-model-inputs-identical "${#paths[@]}"
      else
        emit_decision "$lifecycle" "$settlement" effective-model-inputs-changed-or-unavailable "${#paths[@]}"
      fi
    else
      classify_paths "${paths[@]}"
    fi
    ;;
  *) usage ;;
esac
