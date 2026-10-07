#!/usr/bin/env bash
set -Eeuo pipefail

caller_dir=$PWD
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "$script_dir/.." && pwd)
book_dir="$repo_root/docs/book"
out_dir="$repo_root/build/book"
cache_dir="$repo_root/.tools/tectonic-cache"
pdf="$out_dir/simpleKafka.pdf"

offline=${BOOK_OFFLINE:-0}
case "$offline" in
  0|"") ;;
  1) ;;
  *) printf 'BOOK_OFFLINE must be 0 or 1 (got %s)\n' "$offline" >&2; exit 2 ;;
esac
if [[ -d "$out_dir" ]]; then
  rm -f -- "$pdf"
fi

tectonic=${TECTONIC:-$repo_root/.tools/tectonic-0.17.0/tectonic}
if [[ "$tectonic" == */* ]]; then
  if [[ "$tectonic" != /* ]]; then
    tectonic="$caller_dir/$tectonic"
  fi
  if [[ ! -x "$tectonic" ]]; then
    printf 'Tectonic executable is unavailable: %s\nSet TECTONIC to an executable path.\n' "$tectonic" >&2
    exit 127
  fi
  tectonic_bin=$tectonic
else
  if ! tectonic_bin=$(command -v -- "$tectonic"); then
    printf 'Tectonic executable is unavailable: %s\nSet TECTONIC to an executable path.\n' "$tectonic" >&2
    exit 127
  fi
fi

mkdir -p -- "$out_dir" "$cache_dir"

args=(-X compile simpleKafka.tex --outdir "$out_dir" --keep-logs --untrusted)
if [[ "$offline" == 1 ]]; then
  args+=(--only-cached)
fi
if [[ -n ${TEX_BUNDLE:-} ]]; then
  bundle=$TEX_BUNDLE
  if [[ "$bundle" != *://* && "$bundle" != /* ]]; then
    bundle="$caller_dir/$bundle"
  fi
  args+=(--bundle "$bundle")
fi

if (
  cd -- "$book_dir"
  TECTONIC_CACHE_DIR="$cache_dir" "$tectonic_bin" "${args[@]}"
); then
  :
else
  status=$?
  rm -f -- "$pdf"
  exit "$status"
fi

if [[ ! -s "$pdf" ]]; then
  rm -f -- "$pdf"
  printf 'Tectonic exited successfully but did not create a non-empty PDF: %s\n' "$pdf" >&2
  exit 1
fi
printf 'PDF written: %s\n' "$pdf"
