#!/usr/bin/env bash
set -Eeuo pipefail

caller_dir=$PWD
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "$script_dir/.." && pwd)
source "$repo_root/scripts/tools.sh"
validate_course_lang || exit $?

book_dir="$repo_root/docs/book"
cache_dir="$repo_root/.tools/tectonic-cache"
offline=${BOOK_OFFLINE-0}
case "$offline" in
  0|"") ;;
  1) ;;
  *) printf 'BOOK_OFFLINE must be 0 or 1 (got %s)\n' "$offline" >&2; exit 2 ;;
esac

if [[ "$COURSE_LANG" == en ]]; then
  source_file=simpleKafka.tex
  out_dir="$repo_root/build/book/en"
  pdf="$out_dir/simpleKafka.pdf"
else
  source_file=simpleKafka-zh.tex
  out_dir="$repo_root/build/book/zh"
  pdf="$out_dir/simpleKafka-zh.pdf"
fi
if [[ ! -f "$book_dir/$source_file" ]]; then
  printf 'Textbook source is missing: %s\n' "$book_dir/$source_file" >&2
  exit 2
fi

tectonic_bin=$(resolve_tectonic "$repo_root" "$caller_dir") || exit $?
bundle='https://relay.fullyjustified.net/default_bundle_v33.tar'
if [[ ${TEX_BUNDLE+x} ]]; then
  if [[ -z "$TEX_BUNDLE" ]]; then
    printf 'TEX_BUNDLE is set but empty; use a bundle URL, directory, .zip, or .ttb file.\n' >&2
    exit 2
  fi
  bundle=$TEX_BUNDLE
  if [[ "$bundle" != *://* ]]; then
    [[ "$bundle" == /* ]] || bundle="$caller_dir/$bundle"
    if [[ ! -d "$bundle" ]]; then
      case "$bundle" in
        *.zip|*.ttb)
          [[ -f "$bundle" ]] || { printf 'TEX_BUNDLE does not exist: %s\n' "$bundle" >&2; exit 2; } ;;
        *)
          printf 'TEX_BUNDLE must be a directory, .zip, .ttb, or URL (plain tar archives are not supported): %s\n' "$bundle" >&2
          exit 2 ;;
      esac
    fi
  fi
fi

mkdir -p -- "$out_dir" "$cache_dir"
rm -f -- "$pdf"
args=(-X compile "$source_file" --outdir "$out_dir" --keep-logs --untrusted --bundle "$bundle")
if [[ "$offline" == 1 ]]; then
  args+=(--only-cached)
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
