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
  0|"") offline=0 ;;
  1) ;;
  *) printf 'BOOK_OFFLINE must be 0 or 1 (got %s)\n' "$offline" >&2; exit 2 ;;
esac

if [[ "$COURSE_LANG" == en ]]; then
  source_file=simpleKafka.tex
  out_dir="$repo_root/build/book/en"
  basename=simpleKafka
  language=en
else
  source_file=simpleKafka-zh.tex
  out_dir="$repo_root/build/book/zh"
  basename=simpleKafka-zh
  language=zh-CN
fi
pdf="$out_dir/$basename.pdf"
epub="$out_dir/$basename.epub"
work_dir="$out_dir/epub-work"
dist_dir="$repo_root/docs/book/dist/$COURSE_LANG"
dist_pdf="$dist_dir/$basename.pdf"
dist_epub="$dist_dir/$basename.epub"
if [[ ! -f "$book_dir/$source_file" ]]; then
  printf 'Textbook source is missing: %s\n' "$book_dir/$source_file" >&2
  exit 2
fi

tectonic_bin=$(resolve_tectonic "$repo_root" "$caller_dir") || exit $?
pandoc_bin=$(resolve_pandoc "$repo_root" "$caller_dir") || exit $?
pdftoppm_bin=$(type -P pdftoppm) || pdftoppm_bin=
if [[ -z "$pdftoppm_bin" || ! -f "$pdftoppm_bin" || ! -x "$pdftoppm_bin" ]]; then
  case "$(uname -s)" in
    Darwin) poppler_hint='brew install poppler' ;;
    *) poppler_hint='sudo apt-get install poppler-utils' ;;
  esac
  printf 'Required EPUB image renderer pdftoppm is unavailable; install Poppler with: %s\n' "$poppler_hint" >&2
  exit 2
fi

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

build_started=0
build_succeeded=0
publish_active=0
had_dist_pdf=0
had_dist_epub=0
stage_pdf=
stage_epub=
backup_pdf=
backup_epub=
cleanup() {
  local status=$?
  trap - EXIT
  set +e
  if (( publish_active )); then
    if (( had_dist_pdf )); then
      [[ ! -e "$backup_pdf" ]] || mv -f -- "$backup_pdf" "$dist_pdf"
    else
      rm -f -- "$dist_pdf"
    fi
    if (( had_dist_epub )); then
      [[ ! -e "$backup_epub" ]] || mv -f -- "$backup_epub" "$dist_epub"
    else
      rm -f -- "$dist_epub"
    fi
  fi
  [[ -z "$stage_pdf" ]] || rm -f -- "$stage_pdf"
  [[ -z "$stage_epub" ]] || rm -f -- "$stage_epub"
  [[ -z "$backup_pdf" ]] || rm -f -- "$backup_pdf"
  [[ -z "$backup_epub" ]] || rm -f -- "$backup_epub"
  if (( build_started && ! build_succeeded )); then
    rm -f -- "$pdf" "$epub"
  fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

mkdir -p -- "$out_dir" "$cache_dir"
build_started=1
rm -f -- "$pdf" "$epub"
rm -rf -- "$work_dir"
mkdir -p -- "$work_dir"

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
  printf 'Tectonic failed to build %s.\n' "$source_file" >&2
  exit "$status"
fi
if [[ ! -s "$pdf" ]]; then
  printf 'Tectonic exited successfully but did not create a non-empty PDF: %s\n' "$pdf" >&2
  exit 1
fi

pandoc_args=(
  "--from=$repo_root/scripts/book-epub.lua"
  --to=epub3
  --standalone
  --toc
  --toc-depth=2
  --split-level=1
  "--metadata=lang:$language"
  "--css=$book_dir/epub.css"
  "$source_file"
  "--output=$epub"
)
if (
  cd -- "$book_dir"
  TECTONIC_CACHE_DIR="$cache_dir" \
    BOOK_EPUB_TECTONIC="$tectonic_bin" \
    BOOK_EPUB_PDFTOPPM="$pdftoppm_bin" \
    BOOK_EPUB_BUNDLE="$bundle" \
    BOOK_EPUB_WORK="$work_dir" \
    BOOK_OFFLINE="$offline" \
    "$pandoc_bin" "${pandoc_args[@]}"
); then
  :
else
  status=$?
  printf 'Pandoc failed to build the %s EPUB.\n' "$COURSE_LANG" >&2
  exit "$status"
fi
if [[ ! -s "$epub" ]]; then
  printf 'Pandoc exited successfully but did not create a non-empty EPUB: %s\n' "$epub" >&2
  exit 1
fi
build_succeeded=1

if [[ -e "$dist_pdf" && ! -f "$dist_pdf" ]] || [[ -e "$dist_epub" && ! -f "$dist_epub" ]]; then
  printf 'Distribution targets must be regular files: %s and %s\n' "$dist_pdf" "$dist_epub" >&2
  exit 1
fi
if ! mkdir -p -- "$dist_dir"; then
  printf 'Cannot create textbook distribution directory: %s\n' "$dist_dir" >&2
  exit 1
fi
stage_pdf="$dist_dir/.$basename.pdf.new.$$"
stage_epub="$dist_dir/.$basename.epub.new.$$"
backup_pdf="$dist_dir/.$basename.pdf.backup.$$"
backup_epub="$dist_dir/.$basename.epub.backup.$$"
if ! cp -- "$pdf" "$stage_pdf" || [[ ! -s "$stage_pdf" ]]; then
  printf 'Cannot stage distribution PDF: %s\n' "$stage_pdf" >&2
  exit 1
fi
if ! cp -- "$epub" "$stage_epub" || [[ ! -s "$stage_epub" ]]; then
  printf 'Cannot stage distribution EPUB: %s\n' "$stage_epub" >&2
  exit 1
fi
if [[ -e "$dist_pdf" ]]; then
  if ! cp -p -- "$dist_pdf" "$backup_pdf" || [[ ! -s "$backup_pdf" ]]; then
    printf 'Cannot back up existing distribution PDF: %s\n' "$dist_pdf" >&2
    exit 1
  fi
  had_dist_pdf=1
fi
if [[ -e "$dist_epub" ]]; then
  if ! cp -p -- "$dist_epub" "$backup_epub" || [[ ! -s "$backup_epub" ]]; then
    printf 'Cannot back up existing distribution EPUB: %s\n' "$dist_epub" >&2
    exit 1
  fi
  had_dist_epub=1
fi

publish_active=1
if ! mv -f -- "$stage_pdf" "$dist_pdf"; then
  printf 'Cannot publish distribution PDF: %s\n' "$dist_pdf" >&2
  exit 1
fi
stage_pdf=
if ! mv -f -- "$stage_epub" "$dist_epub"; then
  printf 'Cannot publish distribution EPUB: %s\n' "$dist_epub" >&2
  exit 1
fi
stage_epub=
publish_active=0
rm -f -- "$backup_pdf" "$backup_epub"
backup_pdf=
backup_epub=

printf 'PDF written: %s\n' "$pdf"
printf 'EPUB written: %s\n' "$epub"
printf 'Distributed PDF: %s\n' "$dist_pdf"
printf 'Distributed EPUB: %s\n' "$dist_epub"
