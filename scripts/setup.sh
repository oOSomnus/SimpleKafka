#!/usr/bin/env bash
set -euo pipefail

caller_dir=$PWD
ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
source "$ROOT/scripts/tools.sh"
validate_course_lang || exit $?
cd "$ROOT"

mode=${1:-book}
if (( $# > 1 )); then
  printf 'usage: scripts/setup.sh [book]\n' >&2
  exit 2
fi
if [[ "$mode" != book ]]; then
  printf 'setup: unknown mode: %s (expected book)\n' "$mode" >&2
  exit 2
fi

TOOLS="$ROOT/.tools"

fetch_verified() {
  local url=$1 expected=$2 dest=$3 tmp="$3.part.$$" actual
  if [[ -f "$dest" ]]; then
    if actual=$(sha256_file "$dest") && [[ "$actual" == "$expected" ]]; then
      printf 'verified: %s\n' "${dest#$ROOT/}"
      return 0
    fi
    rm -f -- "$dest"
  fi
  rm -f -- "$tmp"
  printf 'Downloading %s\n' "$url"
  if ! curl --fail --location --retry 2 --output "$tmp" "$url"; then
    rm -f -- "$tmp"
    printf 'setup: download failed: %s\n' "$url" >&2
    return 2
  fi
  actual=$(sha256_file "$tmp") || {
    rm -f -- "$tmp"
    printf 'setup: cannot calculate SHA-256 for %s\n' "$url" >&2
    return 2
  }
  if [[ "$actual" != "$expected" ]]; then
    rm -f -- "$tmp"
    printf 'setup: SHA-256 mismatch for %s (expected %s, got %s)\n' "$url" "$expected" "$actual" >&2
    return 2
  fi
  mv -- "$tmp" "$dest" || {
    rm -f -- "$tmp"
    printf 'setup: cannot cache verified download: %s\n' "$dest" >&2
    return 2
  }
}

install_tectonic() {
  local target=$1 archive url sha extract candidate version parent destination stage backup
  case "$target" in
    x86_64-unknown-linux-musl)
      sha=8533d07f9ccbd7a65824b9e0459041bca34af1eb33daba48f59215593753a3b7 ;;
    x86_64-unknown-linux-gnu)
      sha=1a715688baf591e650c8aeb160ae934e181685eecbb38b317de30b269ac5d606 ;;
    aarch64-unknown-linux-musl)
      sha=b10954a95404f3ab2328d2fa59a5ebab8e657f893fab096f98be8db7c0c979b8 ;;
    x86_64-apple-darwin)
      sha=7c90ef5b6ddb1eb1937e4337add5237b79338e4b9676459fa91187d24d6cdf80 ;;
    aarch64-apple-darwin)
      sha=a3f1cac7c5678f01661a92212f58480ae3b0634115d880dbc59e2953ded45667 ;;
    *) printf 'setup: no pinned Tectonic archive for %s\n' "$target" >&2; return 2 ;;
  esac
  url="https://github.com/tectonic-typesetting/tectonic/releases/download/tectonic%400.17.0/tectonic-0.17.0-$target.tar.gz"
  archive="$TOOLS/tectonic-0.17.0-$target.tar.gz"
  extract="$TOOLS/.tectonic-extract-$target.$$"
  parent="$TOOLS/tectonic-0.17.0"
  destination="$parent/$target"
  stage="$parent/.$target.part.$$"
  backup="$parent/.$target.old.$$"
  fetch_verified "$url" "$sha" "$archive" || return $?
  rm -rf -- "$extract" "$stage" "$backup"
  if ! mkdir -p "$extract"; then
    printf 'setup: cannot create extraction directory: %s\n' "$extract" >&2
    return 2
  fi
  if ! tar -xzf "$archive" -C "$extract"; then
    rm -rf -- "$extract"
    printf 'setup: cannot extract Tectonic archive: %s\n' "$archive" >&2
    return 2
  fi
  candidate=$(find "$extract" -type f -name tectonic -print -quit)
  if [[ -z "$candidate" ]] || ! chmod +x "$candidate"; then
    rm -rf -- "$extract"
    printf 'setup: archive has no usable tectonic executable: %s\n' "$archive" >&2
    return 2
  fi
  if ! version=$("$candidate" --version 2>&1) || [[ "$version" != *0.17.0* ]]; then
    rm -rf -- "$extract"
    printf 'setup: archive did not provide working Tectonic 0.17.0: %s\n' "$archive" >&2
    return 2
  fi
  if ! mkdir -p "$parent" "$stage"; then
    rm -rf -- "$extract" "$stage"
    printf 'setup: cannot stage Tectonic under %s\n' "$parent" >&2
    return 2
  fi
  if ! install -m 755 "$candidate" "$stage/tectonic"; then
    rm -rf -- "$extract" "$stage"
    printf 'setup: cannot install staged Tectonic binary\n' >&2
    return 2
  fi
  if [[ -e "$destination" ]] && ! mv -- "$destination" "$backup"; then
    rm -rf -- "$extract" "$stage"
    printf 'setup: cannot preserve existing Tectonic installation: %s\n' "$destination" >&2
    return 2
  fi
  if ! mv -- "$stage" "$destination"; then
    [[ ! -e "$backup" ]] || mv -- "$backup" "$destination"
    rm -rf -- "$extract" "$stage"
    printf 'setup: cannot publish Tectonic installation: %s\n' "$destination" >&2
    return 2
  fi
  rm -rf -- "$backup" "$extract"
  printf 'verified: %s\n' "${destination#$ROOT/}/tectonic"
  "$destination/tectonic" --version
}

if [[ ${TECTONIC+x} ]]; then
  TECTONIC_BIN=$(resolve_tectonic "$ROOT" "$caller_dir") || exit $?
  "$TECTONIC_BIN" --version
  printf 'Using the TECTONIC override; no repository tool was downloaded.\n'
  exit 0
fi

target=$(tectonic_target) || exit $?
TECTONIC_BIN="$TOOLS/tectonic-0.17.0/$target/tectonic"
if [[ -x "$TECTONIC_BIN" ]] && version=$("$TECTONIC_BIN" --version 2>&1) && [[ "$version" == *0.17.0* ]]; then
  printf 'verified: %s\n' "${TECTONIC_BIN#$ROOT/}"
  "$TECTONIC_BIN" --version
  exit 0
fi
for cmd in curl tar install find; do
  command -v "$cmd" >/dev/null 2>&1 || { printf 'setup: required command not found: %s\n' "$cmd" >&2; exit 2; }
done
if ! command -v sha256sum >/dev/null 2>&1 && ! command -v shasum >/dev/null 2>&1; then
  printf 'setup: required command not found: sha256sum or shasum\n' >&2
  exit 2
fi
rm -rf -- "$TOOLS/tectonic-0.17.0/$target"
mkdir -p "$TOOLS"
if install_tectonic "$target"; then
  :
elif [[ "$target" == x86_64-unknown-linux-musl ]]; then
  printf 'Pinned Linux musl Tectonic unavailable; trying the pinned glibc build.\n' >&2
  install_tectonic x86_64-unknown-linux-gnu
else
  exit 2
fi
printf 'PDF tool ready. The first book build downloads the TeX bundle; warm both languages before BOOK_OFFLINE=1.\n'
