#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"
TOOLS="$ROOT/.tools"
mkdir -p "$TOOLS"
JAVA=${JAVA_HOME:+$JAVA_HOME/bin/java}; JAVA=${JAVA:-java}
JAVAC=${JAVA_HOME:+$JAVA_HOME/bin/javac}; JAVAC=${JAVAC:-javac}
for cmd in curl tar sha256sum; do command -v "$cmd" >/dev/null 2>&1 || { printf 'setup: required command not found: %s\n' "$cmd" >&2; exit 2; }; done
command -v "$JAVA" >/dev/null 2>&1 && command -v "$JAVAC" >/dev/null 2>&1 || { printf 'setup: Java 21 JDK required; select it with SDKMAN or set JAVA_HOME\n' >&2; exit 2; }
version=$("$JAVA" -XshowSettings:properties -version 2>&1 | awk -F'= ' '/java.specification.version/{print $2; exit}')
[[ "$version" =~ ^[0-9]+$ ]] && ((version >= 21)) || { printf 'setup: Java 21 or newer required (found %s); use: sdk use java 21.0.12-amzn\n' "${version:-unknown}" >&2; exit 2; }
fetch_verified() {
  local url=$1 expected=$2 dest=$3 tmp="$3.part.$$"
  if [[ -f "$dest" ]]; then
    if [[ "$(sha256sum "$dest" | cut -d' ' -f1)" == "$expected" ]]; then printf 'verified: %s\n' "${dest#$ROOT/}"; return; fi
    rm -f -- "$dest"
  fi
  rm -f -- "$tmp"
  printf 'Downloading %s\n' "$url"
  if ! curl --fail --location --retry 2 --output "$tmp" "$url"; then rm -f -- "$tmp"; printf 'setup: download failed: %s\n' "$url" >&2; exit 2; fi
  local actual
  actual=$(sha256sum "$tmp" | cut -d' ' -f1)
  if [[ "$actual" != "$expected" ]]; then rm -f -- "$tmp"; printf 'setup: SHA-256 mismatch for %s (expected %s, got %s)\n' "$url" "$expected" "$actual" >&2; exit 2; fi
  mv -- "$tmp" "$dest"
}
JUNIT_URL='https://repo1.maven.org/maven2/org/junit/platform/junit-platform-console-standalone/1.11.4/junit-platform-console-standalone-1.11.4.jar'
JUNIT_SHA='b016ef6b1c3454d6d7c2c88ce081dabf289699686af6622d6e4e2e1b54b4a2fc'
fetch_verified "$JUNIT_URL" "$JUNIT_SHA" "$TOOLS/junit-platform-console-standalone-1.11.4.jar"

install_tectonic() {
  local flavor=$1 url sha archive extracted
  archive="$TOOLS/tectonic-$flavor.tar.gz"
  extracted="$TOOLS/tectonic-0.17.0.part.$$"
  if [[ "$flavor" == musl ]]; then
    url='https://github.com/tectonic-typesetting/tectonic/releases/download/tectonic%400.17.0/tectonic-0.17.0-x86_64-unknown-linux-musl.tar.gz'
    sha='8533d07f9ccbd7a65824b9e0459041bca34af1eb33daba48f59215593753a3b7'
  else
    url='https://github.com/tectonic-typesetting/tectonic/releases/download/tectonic%400.17.0/tectonic-0.17.0-x86_64-unknown-linux-gnu.tar.gz'
    sha='1a715688baf591e650c8aeb160ae934e181685eecbb38b317de30b269ac5d606'
  fi
  fetch_verified "$url" "$sha" "$archive"
  rm -rf -- "$extracted"
  mkdir -p "$extracted"
  tar -xzf "$archive" -C "$extracted" --no-same-owner
  local candidate
  candidate=$(find "$extracted" -type f -name tectonic -print -quit)
  [[ -n "$candidate" ]] || { rm -rf -- "$extracted"; printf 'setup: archive has no tectonic executable: %s\n' "$archive" >&2; return 1; }
  chmod +x "$candidate"
  if ! "$candidate" --version; then rm -rf -- "$extracted"; return 1; fi
  rm -rf -- "$TOOLS/tectonic-0.17.0"
  mkdir -p "$TOOLS/tectonic-0.17.0"
  install -m 755 "$candidate" "$TOOLS/tectonic-0.17.0/tectonic"
  rm -rf -- "$extracted"
}
TECTONIC_BIN="$TOOLS/tectonic-0.17.0/tectonic"
if [[ -x "$TECTONIC_BIN" ]] && "$TECTONIC_BIN" --version >/dev/null 2>&1; then
  printf 'verified: .tools/tectonic-0.17.0/tectonic\n'
else
  rm -rf -- "$TOOLS/tectonic-0.17.0"
  if ! install_tectonic musl; then
    printf 'musl Tectonic unavailable; trying the pinned glibc build.\n' >&2
    install_tectonic gnu
  fi
fi
printf 'Course tools ready. First PDF build downloads the Tectonic bundle; later builds can use BOOK_OFFLINE=1.\n'
