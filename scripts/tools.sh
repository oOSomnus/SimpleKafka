#!/usr/bin/env bash

validate_course_lang() {
  if [[ ! ${COURSE_LANG+x} ]]; then COURSE_LANG=en; fi
  case "$COURSE_LANG" in
    en|zh) ;;
    *) printf 'COURSE_LANG must be en or zh\n' >&2; return 2 ;;
  esac
}

sha256_file() {
  local output digest
  if command -v sha256sum >/dev/null 2>&1; then
    output=$(sha256sum -- "$1") || return 1
  elif command -v shasum >/dev/null 2>&1; then
    output=$(shasum -a 256 -- "$1") || return 1
  else
    printf 'setup: required command not found: sha256sum or shasum\n' >&2
    return 2
  fi
  read -r digest _ <<< "$output"
  [[ "$digest" =~ ^[[:xdigit:]]{64}$ ]] || return 1
  printf '%s\n' "$digest"
}

tectonic_target() {
  local os arch
  os=$(uname -s) || return 2
  arch=$(uname -m) || return 2
  case "$os:$arch" in
    Linux:x86_64|Linux:amd64) printf '%s\n' 'x86_64-unknown-linux-musl' ;;
    Linux:aarch64|Linux:arm64) printf '%s\n' 'aarch64-unknown-linux-musl' ;;
    Darwin:x86_64|Darwin:amd64) printf '%s\n' 'x86_64-apple-darwin' ;;
    Darwin:arm64|Darwin:aarch64) printf '%s\n' 'aarch64-apple-darwin' ;;
    *) printf 'Tectonic is unsupported on %s/%s; supported platforms are Linux and macOS on x86_64 or arm64. Set TECTONIC to a compatible executable to override.\n' "$os" "$arch" >&2; return 2 ;;
  esac
}

resolve_tectonic() {
  local repo_root=$1 caller_dir=$2 target candidate
  if [[ ${TECTONIC+x} ]]; then
    if [[ -z "$TECTONIC" ]]; then
      printf 'TECTONIC is set but empty; set it to an executable name or path.\n' >&2
      return 127
    fi
    if [[ "$TECTONIC" == */* ]]; then
      candidate=$TECTONIC
      [[ "$candidate" == /* ]] || candidate="$caller_dir/$candidate"
      if [[ -f "$candidate" && -x "$candidate" ]]; then
        printf '%s\n' "$candidate"
        return 0
      fi
    else
      candidate=$(type -P "$TECTONIC") || candidate=
      if [[ -n "$candidate" ]]; then
        [[ "$candidate" == /* ]] || candidate="$caller_dir/$candidate"
        if [[ -f "$candidate" && -x "$candidate" ]]; then
          printf '%s\n' "$candidate"
          return 0
        fi
      fi
    fi
    printf 'Tectonic executable is unavailable: %s\n' "$TECTONIC" >&2
    return 127
  fi

  target=$(tectonic_target) || return $?
  candidate="$repo_root/.tools/tectonic-0.17.0/$target/tectonic"
  if [[ -f "$candidate" && -x "$candidate" ]]; then
    printf '%s\n' "$candidate"
    return 0
  fi
  if [[ "$target" == x86_64-unknown-linux-musl ]]; then
    candidate="$repo_root/.tools/tectonic-0.17.0/x86_64-unknown-linux-gnu/tectonic"
    if [[ -f "$candidate" && -x "$candidate" ]]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  fi
  candidate=$(type -P tectonic) || candidate=
  if [[ -n "$candidate" ]]; then
    [[ "$candidate" == /* ]] || candidate="$caller_dir/$candidate"
    if [[ -f "$candidate" && -x "$candidate" ]]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  fi
  printf 'Tectonic executable is unavailable; run ./gradlew :setupBook or set TECTONIC to an executable.\n' >&2
  return 127
}
