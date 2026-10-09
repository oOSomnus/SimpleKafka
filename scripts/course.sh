#!/usr/bin/env bash
set -euo pipefail

caller_dir=$PWD
ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
source "$ROOT/scripts/tools.sh"
validate_course_lang || exit $?
cd "$ROOT"
ACTION=${1:-}
JAVA=${JAVA_HOME:+$JAVA_HOME/bin/java}
JAVA=${JAVA:-java}
JAVAC=${JAVA_HOME:+$JAVA_HOME/bin/javac}
JAVAC=${JAVAC:-javac}
MANIFEST="$ROOT/course/steps.tsv"

fail() { printf 'course: %s\n' "$*" >&2; exit 2; }
check_java() {
  command -v "$JAVA" >/dev/null 2>&1 || fail "Java not found: $JAVA (set JAVA_HOME or install a Java 21+ JDK)"
  command -v "$JAVAC" >/dev/null 2>&1 || fail "javac not found: $JAVAC"
  local version
  version=$("$JAVA" -XshowSettings:properties -version 2>&1 | awk -F'= ' '/java.specification.version/{print $2; exit}')
  [[ "$version" =~ ^[0-9]+$ ]] || fail 'Cannot determine Java version'
  (( version >= 21 )) || fail "Java 21 or newer required; current specification version is $version"
}
check_manifest() {
  [[ -f "$MANIFEST" ]] || fail "missing course manifest: $MANIFEST"
  local expected=1 previous_chapter=0 step chapter title_en title_zh methods test prereq header count=0
  IFS= read -r header < "$MANIFEST" || fail 'course manifest is empty'
  [[ "$header" == $'step\tchapter\ttitle_en\ttitle_zh\teditable_methods\ttest_class\tprerequisites' ]] || fail 'course manifest must use the seven-column bilingual schema'
  awk -F '\t' 'NF != 7 { exit 1 } NR > 1 && ($1 == "" || $2 == "" || $3 == "" || $4 == "" || $5 == "" || $6 == "" || $7 == "") { exit 1 }' "$MANIFEST" || fail 'course manifest must contain exactly seven non-empty columns'
  while IFS=$'\t' read -r step chapter title_en title_zh methods test prereq; do
    [[ "$step" == step ]] && continue
    [[ "$step" =~ ^[0-9]+$ && "$step" -eq "$expected" ]] || fail "manifest step sequence broken at $step; expected $expected"
    [[ "$chapter" =~ ^[0-9]+$ ]] || fail "manifest chapter is not an integer for step $step"
    if (( expected == 1 )); then
      (( chapter == 1 )) || fail "manifest must start at chapter 1"
    else
      (( chapter == previous_chapter || chapter == previous_chapter + 1 )) || fail "manifest chapter sequence broken at step $step"
    fi
    [[ "$test" == "Step$(printf '%02d' "$step")Test" ]] || fail "manifest test class mismatch for step $step"
    [[ -n "$title_en" && -n "$title_zh" ]] || fail "manifest titles are required for step $step"
    previous_chapter=$chapter
    ((expected+=1))
    ((count+=1))
  done < "$MANIFEST"
  (( count > 0 )) || fail 'manifest must contain at least one step'
}

check_manifest
case "$ACTION" in
  validate)
    ;;
  doctor)
    check_java
    printf 'Java: '; "$JAVA" -version 2>&1 | awk 'NR==1{print}'
    printf 'javac: '; "$JAVAC" -version
    for tool in find awk curl tar install; do command -v "$tool" >/dev/null 2>&1 && printf '%s: available\n' "$tool" || printf '%s: missing\n' "$tool"; done
    if command -v sha256sum >/dev/null 2>&1 || command -v shasum >/dev/null 2>&1; then
      printf 'SHA-256 tool: available\n'
    else
      printf 'SHA-256 tool: missing (sha256sum or shasum required for setup)\n'
    fi
    if tectonic_bin=$(resolve_tectonic "$ROOT" "$caller_dir"); then
      printf 'Tectonic: '
      "$tectonic_bin" --version
    else
      resolve_status=$?
      if [[ ${TECTONIC+x} ]]; then
        fail 'the TECTONIC override is unavailable or unsupported'
      elif (( resolve_status == 127 )); then
        printf 'Tectonic: not prepared (PDF-only; run ./gradlew :setupBook)\n'
      else
        fail 'cannot resolve Tectonic for this platform'
      fi
    fi
    printf 'PDF fonts come from the Tectonic bundle; the first book build needs network access. Warm both languages before BOOK_OFFLINE=1.\n'
    ;;
  list)
    if [[ "$COURSE_LANG" == en ]]; then
      printf 'Step  Chapter  Lesson\tEditable methods\tPrerequisites\n'
    else
      printf '步骤  章节  课程\t编辑方法\t前置步骤\n'
    fi
    while IFS=$'\t' read -r step chapter title_en title_zh methods test prereq; do
      [[ "$step" == step ]] && continue
      title=$title_en
      [[ "$COURSE_LANG" == zh ]] && title=$title_zh
      printf '%02d\t%s\t%s\t%s\t%s\n' "$step" "$chapter" "$title" "$methods" "$prereq"
    done < "$MANIFEST"
    ;;
  *) fail "unknown action: $ACTION" ;;
esac
