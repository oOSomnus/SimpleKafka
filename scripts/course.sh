#!/usr/bin/env bash
set -euo pipefail

caller_dir=$PWD
ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
source "$ROOT/scripts/tools.sh"
validate_course_lang || exit $?
cd "$ROOT"
ACTION=${1:-}
STEP_VALUE=${2:-}
GOALS=${3:-}
JAVA=${JAVA_HOME:+$JAVA_HOME/bin/java}
JAVA=${JAVA:-java}
JAVAC=${JAVA_HOME:+$JAVA_HOME/bin/javac}
JAVAC=${JAVAC:-javac}
MANIFEST="$ROOT/course/steps.tsv"
JUNIT="$ROOT/.tools/junit-platform-console-standalone-1.11.4.jar"

fail() { printf 'course: %s\n' "$*" >&2; exit 2; }
check_java() {
  command -v "$JAVA" >/dev/null 2>&1 || fail "Java not found: $JAVA (set JAVA_HOME or install a Java 21+ JDK)"
  command -v "$JAVAC" >/dev/null 2>&1 || fail "javac not found: $JAVAC"
  local version
  version=$("$JAVA" -XshowSettings:properties -version 2>&1 | awk -F'= ' '/java.specification.version/{print $2; exit}')
  [[ "$version" =~ ^[0-9]+$ ]] || fail 'Cannot determine Java version'
  (( version >= 21 )) || fail "Java 21 or newer required; current specification version is $version"
}
check_junit() {
  [[ -f "$JUNIT" ]] || fail "JUnit Console is missing; run 'make setup' (expected $JUNIT)"
}
check_manifest() {
  [[ -f "$MANIFEST" ]] || fail "missing course manifest: $MANIFEST"
  local expected=1 step chapter title_en title_zh methods test prereq header
  IFS= read -r header < "$MANIFEST" || fail 'course manifest is empty'
  [[ "$header" == $'step\tchapter\ttitle_en\ttitle_zh\teditable_methods\ttest_class\tprerequisites' ]] || fail 'course manifest must use the seven-column bilingual schema'
  awk -F '\t' 'NF != 7 { exit 1 } NR > 1 && ($1 == "" || $2 == "" || $3 == "" || $4 == "" || $5 == "" || $6 == "" || $7 == "") { exit 1 }' "$MANIFEST" || fail 'course manifest must contain exactly seven non-empty columns'
  while IFS=$'\t' read -r step chapter title_en title_zh methods test prereq; do
    [[ "$step" == step ]] && continue
    [[ "$step" =~ ^[0-9]+$ && "$step" -eq "$expected" ]] || fail "manifest step sequence broken at $step; expected $expected"
    [[ "$chapter" == "$(( (step - 1) / 4 + 1 ))" ]] || fail "manifest chapter mismatch for step $step"
    [[ "$test" == "Step$(printf '%02d' "$step")Test" ]] || fail "manifest test class mismatch for step $step"
    [[ -n "$title_en" && -n "$title_zh" ]] || fail "manifest titles are required for step $step"
    ((expected+=1))
  done < "$MANIFEST"
  (( expected == 29 )) || fail "manifest must contain 28 steps, found $((expected-1))"
}
parse_goals() {
  local -a goals=() actions=() numbers=() unknown=()
  read -r -a goals <<< "$GOALS"
  for goal in "${goals[@]}"; do
    case "$goal" in
      setup|setup-book|doctor|compile|test|step-test|reference-test|list|demo|reference-demo|book) actions+=("$goal") ;;
      0[1-9]|[1-9]|1[0-9]|2[0-8]) numbers+=("$goal") ;;
      '') ;;
      *) unknown+=("$goal") ;;
    esac
  done
  ((${#unknown[@]} == 0)) || fail "unsupported goal(s): ${unknown[*]}"
  ((${#actions[@]} == 1)) || fail 'exactly one action goal is required'
  [[ "${actions[0]}" == "$ACTION" ]] || fail 'internal action/goal mismatch'
  ((${#numbers[@]} <= 1)) || fail 'use at most one numeric step argument'
  if ((${#numbers[@]} == 1)); then
    [[ -z "$STEP_VALUE" ]] || fail 'do not combine numeric goal and STEP='
    STEP_VALUE=${numbers[0]}
  fi
  if [[ -n "$STEP_VALUE" && "$STEP_VALUE" =~ ^[0-9]+$ ]]; then
    ((10#$STEP_VALUE >= 1 && 10#$STEP_VALUE <= 28)) || fail "step must be in 1..28: $STEP_VALUE"
  elif [[ -n "$STEP_VALUE" ]]; then
    fail "step must be an integer in 1..28: $STEP_VALUE"
  fi
  if ((${#numbers[@]})) && [[ "$ACTION" != test && "$ACTION" != step-test && "$ACTION" != reference-test ]]; then
    fail "numeric step argument is invalid for $ACTION"
  fi
  if [[ "$ACTION" == step-test && -z "$STEP_VALUE" ]]; then fail 'usage: make step-test N'; fi
}
compile_mode() {
  local mode=$1 main="$ROOT/build/$1/main" tests="$ROOT/build/$1/test"
  local source_root="$ROOT/exercises/src/main/java"
  [[ "$mode" == reference ]] && source_root="$ROOT/reference/src/main/java"
  [[ -d "$ROOT/provided/src/main/java" && -d "$source_root" && -d "$ROOT/tests/src/test/java" ]] || fail "incomplete source roots for $mode mode"
  check_java
  check_junit
  rm -rf -- "$main" "$tests"
  mkdir -p "$main" "$tests"
  local source
  local -a main_sources=() test_sources=()
  while IFS= read -r -d '' source; do main_sources+=("$source"); done < <(find "$ROOT/provided/src/main/java" "$source_root" -type f -name '*.java' -print0)
  ((${#main_sources[@]})) || fail "no $mode Java main sources found"
  "$JAVAC" --release 21 -encoding UTF-8 -d "$main" "${main_sources[@]}"
  while IFS= read -r -d '' source; do test_sources+=("$source"); done < <(find "$ROOT/tests/src/test/java" -type f -name '*.java' -print0)
  ((${#test_sources[@]})) || fail 'no course tests found'
  "$JAVAC" --release 21 -encoding UTF-8 -cp "$main:$JUNIT" -d "$tests" "${test_sources[@]}"
}
run_tests() {
  local mode=$1 max=$2 single=${3:-false}
  compile_mode "$mode"
  local classdir="$ROOT/build/$mode/test" main="$ROOT/build/$mode/main" reports="$ROOT/build/$mode/reports"
  rm -rf -- "$reports"
  mkdir -p "$reports"
  local -a selectors=() manifest_tests=()
  local step chapter title_en title_zh methods test prereq number completed
  while IFS=$'\t' read -r step chapter title_en title_zh methods test prereq; do
    [[ "$step" == step ]] && continue
    manifest_tests[$step]=$test
  done < "$MANIFEST"
  if [[ "$single" == true ]]; then
    number=$((10#$max))
    completed=$number
    selectors+=("io.simplekafka.course.${manifest_tests[number]}")
  else
    max=${max:-28}
    max=$((10#$max))
    completed=$max
    for ((number=1; number<=max; number++)); do
      selectors+=("io.simplekafka.course.${manifest_tests[number]}")
    done
  fi
  local selector
  for selector in "${selectors[@]}"; do
    local classfile=${selector//./\/}.class
    [[ -f "$classdir/$classfile" ]] || fail "expected test class was not compiled: $selector"
  done
  local -a junit_args=(execute --class-path "$main:$classdir" --details=tree --disable-ansi-colors --fail-if-no-tests --reports-dir "$reports")
  for selector in "${selectors[@]}"; do junit_args+=(--select-class "$selector"); done
  printf 'Running %s tests: %s\n' "$mode" "${selectors[*]}"
  "$JAVA" -jar "$JUNIT" "${junit_args[@]}"
  if [[ "$single" == true ]]; then
    printf 'Step %02d diagnostic passed; single-step tests do not grant chapter completion.\n' "$number"
  elif ((completed == 28)); then
    printf 'Course complete\n'
  elif ((completed % 4 == 0)); then
    printf 'Chapter %d complete; next: Chapter %d / Step %d\n' "$((completed / 4))" "$((completed / 4 + 1))" "$((completed + 1))"
  else
    printf 'Step %d passed; next: Step %d\n' "$completed" "$((completed + 1))"
  fi
}

check_manifest
case "$ACTION" in
  doctor)
    check_java
    printf 'Java: '; "$JAVA" -version 2>&1 | awk 'NR==1{print}'
    printf 'javac: '; "$JAVAC" -version
    printf 'Make: '; make --version | awk 'NR==1{print}'
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
        printf 'Tectonic: not prepared (PDF-only; run make setup-book)\n'
      else
        fail 'cannot resolve Tectonic for this platform'
      fi
    fi
    if [[ -f "$JUNIT" ]]; then
      printf 'JUnit Console: '
      "$JAVA" -jar "$JUNIT" --version | awk 'NR==1{print}'
    else
      printf 'JUnit Console: not prepared (run make setup)\n'
    fi
    printf 'PDF fonts come from the Tectonic bundle; the first book build needs network access. Warm both languages before BOOK_OFFLINE=1.\n'
    ;;
  compile)
    compile_mode student
    printf 'Student skeleton and tests compiled.\n'
    ;;
  list)
    check_manifest
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
  test)
    parse_goals
    run_tests student "${STEP_VALUE:-28}" false
    ;;
  step-test)
    parse_goals
    run_tests student "$STEP_VALUE" true
    ;;
  reference-test)
    parse_goals
    run_tests reference "${STEP_VALUE:-28}" false
    ;;
  demo|reference-demo)
    parse_goals
    mode=student; [[ "$ACTION" == reference-demo ]] && mode=reference
    compile_mode "$mode"
    "$JAVA" -cp "$ROOT/build/$mode/main" io.simplekafka.cli.CourseDemo
    ;;
  *) fail "unknown action: $ACTION" ;;
esac
