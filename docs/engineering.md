# Engineering guide

[Project README](../README.md) · [简体中文](engineering.zh-CN.md)

## Environment and dependencies

The Gradle 9.8.1 Wrapper can start with Java 17 or newer. Java compilation, tests, demos, and source-parity checks require a Java 21+ JDK. No system Gradle installation is needed. SDKMAN is optional; the project does not switch JDKs or install system packages.

The shell scripts require Bash 3.2+, `find`, `awk`, `curl`, and either `sha256sum` or `shasum`. Preparing the optional PDF tool also uses `tar` and `install`. `./gradlew :setup` resolves JUnit 1.11.4 through Gradle dependency verification; the first resolution needs network access. Course tests do not need the PDF engine or system fonts. `./gradlew :setupBook` separately prepares the pinned Tectonic 0.17.0 engine. The first PDF build downloads its TeX bundle and Latin Modern/Fandol fonts. Noto, fontconfig, xelatex, latexmk, Maven, Docker, and Kafka are not project dependencies.

## Tasks and test reports

`./gradlew :compile` compiles the student skeleton and shared tests; a successful compile does not mean the exercise methods are implemented. `./gradlew :test -Pstep=N` runs the student contract cumulatively through step `N`. `./gradlew :stepTest -Pstep=N` runs one step for diagnosis only; it does not establish chapter completion. The course has 8 chapters and 31 steps; chapter-end cumulative steps are 4, 8, 12, 16, 20, 24, 28, and 31. Run the complete answer contract and the real TCP scenarios with:

```sh
./gradlew :referenceTest -Pstep=31
./gradlew :referenceDemo
./gradlew :referenceConsistencyDemo
```

Course tasks run `CourseTestRunner` through `JavaExec`, print plain-text results, and write legacy JUnit XML under `build/student/reports/` or `build/reference/reports/`. The standard `:exercises:test` and `:reference:test` tasks remain available for IDE and `--tests` debugging; their XML and HTML reports are under `build/<mode>/gradle-test-results/` and `build/<mode>/gradle-test-reports/`. Report text is English regardless of `COURSE_LANG`. The runner honors the `NO_COLOR` and `TERM=dumb` color gate.

## Source scaffold parity

The checker compares every file under `reference/src/main/java` and `exercises/src/main/java` without compiling or loading either implementation. Java files are parsed with the public JDK compiler-tree API; non-Java files must match byte-for-byte. Comments, Javadoc, formatting, and equivalent literal spellings do not affect the comparison. This is an AST-structure contract, not proof of behavioral equivalence.

`course/source-parity.tsv` is the explicit five-column policy: `kind`, `side`, `path`, `symbol`, and `reason`. A `method-body` / `both` rule masks only the named method body; signatures, parameter names, annotations, modifiers, and throws clauses remain checked. A `private-member` / `reference` or `exercises` rule removes only one explicitly private, single-side implementation member; constructors cannot use this rule. Common private members, provided method bodies, constructors, constants, field initializers, and member order remain checked. Exact imports are ignored only when their simple name is absent from the projected AST; wildcard imports remain checked.

Paths are relative to the source root. Method symbols include the lexical owner and parameter types, for example `RecordCodec#encode(LogRecord)`; fields use `Owner#field:name`, and nested types use `Owner#type:Name`. Duplicate, stale, overlapping, or invalid rules fail closed. A listed private member must remain private and absent from the other side; split a multi-variable field declaration before exempting one field. Diagnostics include the source file, symbol, original line, and a bounded excerpt around the first difference. Exit codes are **0** for parity, **1** for source/file drift, and **2** for invalid invocation, environment, policy, or parsing.

Run the Gradle gate and its deterministic checker self-test, or use the dependency-free Java 21+ command directly:

```sh
./gradlew :verifySourceParity
./gradlew :sourceParitySelfTest
java --source 21 scripts/SourceParity.java verify .
java --source 21 scripts/SourceParity.java self-test
```

`:check` and CI enforce the parity gate. Student `:test`, `:stepTest`, and `:compile` do not depend on it. The gate should pass for the controlled course skeleton; a failure indicates unregistered structural drift or an invalid policy/parser input, not a requirement that student algorithms match the answer.

## Code formatting

Use the existing Google Java Format 1.28.0 configuration. `:format` and `:formatCheck` cover Java sources in `exercises/`, `reference/`, `provided/`, `tests/`, and `scripts/`; they use AOSP style and preserve long string literals.

```sh
./gradlew :format
./gradlew :formatCheck
```

## Textbook PDFs and offline builds

`./gradlew :setupBook` prepares the pinned PDF engine. `./gradlew :book` builds English by default; set `COURSE_LANG=zh` for the Chinese edition. The output files are `build/book/en/simpleKafka.pdf` and `build/book/zh/simpleKafka-zh.pdf`. The Wrapper needs Java 17+ to launch these tasks, but `:book` does not compile Java or resolve JUnit. For offline builds, first build both languages online, then run:

```sh
BOOK_OFFLINE=1 ./gradlew :book --offline
COURSE_LANG=zh BOOK_OFFLINE=1 ./gradlew :book --offline
```

`BOOK_OFFLINE=1` selects the cached TeX bundle; Gradle's `--offline` separately controls Gradle dependency resolution. The language-specific build options are documented in [the English contract appendix](book/en/appendices/contracts.tex) and [the Chinese contract appendix](book/zh/appendices/contracts.tex).

## Acceptance and behavioral coverage

Each `StepNNTest` is registered in [`course/steps.tsv`](../course/steps.tsv). The chapter-end commands run that step and its prerequisites. The matrix records representative boundaries in the teaching contract; it is not exhaustive, does not claim fuzz testing or production Kafka certification, and is not a 100% code-coverage statement.

| Chapter / steps | Contract highlights |
|---|---|
| [Records and log](book/en/chapters/01-record-log.tex), 1–4 | Independent on-disk golden bytes; CRC, invalid fields, buffer prefixes, and maximum-record boundaries; contiguous offsets for batch append; hints and total budgets; nonzero bases, truncated-tail recovery, and rejection of complete corruption. |
| [Index and segments](book/en/chapters/02-segments-index.tex), 5–8 | Independent index bytes and corrupt-index rebuild; segment rolling, cross-segment record/byte budgets, concurrent batches; retention boundaries, reopen after truncation, and a fixed-seed state model. |
| [Routing and TCP](book/en/chapters/03-partition-network.tex), 9–12 | CRC/null-key routing and topic-metadata isolation; fixed frame bytes, short reads, truncation, and invalid wire fields; raw-socket request errors, absence of side effects, and subsequent connection survival. |
| [Clients and offsets](book/en/chapters/04-client-offset.tex), 13–16 | Producer batches and partition order; no replay after append with a timed-out response; consumer total budgets, positions, and error boundaries; offset rewind and restart recovery; at-least-once duplicate windows after processor or commit failure. |
| [Consumer groups](book/en/chapters/05-consumer-groups.tex), 17–20 | Lexicographic assignment and empty members; generation, heartbeat, and expiry; real TCP token FETCH/COMMIT rejection for stale, future, non-owner, and empty-owner cases; revoked/retained positions and committed-offset recovery after restart. |
| [Replication and acknowledgments](book/en/chapters/06-replication.tex), 21–24 | Real TCP fetch, retained-start/offline-requester rejection, bad responses, and in-flight role/epoch changes; ISR/HW monotonicity and exact timeout boundaries, including invalid negative timeouts; ACK waits/interruption; concurrent append, HW visibility, and cross-broker progress. |
| [Consistency across layers](book/en/chapters/08-consistency.tex), 29–31 | Real TCP fault windows across producer ACKs, broker replication/HW, consumer position/commit, and stale group generations; stamped producer-batch identity, explicit retry and disk recovery; durable event-id deduplication across callback replay. |

Run the complete answer contract with `./gradlew :referenceTest -Pstep=31`, then the demonstrations with `./gradlew :referenceDemo` and `./gradlew :referenceConsistencyDemo`. After implementing the corresponding student steps, use `./gradlew :test -Pstep=N` with the desired step number.
