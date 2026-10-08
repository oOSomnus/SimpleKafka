# simpleKafka: an incremental Kafka learning project

English | [简体中文](README.zh-CN.md)

Learn Kafka by completing a runnable Java broker in small steps: durable logs, TCP requests, producers and consumers, consumer groups, replication, ISR, high watermarks, and controlled failover. The project defines a teaching protocol; it is not compatible with Kafka clients or the Kafka wire protocol.

## Environment

Supported: Linux, macOS, and Windows WSL2 on x86_64 or arm64; native Windows is not supported. The Gradle 9.8.1 Wrapper requires Java 17+; Java compile, test, and demo tasks require a Java 21+ JDK. The scripts use Bash 3.2+, `find`, `awk`, `curl`, and `sha256sum` or `shasum`; Tectonic setup also uses `tar` and `install`. No system Gradle installation is needed. SDKMAN is optional; the project does not switch JDKs, install system packages, or depend on author-specific paths.

```sh
./gradlew :setup       # resolve checksum-verified JUnit through Gradle
./gradlew :setupBook   # separately prepare the pinned Tectonic PDF engine
./gradlew :doctor
./gradlew :list        # English by default
./gradlew :compile
```

Course tests do not need the PDF engine or system fonts. `./gradlew :setup` resolves JUnit 1.11.4 through Gradle dependency verification; first dependency resolution needs network access. `./gradlew --offline` controls Gradle dependency access. The first PDF build also downloads a TeX bundle and its Latin Modern/Fandol fonts. `curl`, `tar`, `install`, and checksum tools are used only to prepare the PDF engine. Noto, fontconfig, xelatex, latexmk, the Maven command-line tool, Docker, and Kafka are not project dependencies.

## Incremental steps

The course has 7 chapters and 28 steps. `exercises/` is the only default learning workspace; `reference/` contains isolated complete answers and is never loaded onto the student classpath. Import the repository root in a Gradle-aware IDE/LSP through the Wrapper; use `exercises` as the learning project and do not add `reference` to its classpath. Tests are cumulative through prerequisites; chapter boundaries are Steps 4, 8, 12, 16, 20, 24, and 28. To list Chinese step titles:

```sh
COURSE_LANG=zh ./gradlew :list
```

```sh
./gradlew :test -Pstep=1        # cumulative through Step 1; the empty skeleton is expected to fail
./gradlew :test -Pstep=12       # cumulative through Steps 1–12
./gradlew :stepTest -Pstep=12   # diagnose only Step 12; not chapter completion
./gradlew :referenceTest -Pstep=28
./gradlew :referenceDemo
./gradlew :book                 # English PDF: build/book/en/simpleKafka.pdf
COURSE_LANG=zh ./gradlew :book  # Chinese PDF: build/book/zh/simpleKafka-zh.pdf
```

The test report is always in English, independent of `COURSE_LANG`. Course tasks run the custom runner as `JavaExec`, print plain text, and write legacy JUnit XML under `build/student/reports/` or `build/reference/reports/`. The standard `:exercises:test` and `:reference:test` tasks support IDE and `--tests` debugging; their XML/HTML reports use `build/<mode>/gradle-test-results/` and `build/<mode>/gradle-test-reports/`. The runner retains its `NO_COLOR` / `TERM=dumb` color gate.

`docs/book/simpleKafka.tex` is the default English textbook entry point; `docs/book/simpleKafka-zh.tex` is the complete Chinese edition. After both languages have been built once, use `BOOK_OFFLINE=1 ./gradlew :book --offline` and `COURSE_LANG=zh BOOK_OFFLINE=1 ./gradlew :book --offline` for offline builds. `BOOK_OFFLINE=1` controls the TeX bundle cache; Gradle's `--offline` is separate. The Wrapper needs Java 17+ to launch PDF tasks, but `:book` does not compile Java or resolve JUnit.

## Strict behavioral coverage

Acceptance is defined by the course contract, not by test count or line coverage. Each `StepNNTest` is registered through `course/steps.tsv`; chapter-end commands cumulatively run that chapter and all prerequisites.

| Chapter / steps | Behaviors and boundaries |
|---|---|
| Records and log, 1–4 | Independent on-disk golden bytes; CRC, invalid fields, buffer prefixes, and maximum-record boundaries; contiguous offsets for batch append; hints and total budgets; nonzero bases, truncated-tail recovery, and rejection of complete corruption. |
| Index and segments, 5–8 | Independent index bytes and corrupt-index rebuild; segment rolling, cross-segment record/byte budgets, concurrent batches; retention boundaries, reopen after truncation, and a fixed-seed state model. |
| Routing and TCP, 9–12 | CRC/null-key routing and topic-metadata isolation; fixed frame bytes, short reads, truncation, and invalid wire fields; raw-socket request errors, absence of side effects, and subsequent connection survival. |
| Clients and offsets, 13–16 | Producer batches and partition order; no replay after append with a timed-out response; consumer total budgets, positions, and error boundaries; offset rewind and restart recovery; at-least-once duplicate windows after processor or commit failure. |
| Consumer groups, 17–20 | Lexicographic assignment and empty members; generation, heartbeat, and expiry; real TCP token FETCH/COMMIT rejection for stale, future, non-owner, and empty-owner cases; revoked/retained positions and committed-offset recovery after restart. |
| Replication and acknowledgments, 21–24 | Real TCP fetch, retained-start/offline-requester rejection, bad responses, and in-flight role/epoch changes; ISR/HW monotonicity and exact timeout boundaries, including invalid negative timeouts; ACK waits/interruption; concurrent append, HW visibility, and cross-broker progress. |
| Election and repair, 25–28 | Clean-candidate eligibility and unchanged state when no candidate exists; HW conflicts, batched repair, and invalid proofs; metadata refresh without replay and topic/partition routing isolation; proofs bound to log identity/version, stale after truncation or same-LEO rewrite, retention invalidation, readmission deadlines, plus minISR/HW/waiter recovery. |

This matrix covers representative boundaries in simpleKafka’s explicit teaching contract. It does not claim exhaustive inputs, fuzzing, production Kafka certification, or 100% code coverage. Run the complete reference contract with `./gradlew :referenceTest -Pstep=28`. After implementing the corresponding steps, run `./gradlew :test -Pstep=N`, replacing `N` with the step to verify.

## Scope

The project implements a subset of core mechanisms: three real TCP brokers in one JVM, separate disk logs and threads, and one trusted teaching authority. It does not provide KRaft/controller high availability, consensus under network partitions, Kafka wire compatibility, transactions, idempotent producers, compression, compaction, production-grade consumer-group rebalancing, or end-to-end exactly-once semantics. The textbook explains these boundaries step by step.
