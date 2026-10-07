# simpleKafka: an incremental Kafka learning project

English | [简体中文](README.zh-CN.md)

Learn Kafka by completing a runnable Java broker in small steps: durable logs, TCP requests, producers and consumers, consumer groups, replication, ISR, high watermarks, and controlled failover. The project defines a teaching protocol; it is not compatible with Kafka clients or the Kafka wire protocol.

## Environment

Supported: Linux, macOS, and Windows WSL2 on x86_64 or arm64. Native Windows is not supported. The course requires a Java 21+ JDK, GNU Make, Bash 3.2+, `find`, `awk`, `curl`, and either `sha256sum` or `shasum`. SDKMAN is optional; the project does not switch JDKs, install system packages, or depend on author-specific paths.

```sh
make setup       # prepare only the checksum-verified JUnit console
make setup-book  # separately prepare the platform-specific Tectonic PDF engine
make doctor
make list        # English by default
make compile
```

Course tests do not need the PDF engine or system fonts. Initial dependency setup needs network access; the first PDF build also downloads a TeX bundle and its Latin Modern/Fandol fonts. `tar` and `install` are used only to prepare the PDF engine. Noto, fontconfig, xelatex, latexmk, Maven, Gradle, Docker, and Kafka are not project dependencies.

## Incremental steps

The course has 7 chapters and 28 steps. `exercises/` is the only default learning workspace; `reference/` contains isolated complete answers and is never loaded onto the default student classpath. Tests are cumulative through prerequisites; chapter boundaries are Steps 4, 8, 12, 16, 20, 24, and 28. To list Chinese step titles:

```sh
make list COURSE_LANG=zh
```

```sh
make test 1                 # cumulative through Step 1; the empty skeleton is expected to fail
make test STEP=12           # cumulative through Steps 1–12
make step-test 12           # diagnose only Step 12; not chapter completion
make reference-test 28
make reference-demo
make book                   # English PDF: build/book/en/simpleKafka.pdf
make book COURSE_LANG=zh    # Chinese PDF: build/book/zh/simpleKafka-zh.pdf
```

`docs/book/simpleKafka.tex` is the default English textbook entry point; `docs/book/simpleKafka-zh.tex` is the complete Chinese edition. After both languages have been built once, use `make book BOOK_OFFLINE=1` and `make book COURSE_LANG=zh BOOK_OFFLINE=1` for offline builds.

## Strict behavioral coverage

Acceptance is defined by the course contract, not by test count or line coverage. Each `StepNNTest` is registered through `course/steps.tsv`; chapter-end commands cumulatively run that chapter and all prerequisites.

| Chapter / steps | Behaviors and boundaries |
|---|---|
| Records and log, 1–4 | Independent on-disk golden bytes; CRC, invalid fields, buffer prefixes, and maximum-record boundaries; contiguous offsets for batch append; hints and total budgets; nonzero bases, truncated-tail recovery, and rejection of complete corruption. |
| Index and segments, 5–8 | Independent index bytes and corrupt-index rebuild; segment rolling, cross-segment record/byte budgets, concurrent batches; retention boundaries, reopen after truncation, and a fixed-seed state model. |
| Routing and TCP, 9–12 | CRC/null-key routing and topic-metadata isolation; fixed frame bytes, short reads, truncation, and invalid wire fields; raw-socket request errors, absence of side effects, and subsequent connection survival. |
| Clients and offsets, 13–16 | Producer batches and partition order; no replay after append with a timed-out response; consumer total budgets, positions, and error boundaries; offset rewind and restart recovery; at-least-once duplicate windows after processor or commit failure. |
| Consumer groups, 17–20 | Lexicographic assignment and empty members; generation, heartbeat, and expiry; real TCP token FETCH/COMMIT rejection for stale, future, non-owner, and empty-owner cases; revoked/retained positions and committed-offset recovery after restart. |
| Replication and acknowledgments, 21–24 | Real TCP fetch, bad responses, and in-flight role/epoch changes; ISR/HW monotonicity and exact timeout boundaries; ACK waits/interruption; concurrent append, HW visibility, and cross-broker progress. |
| Election and repair, 25–28 | Clean-candidate eligibility and unchanged state when no candidate exists; HW conflicts, batched repair, and invalid proofs; metadata refresh without replay and topic/partition routing isolation; proofs bound to log identity/version, stale after truncation or same-LEO rewrite, plus minISR/HW/waiter recovery. |

This matrix covers representative boundaries in simpleKafka’s explicit teaching contract. It does not claim exhaustive inputs, fuzzing, production Kafka certification, or 100% code coverage. Run the complete reference contract with `make reference-test 28`. After implementing the corresponding steps, run `make test N`, replacing `N` with the step to verify.

## Scope

The project implements a subset of core mechanisms: three real TCP brokers in one JVM, separate disk logs and threads, and one trusted teaching authority. It does not provide KRaft/controller high availability, consensus under network partitions, Kafka wire compatibility, transactions, idempotent producers, compression, compaction, production-grade consumer-group rebalancing, or end-to-end exactly-once semantics. The textbook explains these boundaries step by step.
