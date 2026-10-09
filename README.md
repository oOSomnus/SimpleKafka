# simpleKafka: an incremental Kafka learning project

English | [简体中文](README.zh-CN.md)

Learn Kafka by completing a runnable Java broker in small steps: durable logs, TCP requests, producers and consumers, consumer groups, replication, ISR, high watermarks, and controlled failover. The project defines a teaching protocol; it is not compatible with Kafka clients or the Kafka wire protocol.

## Environment

Supported: Linux, macOS, and Windows WSL2 on x86_64 or arm64; native Windows is not supported. Use a Java 21+ JDK. No system Gradle installation is needed.

## Quick start

```sh
./gradlew :setup
./gradlew :doctor
./gradlew :list
./gradlew :compile
```

`:compile` checks that the student skeleton and tests compile; it does not mean the exercises are implemented.

## Incremental steps

The course has 8 chapters and 31 steps. `exercises/` is the student workspace; `reference/` contains isolated complete answers and is never loaded onto the student classpath. Import the repository root as a Gradle Wrapper project in an IDE, and use `exercises` as the learning project. Tests run cumulatively through prerequisites; chapter boundaries are Steps 4, 8, 12, 16, 20, 24, 28, and 31.

```sh
./gradlew :test -Pstep=1        # the empty exercise skeleton is expected to fail
./gradlew :test -Pstep=12       # cumulative through Steps 1–12
./gradlew :stepTest -Pstep=12   # diagnose Step 12 only; not chapter completion
COURSE_LANG=zh ./gradlew :list  # show Chinese step titles
```

## Documentation

- [Engineering guide](docs/engineering.md) · [简体中文工程指南](docs/engineering.zh-CN.md)
- Textbook sources: [English entry](docs/book/simpleKafka.tex) · [Chinese entry](docs/book/simpleKafka-zh.tex)
- Chapter 00: [English guide](docs/book/en/chapters/00-guide.tex) · [中文指南](docs/book/zh/chapters/00-guide.tex)
- Chapter 08: [Consistency experiments and idempotence](docs/book/en/chapters/08-consistency.tex) · [一致性实验与幂等性](docs/book/zh/chapters/08-consistency.tex)

### Prebuilt textbooks

Repository clones and source ZIPs include the compiled books; reading requires no Java or build tools.

| Language | PDF | EPUB |
|---|---|---|
| English | [PDF](docs/book/dist/en/simpleKafka.pdf) | [EPUB](docs/book/dist/en/simpleKafka.epub) |
| 简体中文 | [PDF](docs/book/dist/zh/simpleKafka-zh.pdf) | [EPUB](docs/book/dist/zh/simpleKafka-zh.epub) |

Maintainers rebuild the artifacts with `./gradlew :book` and `COURSE_LANG=zh ./gradlew :book`, then commit them alongside the TeX sources.

## Scope

The project implements a teaching subset: three real TCP brokers in one JVM, separate disk logs and threads, and one trusted authority. It teaches a limited idempotent-producer protocol with caller-managed identity and a durable local consumer-effect ledger, but these do not provide transactions or end-to-end exactly-once semantics. The project is not Kafka-wire compatible and does not provide controller high availability, consensus under network partitions, Kafka transactions, compression, compaction, production-grade consumer-group rebalancing, or production-grade exactly-once guarantees. The textbook explains these boundaries step by step.
