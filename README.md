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

The course has 7 chapters and 28 steps. `exercises/` is the student workspace; `reference/` contains isolated complete answers and is never loaded onto the student classpath. Import the repository root as a Gradle Wrapper project in an IDE, and use `exercises` as the learning project. Tests run cumulatively through prerequisites; chapter boundaries are Steps 4, 8, 12, 16, 20, 24, and 28.

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

## Scope

The project implements a teaching subset: three real TCP brokers in one JVM, separate disk logs and threads, and one trusted authority. It is not Kafka-wire compatible and does not provide controller high availability, consensus under network partitions, transactions, idempotent producers, compression, compaction, production-grade consumer-group rebalancing, or end-to-end exactly-once semantics. The textbook explains these boundaries step by step.
