# simpleKafka：渐进式 Kafka 学习项目

[English](README.md) | 简体中文

用 Java 21+ 从可运行的 broker 开始，逐步补全持久化日志、TCP 请求、producer/consumer、消费组、复制、ISR、高水位与受控故障切换。项目协议是教学协议，不兼容 Kafka 客户端或 Kafka wire protocol。

## 环境

支持 Linux、macOS 与 Windows WSL2，x86_64 和 arm64；不支持原生 Windows。使用 Java 21+ JDK，无需安装系统 Gradle。

## 快速开始

```sh
./gradlew :setup
./gradlew :doctor
./gradlew :list
./gradlew :compile
```

`:compile` 检查学生骨架和测试能否编译，不代表练习已经实现。

## 渐进式步骤

课程共 8 章、31 步。`exercises/` 是学生学习源码；`reference/` 保存隔离的完整答案，不会加载到学生 classpath。在 IDE 中从仓库根目录导入 Gradle Wrapper 项目，并以 `exercises` 为学习项目。测试会累计运行前置步骤；章节末步骤为 4、8、12、16、20、24、28、31。

```sh
./gradlew :test -Pstep=1        # 空练习骨架预期失败
./gradlew :test -Pstep=12       # 累计运行第 1–12 步
./gradlew :stepTest -Pstep=12   # 仅诊断第 12 步，不代表整章完成
COURSE_LANG=zh ./gradlew :list  # 显示中文步骤标题
```

## 文档

- [工程指南](docs/engineering.zh-CN.md) · [English engineering guide](docs/engineering.md)
- 教材源码：[英文入口](docs/book/simpleKafka.tex) · [中文入口](docs/book/simpleKafka-zh.tex)
- 第 00 章：[English guide](docs/book/en/chapters/00-guide.tex) · [中文指南](docs/book/zh/chapters/00-guide.tex)
- 第 08 章：[Consistency experiments and idempotence](docs/book/en/chapters/08-consistency.tex) · [一致性实验与幂等性](docs/book/zh/chapters/08-consistency.tex)

## 边界

课程实现的是教学机制子集：单 JVM 内的三个真实 TCP broker、独立磁盘日志和线程、一个可信 authority。课程会教学一个由调用方管理身份的有限幂等生产者协议，以及持久化的本地消费副作用账本；但它们不提供事务或端到端 exactly-once 语义。本项目不兼容 Kafka wire protocol，也不提供 controller 高可用、网络分区下共识、Kafka 事务、压缩、compaction、生产级消费组再平衡或生产级 exactly-once 保证。教材会逐步解释这些边界。
