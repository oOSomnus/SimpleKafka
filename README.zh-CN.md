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

## 其他 Gradle 任务

以上命令涵盖快速开始与学生逐步验收。项目中的其他常用任务：

| 命令 | 用途 |
|---|---|
| `./gradlew :demo` · `./gradlew :consistencyDemo` | 运行学生实现的 TCP/failover 与一致性场景。 |
| `./gradlew :referenceTest -Pstep=N` | 累计运行参考实现至第 `N` 步的契约测试。 |
| `./gradlew :referenceDemo` · `./gradlew :referenceConsistencyDemo` | 运行对应的参考实现演示场景。 |
| `./gradlew :validateCourse` | 校验双语课程清单。 |
| `./gradlew :verifySourceParity` · `./gradlew :sourceParitySelfTest` | 检查练习与参考实现的结构，并运行 parity 检查器的 fixture 自测。 |
| `./gradlew :format` · `./gradlew :formatCheck` | 格式化 Java 源码，或只检查格式而不修改文件。 |
| `./gradlew :exercises:test --tests 'io.simplekafka.course.Step31Test'` · `./gradlew :reference:test --tests 'io.simplekafka.course.Step31Test'` | 使用 Gradle 标准 JUnit 测试任务，供 IDE 或筛选测试排查问题。 |
| `./gradlew :assemble` · `./gradlew :check` · `./gradlew :build` · `./gradlew :clean` | 运行 Gradle 生命周期任务；`:clean` 删除生成的构建输出，不删除已提交的教材。 |

准备教材工具使用 `./gradlew :setupBook`；双语 PDF/EPUB 编译命令见[已编译教材](#已编译教材)。

运行 `./gradlew tasks --all` 可查看项目任务与 Gradle 生成的任务。

## 文档

- [工程指南](docs/engineering.zh-CN.md) · [English engineering guide](docs/engineering.md)
- 教材源码：[英文入口](docs/book/simpleKafka.tex) · [中文入口](docs/book/simpleKafka-zh.tex)
- 第 00 章：[English guide](docs/book/en/chapters/00-guide.tex) · [中文指南](docs/book/zh/chapters/00-guide.tex)
- 第 08 章：[Consistency experiments and idempotence](docs/book/en/chapters/08-consistency.tex) · [一致性实验与幂等性](docs/book/zh/chapters/08-consistency.tex)

### 已编译教材

Git 克隆与源码 ZIP 均包含已编译教材；阅读 PDF/EPUB 无需 Java、Gradle 或排版工具。

| 语言 | PDF | EPUB |
|---|---|---|
| English | [PDF](docs/book/dist/en/simpleKafka.pdf) | [EPUB](docs/book/dist/en/simpleKafka.epub) |
| 简体中文 | [PDF](docs/book/dist/zh/simpleKafka-zh.pdf) | [EPUB](docs/book/dist/zh/simpleKafka-zh.epub) |

维护者更新教材时运行 `./gradlew :book` 与 `COURSE_LANG=zh ./gradlew :book`，再将成品与 TeX 源码一起提交。

## 边界

课程实现的是教学机制子集：单 JVM 内的三个真实 TCP broker、独立磁盘日志和线程、一个可信 authority。课程会教学一个由调用方管理身份的有限幂等生产者协议，以及持久化的本地消费副作用账本；但它们不提供事务或端到端 exactly-once 语义。本项目不兼容 Kafka wire protocol，也不提供 controller 高可用、网络分区下共识、Kafka 事务、压缩、compaction、生产级消费组再平衡或生产级 exactly-once 保证。教材会逐步解释这些边界。
