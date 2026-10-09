# 工程指南

[项目 README](../README.zh-CN.md) · [English](engineering.md)

## 环境与依赖

Gradle 9.8.1 Wrapper 可由 Java 17+ 启动；Java 编译、测试、demo 和源码一致性校验需要 Java 21+ JDK。无需安装系统 Gradle。SDKMAN 可选；项目不会自动切换 JDK 或安装系统软件。

Shell 脚本需要 Bash 3.2+、`find`、`awk`、`curl`，以及 `sha256sum` 或 `shasum`。准备固定版本的 Tectonic/Pandoc 使用 `install` 和 `tar`；macOS 解压 Pandoc 还需要 `unzip`。`./gradlew :setupBook` 将 checksum 校验后的 Tectonic 0.17.0 与 Pandoc 3.12.1 安装到 `.tools/`，并检查 Poppler 的 `pdftoppm`，但不安装系统软件。Linux/WSL 使用 `sudo apt-get install poppler-utils`，macOS 使用 `brew install poppler` 安装 Poppler。课程测试不需要教材工具或系统字体。首次 PDF/EPUB 构建会下载 TeX bundle 和 Latin Modern/Fandol 字体。Noto、fontconfig、xelatex、latexmk、Maven、Docker、Kafka 均不是项目依赖。

## 任务与测试报告

`:compile` 编译学生骨架和共享测试；编译成功不代表练习方法已实现。`./gradlew :test -Pstep=N` 累计运行学生契约至第 `N` 步。`./gradlew :stepTest -Pstep=N` 仅运行单步以便定位问题，不代表章节验收。课程共 8 章、31 步；章节末累计步骤为 4、8、12、16、20、24、28、31。完整答案契约和真实 TCP 场景：

```sh
./gradlew :referenceTest -Pstep=31
./gradlew :referenceDemo
./gradlew :referenceConsistencyDemo
```

课程任务通过 `JavaExec` 运行 `CourseTestRunner`，输出纯文本，并将旧格式 JUnit XML 写入 `build/student/reports/` 或 `build/reference/reports/`。标准 `:exercises:test` 与 `:reference:test` 仍可用于 IDE 和 `--tests` 调试；其 XML/HTML 报告位于 `build/<mode>/gradle-test-results/` 与 `build/<mode>/gradle-test-reports/`。报告始终使用英文，与 `COURSE_LANG` 无关。runner 保留 `NO_COLOR` / `TERM=dumb` 颜色门禁。

## 源码骨架一致性

校验器比较 `reference/src/main/java` 和 `exercises/src/main/java` 下的全部文件，不编译或加载任一实现。Java 文件使用 JDK 公共 compiler-tree API 解析；非 Java 文件必须逐字节相等。注释、Javadoc、排版和等价字面量写法不影响比较。这是 AST 结构契约，不证明算法行为等价。

`course/source-parity.tsv` 是显式五列策略：`kind`、`side`、`path`、`symbol`、`reason`。`method-body` / `both` 只屏蔽指定方法体；签名、参数名、annotations、修饰符和 throws 仍比较。`private-member` / `reference` 或 `exercises` 只移除单侧、显式 `private` 的实现成员；构造器不能使用此规则。两侧共有的 private 成员、已提供方法体、构造器、常量、字段初值和成员顺序仍比较。精确 import 仅在简单名不出现在投影 AST 中时忽略；wildcard import 始终比较。

路径相对源码根。方法符号包含词法 owner 和参数类型，例如 `RecordCodec#encode(LogRecord)`；字段使用 `Owner#field:name`，嵌套类型使用 `Owner#type:Name`。重复、过期、重叠或非法规则都会 fail closed。登记的 private 成员必须保持 private，且另一侧不能出现同符号成员；豁免单个字段前应拆分多变量字段声明。诊断包含源码文件、符号、原始行号和首个差异附近的有限片段。退出码：**0** 表示一致，**1** 表示源码/文件漂移，**2** 表示调用、环境、策略或解析错误。

运行 Gradle 门禁及确定性自检，或使用不依赖 Gradle 的 Java 21+ 命令：

```sh
./gradlew :verifySourceParity
./gradlew :sourceParitySelfTest
java --source 21 scripts/SourceParity.java verify .
java --source 21 scripts/SourceParity.java self-test
```

`:check` 与 CI 强制执行一致性门禁。学生的 `:test`、`:stepTest`、`:compile` 不依赖该门禁。受控课程骨架应通过；失败表示存在未登记的结构漂移，或策略/解析输入无效，而不是要求学生算法与答案一致。

## 代码格式化

沿用 Google Java Format 1.28.0。`:format` 和 `:formatCheck` 覆盖 `exercises/`、`reference/`、`provided/`、`tests/`、`scripts/` 中的 Java 源码；使用 AOSP 风格并保留超长字符串字面量。

```sh
./gradlew :format
./gradlew :formatCheck
```

## 教材 PDF/EPUB 构建与分发

`./gradlew :setupBook` 准备固定版本的 Tectonic 与 Pandoc；Poppler（`pdftoppm`）需要单独安装。`./gradlew :book` 默认构建英文版，设置 `COURSE_LANG=zh` 构建中文版。每条命令会生成 PDF 和 EPUB 到 `build/book/<lang>/`，并且只更新所选语言的两个 `docs/book/dist/<lang>/` 分发文件：

| 语言 | 构建输出 | 随源码分发 |
|---|---|---|
| English | `build/book/en/simpleKafka.{pdf,epub}` | `docs/book/dist/en/simpleKafka.{pdf,epub}` |
| 简体中文 | `build/book/zh/simpleKafka-zh.{pdf,epub}` | `docs/book/dist/zh/simpleKafka-zh.{pdf,epub}` |

仓库克隆和源码 ZIP 都包含四份已提交成品，读者无需 Java、Gradle、Tectonic、Pandoc 或 Poppler。维护者使用以下命令重建两种语言：

```sh
./gradlew :book
COURSE_LANG=zh ./gradlew :book
```

将生成文件与 TeX 源码一起提交。Wrapper 启动这些任务需要 Java 17+，但 `:book` 不编译 Java，也不解析 JUnit。

离线构建前先准备工具并在线构建两种语言，再运行：

```sh
./gradlew :setupBook
BOOK_OFFLINE=1 ./gradlew :book --offline
COURSE_LANG=zh BOOK_OFFLINE=1 ./gradlew :book --offline
```

`BOOK_OFFLINE=1` 为两次 Tectonic 调用启用 TeX bundle only-cached 模式；Gradle `--offline` 独立控制 Gradle 依赖解析。`PANDOC` 可指定可执行文件，但版本必须是 3.12.1；`TECTONIC` 与 `TEX_BUNDLE` 保留原有覆盖行为。语言相关的自定义构建选项见[英文契约附录](book/en/appendices/contracts.tex)和[中文契约附录](book/zh/appendices/contracts.tex)。

## 验收与行为覆盖

每个 `StepNNTest` 都登记在 [`course/steps.tsv`](../course/steps.tsv) 中。章节末命令会累计运行该步及其前置步骤。下表列出教学契约中的代表性边界；它不是完整穷举，不表示做过模糊测试、生产 Kafka 认证或达到 100% 代码覆盖率。

| 章节 / 步骤 | 契约重点 |
|---|---|
| [记录与日志](book/zh/chapters/01-record-log.tex)，1–4 | 独立磁盘 golden bytes；CRC、非法字段、buffer 前缀及最大记录边界；批量追加的连续 offset；hint 与总预算；非零 base、截断尾恢复及完整损坏拒绝。 |
| [索引与分段](book/zh/chapters/02-segments-index.tex)，5–8 | 独立索引字节与损坏索引重建；滚段、跨段记录/字节预算、并发批次；保留边界、截断后重开及固定 seed 状态模型。 |
| [路由与 TCP](book/zh/chapters/03-partition-network.tex)，9–12 | CRC/null-key 路由与 topic 元数据隔离；固定帧字节、短读、截断及非法 wire 字段；原始 socket 请求错误、无副作用和后续连接存活。 |
| [客户端与 offsets](book/zh/chapters/04-client-offset.tex)，13–16 | producer 批次和分区顺序；追加后响应超时不重放；consumer 总预算、position 和错误边界；offset 回退与重启恢复；处理或 commit 失败后的至少一次重复窗口。 |
| [消费组](book/zh/chapters/05-consumer-groups.tex)，17–20 | 字典序分配与空成员；generation、heartbeat 和 expiry；真实 TCP token FETCH/COMMIT 对 stale、future、non-owner、empty-owner 的拒绝；撤销/保留 position 与重启后 committed offset 恢复。 |
| [复制与确认](book/zh/chapters/06-replication.tex)，21–24 | 真实 TCP 拉取、保留起点/离线 requester 拒绝、错误响应和飞行中角色/epoch 变化；ISR/HW 单调性与精确超时边界（含负超时拒绝）；ACK 等待/中断；并发追加、HW 可见性和跨 broker 进度。 |
| [跨层一致性](book/zh/chapters/08-consistency.tex)，29–31 | producer ACK、broker 复制/HW、consumer position/commit 与 stale generation 的真实 TCP 故障窗口；stamped producer batch 身份、显式重试及磁盘恢复；回调重放时以 durable event id 去重。 |

完整答案契约：`./gradlew :referenceTest -Pstep=31`；随后运行 demo：`./gradlew :referenceDemo` 与 `./gradlew :referenceConsistencyDemo`。学生完成对应步骤后，用 `./gradlew :test -Pstep=N` 验收指定步骤。
