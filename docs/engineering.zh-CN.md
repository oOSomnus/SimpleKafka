# 工程指南

[项目 README](../README.zh-CN.md) · [English](engineering.md)

## 环境与依赖

Gradle 9.8.1 Wrapper 可由 Java 17+ 启动；Java 编译、测试、demo 和源码一致性校验需要 Java 21+ JDK。无需安装系统 Gradle。SDKMAN 可选；项目不会自动切换 JDK 或安装系统软件。

Shell 脚本需要 Bash 3.2+、`find`、`awk`、`curl`，以及 `sha256sum` 或 `shasum`。准备可选 PDF 工具还会使用 `tar` 和 `install`。`./gradlew :setup` 通过 Gradle dependency verification 解析 JUnit 1.11.4；首次解析需要联网。课程测试不需要 PDF 引擎或系统字体。`./gradlew :setupBook` 单独准备固定版本 Tectonic 0.17.0。首次 PDF 构建会下载 TeX bundle 和 Latin Modern/Fandol 字体。Noto、fontconfig、xelatex、latexmk、Maven、Docker、Kafka 均不是项目依赖。

## 任务与测试报告

`:compile` 编译学生骨架和共享测试；编译成功不代表练习方法已实现。`./gradlew :test -Pstep=N` 累计运行学生契约至第 `N` 步。`./gradlew :stepTest -Pstep=N` 仅运行单步以便定位问题，不代表章节验收。章节末累计步骤为 4、8、12、16、20、24、28。完整答案契约和真实 TCP/failover 场景：

```sh
./gradlew :referenceTest -Pstep=28
./gradlew :referenceDemo
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

## 教材 PDF 与离线构建

`./gradlew :setupBook` 准备固定版本的 PDF 引擎。`./gradlew :book` 默认构建英文版；设置 `COURSE_LANG=zh` 构建中文版。输出文件分别为 `build/book/en/simpleKafka.pdf` 和 `build/book/zh/simpleKafka-zh.pdf`。Wrapper 启动 PDF 任务需要 Java 17+，但 `:book` 不编译 Java，也不解析 JUnit。先在线构建两种语言，再离线构建：

```sh
BOOK_OFFLINE=1 ./gradlew :book --offline
COURSE_LANG=zh BOOK_OFFLINE=1 ./gradlew :book --offline
```

`BOOK_OFFLINE=1` 选择缓存中的 TeX bundle；Gradle `--offline` 独立控制 Gradle 依赖解析。语言相关的自定义构建选项见[英文契约附录](book/en/appendices/contracts.tex)和[中文契约附录](book/zh/appendices/contracts.tex)。

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
| [选举与修复](book/zh/chapters/07-failover.tex)，25–28 | 干净候选资格与无候选时状态不变；HW 冲突、批量修复和无效 proof；metadata 刷新不重放及 topic/partition 路由隔离；proof 绑定日志身份/version、截断或同 LEO 改写后失效、保留清理失效、readmission deadline，以及 minISR/HW/waiter 恢复。 |

完整答案验收：`./gradlew :referenceTest -Pstep=28`。学生完成相应步骤后，用 `./gradlew :test -Pstep=N` 验收指定步骤。
