# simpleKafka：渐进式 Kafka 学习项目

[English](README.md) | 简体中文

用 Java 21+ 从可运行的 broker 开始，逐步补全持久化日志、TCP 请求、producer/consumer、消费组、复制、ISR、高水位与受控故障切换。项目协议是教学协议，不兼容 Kafka 客户端或 Kafka wire protocol。

## 环境

支持 Linux、macOS 与 Windows WSL2，x86_64 和 arm64；不支持原生 Windows。课程需要 Java 21+ JDK、GNU Make、Bash 3.2+、`find`、`awk`、`curl` 以及 `sha256sum` 或 `shasum`。SDKMAN 是可选的 Java 管理器；项目不会自动切换 JDK、安装系统软件或依赖作者本机路径。

```sh
make setup       # 只准备经过校验的 JUnit
make setup-book  # 单独准备平台对应的 Tectonic PDF 引擎
make doctor
make list        # 默认显示英文
make compile
```

课程测试不需要 PDF 引擎或系统字体。首次准备依赖需要联网；PDF 首次编译还会下载 TeX bundle 与其中的 Latin Modern/Fandol 字体。`tar` 和 `install` 仅用于准备 PDF 引擎。Noto、fontconfig、xelatex、latexmk、Maven、Gradle、Docker 与 Kafka 均不是项目依赖。

## 渐进式步骤

课程共 7 章、28 步。`exercises/` 是唯一默认学习源码；`reference/` 保存隔离的完整答案，默认 classpath 不会加载它。每步测试以前置步骤累计运行；章节末步骤为 4、8、12、16、20、24、28。README 与课程清单默认英文，也可查看中文步骤列表：

```sh
make list COURSE_LANG=zh
```

```sh
make test 1          # 累计检查第 1 步；骨架尚未填写时预期失败
make test STEP=12    # 累计检查第 1–12 步
make step-test 12    # 只定位第 12 步，不代表整章通过
make reference-test 28
make reference-demo
make book             # 英文 PDF: build/book/en/simpleKafka.pdf
make book COURSE_LANG=zh  # 中文 PDF: build/book/zh/simpleKafka-zh.pdf
```

测试报告始终使用英文，与 `COURSE_LANG` 无关。输出按步骤分组显示测试名称、各步和总计数；失败时展示断言、步骤/方法定位和重跑命令。JUnit XML 保存在 `build/student/reports/` 或 `build/reference/reports/`；重定向输出、设置 `NO_COLOR` 或 `TERM=dumb` 时不输出颜色。

`docs/book/simpleKafka.tex` 是默认英文教材入口，`docs/book/simpleKafka-zh.tex` 是完整中文教材入口。分别构建两种语言后，可用 `make book BOOK_OFFLINE=1` 与 `make book COURSE_LANG=zh BOOK_OFFLINE=1` 离线编译。

## 严格行为覆盖

课程验收不是按测试数量或行覆盖率定义；每个 `StepNNTest` 都通过 `course/steps.tsv` 加入对应闯关，章节末命令累计运行本章及所有前置步骤。

| 章节/步骤 | 主要行为与边界 |
|---|---|
| 记录与日志，1–4 | 独立磁盘 golden bytes；CRC、非法字段、buffer 前缀和最大记录边界；批量追加连续 offset；hint 与总预算；非零 base、截断尾恢复和完整损坏拒绝。 |
| 索引与分段，5–8 | 独立索引字节、坏索引重建；滚段、跨段记录/字节预算、并发批次；保留边界、截断重开和固定 seed 状态模型。 |
| 路由与 TCP，9–12 | CRC/null-key 路由、topic 元数据隔离；固定帧字节、短读/截断/错误 wire 字段；原始 socket 请求错误、无副作用与后续连接存活。 |
| 客户端与 offsets，13–16 | 生产批次和分区顺序、已追加但响应超时不重放；consumer 总预算/position/error 边界；offset 回退与重启恢复；处理异常和 commit 失败时的至少一次重复窗口。 |
| 消费组，17–20 | 字典序分配及空成员；generation、heartbeat、expiry；真实 TCP token FETCH/COMMIT 的 stale/future/non-owner/empty-owner 拒绝；撤销/保留 position、重启后恢复提交位点。 |
| 复制与确认，21–24 | 真实 TCP 拉取、保留起点/离线 requester 拒绝、坏响应和飞行中角色/epoch 变化；ISR/HW 单调与精确超时边界（含负超时拒绝）；ACK 等待/中断；并发追加、HW 可见性和跨 broker 进度。 |
| 选举与修复，25–28 | 干净候选资格与无候选状态不变；HW 冲突、分批修复与失败 proof；metadata 刷新不重放、topic/partition 路由隔离；proof 绑定日志身份/version，截短或同 LEO 改写不能 admission，保留清理后 proof 失效、重新 admission deadline 初始化，以及 minISR/HW/waiter 恢复。 |

该矩阵覆盖 simpleKafka 明确教学契约中的代表性边界，不是对所有输入的穷举、模糊测试、Kafka 生产实现认证或 100% 代码覆盖率声明。执行完整答案验收：`make reference-test 28`；学生完成相应步骤后可运行示例 `make test 28`，将 28 替换为当前要验收的步骤号。

## 边界

课程实现的是核心机制子集：单 JVM 内的三个真实 TCP broker、独立磁盘日志和线程、一个可信教学 authority。它不提供 KRaft/controller 高可用、网络分区下共识、Kafka wire compatibility、事务、幂等生产者、压缩、compaction、生产级消费组再平衡或端到端 exactly-once。教材逐步解释这些边界。
