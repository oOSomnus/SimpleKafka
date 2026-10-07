# simpleKafka：渐进式 Kafka 学习项目

用 Java 21 从可运行的 broker 开始，逐步补全持久化日志、TCP 请求、producer/consumer、消费组、复制、ISR、高水位与受控故障切换。项目协议是教学协议，不兼容 Kafka 客户端或 Kafka wire protocol。

## 环境

需要 SDKMAN 管理的 Java 21 JDK、GNU Make、Bash、curl、tar 与 sha256sum。PDF 需要 Tectonic 初次下载 LaTeX bundle；中文排版优先使用 Noto CJK 字体。项目不会切换 SDKMAN Java 或安装系统软件。

```sh
make setup
make doctor
make list
make compile
```

## 渐进式步骤

课程共 7 章、28 步。`exercises/` 是唯一默认学习源码；`reference/` 保存隔离的完整答案，默认 classpath 不会加载它。每步测试以前置步骤累计运行；章节末步骤为 4、8、12、16、20、24、28。

```sh
make test 1          # 累计检查第 1 步；骨架尚未填写时预期失败
make test STEP=12    # 累计检查第 1–12 步
make step-test 12    # 只定位第 12 步，不代表整章通过
make reference-test 28
make reference-demo
make book
```

中文教材：`docs/book/simpleKafka.tex`；PDF 输出：`build/book/simpleKafka.pdf`。首次准备工具联网获取经 SHA-256 校验的固定版本 JUnit 与 Tectonic；PDF 首次编译另需下载 TeX bundle，缓存后可用 `make book BOOK_OFFLINE=1`。

## 严格行为覆盖

课程验收不是按测试数量或行覆盖率定义；每个 `StepNNTest` 都通过 `course/steps.tsv` 加入对应闯关，章节末命令累计运行本章及所有前置步骤。

| 章节/步骤 | 主要行为与边界 |
|---|---|
| 记录与日志，1–4 | 独立磁盘 golden bytes；CRC、非法字段、buffer 前缀和最大记录边界；批量追加连续 offset；hint 与总预算；非零 base、截断尾恢复和完整损坏拒绝。 |
| 索引与分段，5–8 | 独立索引字节、坏索引重建；滚段、跨段记录/字节预算、并发批次；保留边界、截断重开和固定 seed 状态模型。 |
| 路由与 TCP，9–12 | CRC/null-key 路由、topic 元数据隔离；固定帧字节、短读/截断/错误 wire 字段；原始 socket 请求错误、无副作用与后续连接存活。 |
| 客户端与 offsets，13–16 | 生产批次和分区顺序、已追加但响应超时不重放；consumer 总预算/position/error 边界；offset 回退与重启恢复；处理异常和 commit 失败时的至少一次重复窗口。 |
| 消费组，17–20 | 字典序分配及空成员；generation、heartbeat、expiry；真实 TCP token FETCH/COMMIT 的 stale/future/non-owner/empty-owner 拒绝；撤销/保留 position、重启后恢复提交位点。 |
| 复制与确认，21–24 | 真实 TCP 拉取、坏响应和飞行中角色/epoch 变化；ISR/HW 单调与精确超时；ACK 等待/中断；并发追加、HW 可见性和跨 broker 进度。 |
| 选举与修复，25–28 | 干净候选资格与无候选状态不变；HW 冲突、分批修复与失败 proof；metadata 刷新不重放、topic/partition 路由隔离；proof 绑定日志身份/version，截短或同 LEO 改写不能 admission，minISR/HW/waiter 恢复。 |

该矩阵覆盖 simpleKafka 明确教学契约中的代表性边界，不是对所有输入的穷举、模糊测试、Kafka 生产实现认证或 100% 代码覆盖率声明。执行完整答案验收：`make reference-test 28`；学生完成相应步骤后可运行示例 `make test 28`，将 28 替换为当前要验收的步骤号。


## 边界

课程实现的是核心机制子集：单 JVM 内的三个真实 TCP broker、独立磁盘日志和线程、一个可信教学 authority。它不提供 KRaft/controller 高可用、网络分区下共识、Kafka wire compatibility、事务、幂等生产者、压缩、compaction、生产级消费组再平衡或端到端 exactly-once。教材逐步解释这些边界。
