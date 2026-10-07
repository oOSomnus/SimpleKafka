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

## 边界

课程实现的是核心机制子集：单 JVM 内的三个真实 TCP broker、独立磁盘日志和线程、一个可信教学 authority。它不提供 KRaft/controller 高可用、网络分区下共识、Kafka wire compatibility、事务、幂等生产者、压缩、compaction、生产级消费组再平衡或端到端 exactly-once。教材逐步解释这些边界。
