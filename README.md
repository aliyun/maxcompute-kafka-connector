# Welcome to MaxCompute Kafka Connector!

## Quick Start

### 前置条件

- Java 8 或更高版本
- Maven 3.x
- Kafka 2.x 或 3.x
- MaxCompute 账号和项目

### 1. 安装 Kafka

#### Linux
```bash
# 下载 Kafka
wget https://archive.apache.org/dist/kafka/3.4.0/kafka_2.13-3.4.0.tgz
tar -xzf kafka_2.13-3.4.0.tgz
cd kafka_2.13-3.4.0

# 启动 Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties &

# 启动 Kafka
bin/kafka-server-start.sh config/server.properties &
```

### 2. 构建项目

```bash
# 克隆项目（如果还没有）
git clone https://github.com/aliyun/maxcompute-kafka-connector.git
cd maxcompute-kafka-connector

# 使用 Maven 构建
mvn clean package

# 构建完成后，会在 target 目录下生成 jar 包
# target/kafka-connect-maxcompute-2.3.0.jar
# target/kafka-connect-maxcompute-2.3.0-jar-with-dependencies.jar
```

### 3. 配置 Connector

创建配置文件 `mc-sink-connector.properties`：

```properties
name=MaxComputeSinkConnector
connector.class=com.aliyun.odps.kafka.connect.MaxComputeSinkConnector
tasks.max=3
topics=your_topic_name
endpoint=your_odps_endpoint
tunnel_endpoint=your_tunnel_endpoint
project=your_project_name
table=your_table_name
account_type=ALIYUN
access_id=your_access_id
access_key=your_access_key
format=TEXT
mode=VALUE
partition_window_type=HOUR
buffer_size_kb=65536
```

### 4. 启动 Kafka Connect

```bash
# 复制 jar 包到 Kafka Connect plugins 目录
mkdir -p $KAFKA_HOME/plugins/maxcompute-connector
cp target/kafka-connect-maxcompute-2.3.0-jar-with-dependencies.jar $KAFKA_HOME/plugins/maxcompute-connector/

# 启动 Kafka Connect
bin/connect-standalone.sh config/connect-standalone.properties mc-sink-connector.properties
```

### 5. 创建 Kafka Topic 并写入测试数据

```bash
# 创建测试 topic
bin/kafka-topics.sh --create --topic your_topic_name --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# 写入测试数据
echo "Hello MaxCompute" | bin/kafka-console-producer.sh --topic your_topic_name --bootstrap-server localhost:9092

# 或者使用生产者发送多条数据
for i in {1..10}; do
  echo "Test message $i: $(date)" | bin/kafka-console-producer.sh --topic your_topic_name --bootstrap-server localhost:9092
done
```

### 6. 验证数据

登录 MaxCompute 控制台，查看数据是否成功写入到目标表中：

```sql
-- 查看表数据
SELECT * FROM your_table_name;
```

### 7. 运行测试

测试分两档，靠 JUnit `@Category(IntegrationTest.class)` 区分，配置在 `pom.xml` 里：

| 档位 | 命令 | 依赖 | 说明 |
| --- | --- | --- | --- |
| 单元回归（PR 门禁） | `mvn clean test` | 无需云凭据、无需 Kafka broker | 纯转换 / 配置 / 类型映射；CI 每次 PR 都跑 |
| 集成回归 | `mvn clean test -Pintegration-tests` | 真实 MaxCompute 凭据（Kafka 用内嵌 cluster） | 只有显式开启该 profile 才执行 |

#### 7.1 单元回归（提交 PR 前本地先跑）

```bash
# 与 GitHub Actions 的 CI 任务完全等价的两步
./scripts/verify-ci.sh unit
```

它会执行 `mvn clean test`（自动排除 `IntegrationTest` 分类），再用
`tools/check-test-reports.py` 校验 surefire 报告：发现的测试类集合必须与
`tools/test-manifest.json` 完全一致，用例数不得低于清单下限，且失败 / 错误 / 跳过都为 0。
新增或删除测试类时请同步更新该清单（PR 描述里说明一句即可）。

想确认门禁本身可信（故意失败的用例会阻断、缺凭据的集成档不会假绿）：

```bash
./scripts/verify-ci.sh drill
```

#### 7.2 集成回归（需要云凭据）

先配置环境变量，缺任意一项时集成用例会直接失败并提示，不会被记为"集成通过"：

```bash
export ALIBABA_CLOUD_ACCESS_KEY_ID="your_access_id"
export ALIBABA_CLOUD_ACCESS_KEY_SECRET="your_access_key"
export odps_endpoint="your_odps_endpoint"
export MAXCOMPUTE_PROJECT="your_project_name"

# 全部集成用例
./scripts/verify-ci.sh integration

# 只跑某一个入口
mvn test -Pintegration-tests -Dtest=TestMaxComputeSinkConnectorIntegration
```

集成用例会用 `TestTableUtils` 在目标 project 里建 `kafka_*_test_table` 等测试表，
请在专用测试 project 上运行。GitHub 上的入口是 `Integration tests (Kafka + MaxCompute)`
工作流（手动触发，需要先在 repository secrets 里配置 `MAXCOMPUTE_ACCESS_KEY_ID`、
`MAXCOMPUTE_ACCESS_KEY_SECRET`、`MAXCOMPUTE_ENDPOINT`、`MAXCOMPUTE_PROJECT`）。

## Configuration example
````$xslt
{
    "name": "your_name",
    "config": {
	"connector.class": "com.aliyun.odps.kafka.connect.MaxComputeSinkConnector",
	"tasks.max": "3",
	"topics": "your_topic",
	"endpoint": "endpoint",
	"tunnel_endpoint": "your_tunnel endpoin
	"project": "project",
	"schema":"",
	"table": "your_table",
	"account_type": "account type (STS or ALIYUN)",
	"access_id": "access id",
	"access_key": "access key",
	"account_id": "account id for sts",
	"sts.endpoint": "sts endpoint",
	"region_id": "region id for sts",
	"role_name": "role name for sts",
	"client_timeout_ms": "STS Token valid period (ms)",
	"format": "TEXT",
	"csv_delimiter": "\t",
	"mode": "KEY",
	"partition_window_type": "MINUTE",
	"use_new_partition_format":true,
	"buffer_size_kb": 65536,
	"runtime.error.topic.name":"kafka topic when runtime errors happens",
	"runtime.error.topic.bootstrap.servers":"kafka bootstrap servers of error topic queue",
	"skip_error":"false"
    }
}
````

## Configuration detail
- name：kafka connector的唯一名称。再次尝试使用相同名称将失败。
- tasks.max：为此connector应创建的最大任务数，必须为大于0的整数。如果connector无法实现此级别的并行，则创建较少的任务。
- topics：用作此connector输入的topic列表。
- endpoint：访问MaxCompute的endpoint。
- tunnel_endpoint: 访问MaxCompute tunnel 的endpoint，默认为""(自动路由，在某些docker环境下会存在外部无法访问的情况)
- project：MaxCompute表所在的project。
- schema: 适用于带有schema层级的project,默认为""
- table：要写入的MaxCompute表。
- account_type：MaxCompute鉴权方式，选项为STS或ALIYUN，默认ALIYUN。
- access_id和access_key：若account_type为ALIYUN，则这两项配置为用户的access_id和access_key。否则保持为空即可，但不能不配置这两项。
- account_id、region_id、role_name和client_timeout_ms：生成STS Token所需信息。若account_type为STS，则如实配置。否则可以不配置。
- client_timeout_ms：刷新STS Token的时间间隔，单位为毫秒，默认值为11小时对应的毫秒数。
- sts.endpoint：可选配置，保持默认即可。
- format：消息的格式，详细解释见官方文档，可选值为TEXT、BINARY与CSV，默认TEXT。
- csv_delimiter：CSV消息的字段分隔符，仅在format为CSV时生效，默认为逗号（,）。Tab分隔符配置为"\t"。
- mode：此connector的处理模式，详细解释见官方文档，可选值为：KEY，VALUE，DEFAULT，默认DEFAULT。
- partition_window_type：如何按照系统时间进行数据分区。例如，若配置为MINUTE，则每分钟开始时数据写到一个新的分区。可选值DAY、HOUR、MINUTE，默认HOUR。
- use_new_partition_format:是否启用新的partitiont value 格式，true代表使用yyyy-MM-dd,否则使用MM-dd-yyyy
- buffer_size_kb: 每个 odps partition writer 内部缓冲区大小，单位 KB。默认 65536 （64MB）
- runtime.error.topic.name: 当connect内部写入某条数据发生未知错误时, 将错误记录写入Kafka消息队列中.默认为空
- runtime.error.topic.bootstrap.servers: 与runtime.error.topic.name搭配使用, 错误消息写入Kafka的bootstrap servers地址
- skip_error: 是否跳过发生未知写入错误的记录, 默认false不会跳过; 如果设置为true且未配置runtime.error.topic.name,则会丢弃错误记录的写入.

## 坏数据与错误上报（skip_error / runtime.error.topic.*）

一条记录写不进 MaxCompute 时（载荷解析失败、列类型不匹配、序列化超限……），连接器按配置走三条路之一。
默认值下走第一条，行为与之前一致：

| 配置组合 | 这条记录的去向 | 提交水位 |
| --- | --- | --- |
| `skip_error=false`（默认） | 异常抛给 Kafka Connect worker：task 失败或（配了 `errors.tolerance=all` 时）由平台的 DLQ 接管；位点不提交过这条，重启后重放 | 不越过这条 |
| `skip_error=true` + `runtime.error.topic.name` + `runtime.error.topic.bootstrap.servers` | 原始记录被投递到错误 topic；投递失败会打 ERROR 日志（带来源 topic/partition/offset） | 越过这条（记录已有去处） |
| `skip_error=true` 且未配置错误 topic | **丢弃**，并在任务日志里留下一条带 `topic-partition@offset` 的 ERROR | 越过这条（日志与计数都会说明） |

要点：

- 第三条路径以前是完全静默的：既没有日志也不计数，提交位点却已经前进，事后无法判断这段 offset 里有没有数据真正落盘。现在每条被丢弃的记录都会计入分区累计，并在 `preCommit` 的日志里与被提交的水位一起写出（`Total write ... skipped N records without an error destination` 出现在任务 flush/close 时）。仍建议要么配错误 topic，要么保持 `skip_error=false`。
- 两条 DLQ 只能选一条：Kafka Connect 平台的 `errors.tolerance` / `errors.deadletterqueue.topic.name` 依赖 task 把异常抛出 `put()`；`skip_error=true` 时本连接器不抛，平台侧收不到任何记录。想用平台 DLQ 就保持 `skip_error=false`。
- 错误 topic 里的载荷是文本化的原始 key/value：`String` 原样；`Struct`/`Map` 序列化成 JSON；`byte[]` 用 base64 并在 header `mc-connect-payload-encoding: base64` 标注；同时带 `mc-connect-error`（异常文本）、`mc-connect-origin-topic` / `-partition` / `-offset` 三个来源坐标 header，便于回溯是哪条记录被退回。
- 复用的记录载体在每条转换前会被清空：`format=JSON` 遇到 value 为 NULL 的墓碑记录、`format=FLATTEN` 遇到 JSON 里缺失的字段时，对应列写 NULL，而不是沿用上一条记录的值（修复前会静默多出一行带着别的记录内容的数据）；`format=FLATTEN` 遇到显式的 JSON `null` 字段值也写成 NULL，不再把整条记录变成 `NullPointerException`。
- 已知的输入边界（这些会报错并被上面三条策略之一处理，不会静默写入）：
  - `format=CSV`：单个字段超过 100,000 字符会被随包的 CSV 解析器拒绝（`Maximum column length of 100,000 exceeded`）；字段数与表列数不一致直接报错；`\N` 表示该列为 NULL。
  - `format=JSON`：载荷列需要是 MaxCompute 的 `JSON` 类型，写到 `STRING` 列会被类型校验拒绝；畸形 JSON 文本在写入载体时即被拒绝。
  - BOOLEAN 列走 `Boolean.valueOf`：除 `"true"`（忽略大小写）外的一切文本都变成 `false` 且不报错，`1`/`yes` 亦然。这是历史行为，本版本未改动，需要严格校验请在写入前用 sink 转换器（`errors.tolerance` 或上游 SMT）处理。
