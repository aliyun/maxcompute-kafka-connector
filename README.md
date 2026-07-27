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
# target/kafka-connector-odps-2.1.0.jar
# target/kafka-connector-odps-2.1.0-jar-with-dependencies.jar
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
cp target/kafka-connector-odps-2.1.0-jar-with-dependencies.jar $KAFKA_HOME/plugins/maxcompute-connector/

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

### 7. 运行集成测试

如果需要运行项目的集成测试，需要先配置环境变量：

```bash
# 设置环境变量
export ALIBABA_CLOUD_ACCESS_KEY_ID="your_access_id"
export ALIBABA_CLOUD_ACCESS_KEY_SECRET="your_access_key"
export odps_endpoint="your_odps_endpoint"
export MAXCOMPUTE_PROJECT="your_project_name"

# 运行集成测试
mvn test -Dtest=TestMaxComputeSinkConnectorIntegration
```

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
