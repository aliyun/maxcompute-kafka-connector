# Welcome to MaxCompute Kafka Connector!

## Configuration example
````$xslt
{
    "name": "your_name",
    "config": {
	"connector.class": "com.aliyun.odps.kafka.connect.MaxComputeSinkConnector",
	"tasks.max": "3",
	"topics": "your_topic",
	"endpoint": "endpoint",
	"tunnel_endpoint": "your_tunnel endpoint",
	"project": "project",
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
	"mode": "KEY",
	"partition_window_type": "MINUTE",
	"use_new_partition_format":true,
	"use_streaming": false,
	"buffer_size_kb": 65536
	"sink_pool_size":"16",
	"record_batch_size":"8000",
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
- table：要写入的MaxCompute表。
- account_type：MaxCompute鉴权方式，选项为STS或ALIYUN，默认ALIYUN。
- access_id和access_key：若account_type为ALIYUN，则这两项配置为用户的access_id和access_key。否则保持为空即可，但不能不配置这两项。
- account_id、region_id、role_name和client_timeout_ms：生成STS Token所需信息。若account_type为STS，则如实配置。否则可以不配置。
- client_timeout_ms：刷新STS Token的时间间隔，单位为毫秒，默认值为11小时对应的毫秒数。
- sts.endpoint：可选配置，保持默认即可。
- format：消息的格式，详细解释见官方文档，可选值为TEXT、BINARY与CSV，默认TEXT。
- mode：此connector的处理模式，详细解释见官方文档，可选值为：KEY，VALUE，DEFAULT，默认DEFAULT。
- partition_window_type：如何按照系统时间进行数据分区。例如，若配置为MINUTE，则每分钟开始时数据写到一个新的分区。可选值DAY、HOUR、MINUTE，默认HOUR。
- use_new_partition_format:是否启用新的partitiont value 格式，true代表使用yyyy-MM-dd,否则使用MM-dd-yyyy
- use_streaming: 是否使用流式数据通道。默认 false ，使用流式数据通道可提升性能。
- buffer_size_kb: 每个 odps partition writer 内部缓冲区大小，单位 KB。默认 65536 （64MB）
- sink_pool_size: 设置多线程写入的最大线程数，默认为系统CPU核数。此参数不同于task.max，task.max参数指定的task任务数可以理解成Kafka消费者任务数，一个分区同一时间只能分配给一个消费者；而该sink_pool_size参数可以设置单分区的消费能力，一个task消费者内部配备的了多线程写入MC的能力,并且能够保证commit的正确性，在单分区数据量较大或者无法更改现有分区数的情况下，可以有效提升性能
- record_batch_size: 和sink_pool_size参数配套使用，设置一个task内部的一个线程最多可以一次并行发送多少消息
- runtime.error.topic.name: 当connect内部写入某条数据发生未知错误时, 将错误记录写入Kafka消息队列中.默认为空
- runtime.error.topic.bootstrap.servers: 与runtime.error.topic.name搭配使用, 错误消息写入Kafka的bootstrap servers地址
- skip_error: 是否跳过发生未知写入错误的记录, 默认false不会跳过; 如果设置为true且未配置runtime.error.topic.name,则会丢弃错误记录的写入.
