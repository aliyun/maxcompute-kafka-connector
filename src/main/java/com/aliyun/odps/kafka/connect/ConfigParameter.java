package com.aliyun.odps.kafka.connect;

import java.util.Map;
import java.util.TimeZone;

import com.aliyun.odps.account.Account;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;

public enum ConfigParameter {
    MAXCOMPUTE_ENDPOINT("endpoint", HIGH, "MaxCompute endpoint"),

    MAXCOMPUTE_PROJECT("project", HIGH, "MaxCompute project"),

    MAXCOMPUTE_SCHEMA("schema", "", MEDIUM, "MaxCompute schema"),

    MAXCOMPUTE_TABLE("table", HIGH, "MaxCompute table"),

    TUNNEL_ENDPOINT("tunnel_endpoint", "", MEDIUM, "Tunnel endpoint"),

    ACCESS_ID("access_id", HIGH, "Alibaba Cloud access ID"),

    ACCESS_KEY("access_key", HIGH, "Alibaba Cloud access key"),

    ACCOUNT_ID("account_id", "", HIGH, "Account id for STS"),

    REGION_ID("region_id", "", HIGH, "Region id for STS"),

    STS_ENDPOINT("sts.endpoint", "sts.aliyuncs.com",
        HIGH, "Sts endpoint"),

    ROLE_NAME("role_name", "", HIGH, "Role name for STS"),

    ACCOUNT_TYPE("account_type", Account.AccountProvider.ALIYUN.toString(), HIGH,
        "Account type: STS Authorization (STS) or Primary Alibaba Cloud Account (ALIYUN)"),

    // 11 hour
    CLIENT_TIMEOUT_MS("client_timeout_ms", LONG,
        11 * 60 * 60 * 1000, MEDIUM, "STS token time out"),

    RUNTIME_ERROR_TOPIC_BOOTSTRAP_SERVERS("runtime.error.topic.bootstrap.servers", "", MEDIUM, "Bootstrap servers"),

    RUNTIME_ERROR_TOPIC_NAME("runtime.error.topic.name", "",
        MEDIUM, "Error topic name"),

    FORMAT("format", "TEXT", HIGH, "Input format, could be TEXT or CSV"),

    CSV_DELIMITER("csv_delimiter", ",", MEDIUM,
        "CSV delimiter. Use \\t for a tab character"),

    MODE("mode", "DEFAULT", HIGH, "Mode, could be default, key, or value"),

    PARTITION_WINDOW_TYPE("partition_window_type", "HOUR",
        HIGH, "Partition window type, could be DAY, HOUR"),

    USE_NEW_PARTITION_FORMAT("use_new_partition_format", BOOLEAN,
        Boolean.FALSE, HIGH, "use new partition format,if true then yyyy-MM-dd else MM-dd-yyyy"),

    TIME_ZONE("time_zone", TimeZone.getDefault().getID(), HIGH, "Timezone"),

    //USE_STREAM_TUNNEL("use_streaming", BOOLEAN, Boolean.FALSE, LOW, "use streaming tunnel instead of batch tunnel"),

    BUFFER_SIZE_KB("buffer_size_kb", INT, 64 * 1024, MEDIUM,
        "internal buffer size per odps partition in KB, default 64MB"),

    FAIL_RETRY_TIMES("retry_times", INT, 3, MEDIUM,
        "retry times on flush failure. default 3 times. if invalid value provided, will fallback to default value."),

    //POOL_SIZE("sink_pool_size", INT, Runtime.getRuntime().availableProcessors(), MEDIUM,
    //    "MaxCompute sink pool size"),

    //RECORD_BATCH_SIZE("record_batch_size", INT, 8000, MEDIUM, "max record size for single writer-thread"),

    SKIP_ERROR("skip_error", BOOLEAN, Boolean.FALSE, LOW, "the task policy when internal errors happen, SKIP or EXIT");

    private final String name;
    private final ConfigDef.Type type;
    private final Object defaultValue;
    private final Importance importance;
    private final String doc;

    ConfigParameter(String name, ConfigDef.Type type, Object defaultValue, Importance importance,
        String doc) {
        this.name = name;
        this.type = type;
        this.defaultValue = defaultValue;
        this.importance = importance;
        this.doc = doc;
    }

    ConfigParameter(String name, Object defaultValue, Importance importance, String doc) {
        this(name, ConfigDef.Type.STRING, defaultValue, importance, doc);
    }

    ConfigParameter(String name, Importance importance, String doc) {
        this(name, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, importance, doc);
    }

    public String getName() {
        return name;
    }

    public ConfigDef.Type getType() {
        return type;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public Importance getImportance() {
        return importance;
    }

    public String getDoc() {
        return doc;
    }

    public String getString(AbstractConfig config) {
        return config.getString(name);
    }

    public int getInt(AbstractConfig config) {
        return config.getInt(name);
    }

    public boolean getBoolean(AbstractConfig config) {
        return config.getBoolean(name);
    }

    public void put(Map<String, String> taskConfig, AbstractConfig config) {
        switch (type) {
            case INT:
                taskConfig.put(name, Integer.toString(config.getInt(name)));
                break;
            case LONG:
                taskConfig.put(name, Long.toString(config.getLong(name)));
                break;
            case BOOLEAN:
                // 注意,Boolean.toString()是小写形式
                taskConfig.put(name, config.getBoolean(name) ? "TRUE" : "FALSE");
                break;
            default:
                taskConfig.put(name, config.getString(name));
        }
    }
}
