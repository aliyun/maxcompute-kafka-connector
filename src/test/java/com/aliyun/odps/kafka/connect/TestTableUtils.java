package com.aliyun.odps.kafka.connect;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.kafka.connect.MaxComputeSinkConnectorConfig.BaseParameter;
import com.aliyun.odps.kafka.connect.utils.OdpsUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility class for creating test tables in MaxCompute
 */
public class TestTableUtils {

    public static List<Record> readTable(Map<String, String> config, String tableName) throws OdpsException,
            IOException {
        MaxComputeSinkConnectorConfig connectorConfig = new MaxComputeSinkConnectorConfig(config);
        Odps odps = OdpsUtils.getOdps(connectorConfig);

        RecordReader reader = odps.tables().get(tableName).read(100);
        List<Record> res = new ArrayList<>();
        Record read = reader.read();
        while (read != null) {
            res.add(read);
            read = reader.read();
        }
        reader.close();
        return res;
    }

    /**
     * Creates a table for CSV testing with appropriate schema
     *
     * @param config    The connector configuration
     * @param tableName The name of the table to create
     * @throws OdpsException if there's an error creating the table
     */
    public static void createCsvTestTable(Map<String, String> config, String tableName) throws OdpsException {
        MaxComputeSinkConnectorConfig connectorConfig = new MaxComputeSinkConnectorConfig(config);
        Odps odps = OdpsUtils.getOdps(connectorConfig);

        String project = connectorConfig.getString(BaseParameter.MAXCOMPUTE_PROJECT.getName());

        // Check if table already exists
        if (odps.tables().exists(project, tableName)) {
            // Drop existing table
            odps.tables().delete(project, tableName);
        }

        // Create schema for CSV test
        TableSchema schema = new TableSchema();
        // Add standard Kafka Connect metadata columns
        schema.addColumn(new Column("topic", com.aliyun.odps.OdpsType.STRING));
        schema.addColumn(new Column("partition", com.aliyun.odps.OdpsType.BIGINT));
        schema.addColumn(new Column("offset", com.aliyun.odps.OdpsType.BIGINT));
        schema.addColumn(new Column("insert_time", com.aliyun.odps.OdpsType.BIGINT));
        // Add user data columns for CSV test (7 columns based on test data)
        schema.addColumn(new Column("col1", com.aliyun.odps.OdpsType.STRING));    // 20230421
        schema.addColumn(new Column("col2", com.aliyun.odps.OdpsType.DOUBLE));    // 3.75
        schema.addColumn(new Column("col3", com.aliyun.odps.OdpsType.BOOLEAN));   // true
        schema.addColumn(new Column("col4", com.aliyun.odps.OdpsType.STRING));    // whq
        schema.addColumn(new Column("col5", com.aliyun.odps.OdpsType.TIMESTAMP)); // 2017-11-11 00:00:00.1234
        schema.addColumn(new Column("col6", com.aliyun.odps.OdpsType.DATE));      // 2023-11-11
        schema.addColumn(new Column("col7", com.aliyun.odps.OdpsType.DATETIME));  // 2021-11-26 00:04:00

        // Add partition column
        schema.addPartitionColumn(new Column("pt", com.aliyun.odps.OdpsType.STRING));

        // Create table
        odps.tables().newTableCreator(project, tableName, schema)
                .debug().create();
    }

    /**
     * Creates a table for JSON testing with appropriate schema
     *
     * @param config    The connector configuration
     * @param tableName The name of the table to create
     * @throws OdpsException if there's an error creating the table
     */
    public static void createJsonTestTable(Map<String, String> config, String tableName) throws OdpsException {
        MaxComputeSinkConnectorConfig connectorConfig = new MaxComputeSinkConnectorConfig(config);
        Odps odps = OdpsUtils.getOdps(connectorConfig);

        String project = connectorConfig.getString(BaseParameter.MAXCOMPUTE_PROJECT.getName());

        // Check if table already exists
        if (odps.tables().exists(project, tableName)) {
            // Drop existing table
            odps.tables().delete(project, tableName);
        }

        // Create schema for JSON test
        TableSchema schema = new TableSchema();
        // Add standard Kafka Connect metadata columns
        schema.addColumn(new Column("topic", com.aliyun.odps.OdpsType.STRING));
        schema.addColumn(new Column("partition", com.aliyun.odps.OdpsType.BIGINT));
        schema.addColumn(new Column("offset", com.aliyun.odps.OdpsType.BIGINT));
        schema.addColumn(new Column("insert_time", com.aliyun.odps.OdpsType.BIGINT));
        // Add user data columns for JSON test
        schema.addColumn(new Column("key", OdpsType.JSON));
        schema.addColumn(new Column("value", com.aliyun.odps.OdpsType.JSON));

        schema.addPartitionColumn(new Column("pt", com.aliyun.odps.OdpsType.STRING));
        // Create table
        odps.tables().newTableCreator(project, tableName, schema)
                .debug().create();
    }

    /**
     * Creates a table for FLATTEN testing with appropriate schema to match flatten.json
     *
     * @param config    The connector configuration
     * @param tableName The name of the table to create
     * @throws OdpsException if there's an error creating the table
     */
    public static void createFlattenTestTable(Map<String, String> config, String tableName) throws OdpsException {
        MaxComputeSinkConnectorConfig connectorConfig = new MaxComputeSinkConnectorConfig(config);
        Odps odps = OdpsUtils.getOdps(connectorConfig);

        String project = connectorConfig.getString(BaseParameter.MAXCOMPUTE_PROJECT.getName());

        // Check if table already exists
        if (odps.tables().exists(project, tableName)) {
            // Drop existing table
            odps.tables().delete(project, tableName);
        }

        // Create schema for FLATTEN test that matches flatten.json structure
        TableSchema schema = new TableSchema();
        // Add standard Kafka Connect metadata columns
        schema.addColumn(new Column("topic", com.aliyun.odps.OdpsType.STRING));
        schema.addColumn(new Column("partition", com.aliyun.odps.OdpsType.BIGINT));
        schema.addColumn(new Column("offset", com.aliyun.odps.OdpsType.BIGINT));
        schema.addColumn(new Column("insert_time", com.aliyun.odps.OdpsType.BIGINT));
        // Add user data columns for FLATTEN test that match flatten.json
        schema.addColumn(new Column("id", OdpsType.BIGINT));
        schema.addColumn(new Column("Name", com.aliyun.odps.OdpsType.STRING));

        schema.addColumn(new Column("Score", com.aliyun.odps.OdpsType.DOUBLE));         // 3.75
        schema.addColumn(new Column("is_fired", com.aliyun.odps.OdpsType.BOOLEAN));     // true
        schema.addColumn(new Column("TimeStamps", OdpsType.TIMESTAMP));    // "2017-11-11 00:00:00.1234"
        schema.addColumn(new Column("dt", OdpsType.DATE));            // "2023-11-11"
        schema.addColumn(new Column("dateTimes", OdpsType.DATETIME));
        schema.addColumn(new Column("extend", OdpsType.STRING));            // 20230421
        // "2021-11-26 00:04:00"

        // Add partition column
        schema.addPartitionColumn(new Column("pt", com.aliyun.odps.OdpsType.STRING));

        // Create table
        odps.tables().newTableCreator(project, tableName, schema)
                .debug().create();
    }

    /**
     * Drops a test table if it exists
     *
     * @param config    The connector configuration
     * @param tableName The name of the table to drop
     * @throws OdpsException if there's an error dropping the table
     */
    public static void dropTableIfExists(Map<String, String> config, String tableName) throws OdpsException {
        MaxComputeSinkConnectorConfig connectorConfig = new MaxComputeSinkConnectorConfig(config);
        Odps odps = OdpsUtils.getOdps(connectorConfig);

        String project = connectorConfig.getString(BaseParameter.MAXCOMPUTE_PROJECT.getName());

        // Check if table exists and drop it
        if (odps.tables().exists(project, tableName)) {
            odps.tables().delete(project, tableName);
        }
    }
}