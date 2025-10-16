package com.aliyun.odps.kafka.connect.converter;

import java.text.ParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.kafka.connect.sink.SinkRecord;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.kafka.connect.utils.ConverterHelper;
import com.aliyun.odps.kafka.connect.utils.JsonHandler;
import com.fasterxml.jackson.core.JsonProcessingException;

public class FlattenRecordConverter implements RecordConverter {

    private final RecordConverterBuilder.Mode mode;
    private final Set<String> colNames = new HashSet<>();
    private final Map<String, Integer> lookID = new HashMap<>();

    public FlattenRecordConverter(RecordConverterBuilder.Mode mode, TableSchema schema) {
        this.mode = mode;
        for (int i = 0; i < schema.getColumns().size(); i++) {
            String cur = schema.getColumn(i).getName().toLowerCase();
            colNames.add(cur);
            lookID.put(cur, i);
        }
        String[] fixedColumns = {TOPIC, PARTITION, OFFSET, INSERT_TIME};
        for (String column : fixedColumns) {
            checkColumnExist(column);
        }
    }

    @Override
    public void convert(SinkRecord in, Record out) throws JsonProcessingException {
        out.setString(TOPIC, in.topic());
        out.setBigint(PARTITION, in.kafkaPartition().longValue());
        out.setBigint(OFFSET, in.kafkaOffset());
        out.setBigint(INSERT_TIME, in.timestamp());

        Map<String, Object> flattenRecord = new HashMap<>();
        switch (mode) {
            case KEY:
                if (in.key() != null) {
                    flattenRecord = flattenFieldFromJson(in.key());
                }
                break;
            case VALUE:
                if (in.value() != null) {
                    flattenRecord = flattenFieldFromJson(in.value());
                }
                break;
            case DEFAULT:
            default:
                throw new RuntimeException("Unsupported mode for FlattenConverter:" + mode);
        }

        for (Entry<String, Object> entry : flattenRecord.entrySet()) {
            String key = entry.getKey().toLowerCase();
            checkColumnExist(key);

            Object value = entry.getValue();
            try {
                String
                  strValue =
                  value instanceof Map ? JsonHandler.getJsonString(value) : value.toString();
                ConverterHelper.setRecordByType((ArrayRecord) out, lookID.get(key), strValue);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void checkColumnExist(String column) {
        if (!colNames.contains(column)) {
            throw new RuntimeException("there is no column name in sinkRecord: " + column);
        }
    }

    private Map<String, Object> flattenFieldFromJson(Object sinkRecord)
      throws JsonProcessingException {
        // flatten the json field to mc table schema without embedded format
        if (sinkRecord instanceof HashMap) {
            return (HashMap<String, Object>) sinkRecord;
        }
        if (sinkRecord instanceof String) {
            return JsonHandler.json2Map((String) sinkRecord);
        }
        throw new RuntimeException(
          "unsupported sinkRecord type please check your data format again!");
    }
}
