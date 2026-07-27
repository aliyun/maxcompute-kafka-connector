/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

package com.aliyun.odps.kafka.connect.converter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

import org.apache.kafka.connect.sink.SinkRecord;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.kafka.connect.utils.ConverterHelper;
import com.csvreader.CsvReader;

/**
 * Convert a {@link SinkRecord} to a {@link Record} with the following schema:
 * <p>
 * Columns:
 * TOPIC STRING, PARTITION BIGINT, OFFSET BIGINT, [Columns described in schema]
 * <p>
 * Partitioned columns:
 * PT STRING
 */
public class CsvRecordConverter implements RecordConverter {

    private static String NULL_TOKEN = "\\N";

    private final RecordConverterBuilder.Mode mode;
    private final char delimiter;
    private List<Integer> userColIndex = new LinkedList<>();

    public CsvRecordConverter(TableSchema schema, RecordConverterBuilder.Mode mode) {
        this(schema, mode, ",");
    }

    public CsvRecordConverter(TableSchema schema, RecordConverterBuilder.Mode mode, String delimiter) {
        this.mode = mode;
        this.delimiter = parseDelimiter(delimiter);

        // Init userColIndex
        for (int i = 0; i < schema.getColumns().size(); i++) {
            Column c = schema.getColumn(i);
            if (!TOPIC.equalsIgnoreCase(c.getName())
                && !PARTITION.equalsIgnoreCase(c.getName())
                && !OFFSET.equalsIgnoreCase(c.getName())
                && !INSERT_TIME.equalsIgnoreCase(c.getName())) {
                userColIndex.add(i);
            }
        }
    }

    @Override
    public void convert(SinkRecord in, Record out) throws IOException {
        out.setString(TOPIC, in.topic());
        out.setBigint(PARTITION, in.kafkaPartition().longValue());
        out.setBigint(OFFSET, in.kafkaOffset());
        out.setBigint(INSERT_TIME, in.timestamp());

        String data;
        if (RecordConverterBuilder.Mode.KEY.equals(mode)) {
            data = (String)in.key();
        } else if (RecordConverterBuilder.Mode.VALUE.equals(mode)) {
            data = (String)in.value();
        } else {
            throw new RuntimeException("Unsupported mode for CsvConverter: " + mode);
        }

        String[] row = load(data, delimiter);
        if (out.getColumnCount() - 4 != row.length) {
            throw new RuntimeException("Column count doesn't match: " + data);
        }

        for (int i = 0; i < row.length; ++i) {
            try {
                // Can be cast to an array record. See TableTunnel.UploadSession.newRecord().
                ConverterHelper.setRecordByType((ArrayRecord)out, userColIndex.get(i), row[i]);
            } catch (Exception e) {
                throw new IOException("Parse Error while trans value", e);
            }
        }
    }

    static char parseDelimiter(String delimiter) {
        if ("\\t".equals(delimiter)) {
            return '\t';
        }
        if (delimiter == null || delimiter.length() != 1) {
            throw new IllegalArgumentException(
                "csv_delimiter must be exactly one character; use \\t for a tab character");
        }
        return delimiter.charAt(0);
    }

    static String[] load(String data, char delimiter) throws IOException {
        InputStream is = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
        CsvReader csvReader = new CsvReader(new InputStreamReader(is, StandardCharsets.UTF_8), delimiter);

        String[] row;
        if (csvReader.readRecord()) {
            row = csvReader.getValues();

            for (int i = 0; i < row.length; ++i) {
                if (row[i].equals(NULL_TOKEN)) {
                    row[i] = null;
                } else if (trimStringQuotes(row[i]).equals(NULL_TOKEN)) {
                    // TODO: bug, could have multi quotes
                    row[i] = row[i].substring(1, row[i].length() - 1);
                }
            }

            return row;
        } else {
            throw new RuntimeException("Data cannot be parsed or is empty: " + data);
        }
    }

    private static String trimStringQuotes(String str) {
        int i = 0;
        for (int len = str.length();
             i < len / 2 && str.charAt(i) == '"' && str.charAt(len - i - 1) == '"'; ++i) {
        }
        return str.substring(i, str.length() - i);
    }
}
