package com.aliyun.odps.kafka.connect;

import java.util.concurrent.atomic.AtomicLong;

import com.aliyun.odps.kafka.connect.sink.BufferedWriter;
import org.apache.kafka.connect.sink.SinkRecord;

public class SinkStatusContext {
    private final BufferedWriter writer;
    private final AtomicLong processedBytes; // 当前分区写入mc的字节数
    private final AtomicLong processedRecords;

    public SinkStatusContext(BufferedWriter writer) {
        this.writer = writer;
        this.processedBytes = new AtomicLong(0);
        this.processedRecords = new AtomicLong(0);
    }

    public boolean putRecord(SinkRecord record) {
        return writer.write(record);
    }

    public long flush() {
        Status status = writer.flushAndReset();
        processedBytes.addAndGet(status.getProcessedBytes());
        processedRecords.addAndGet(status.getProcessedRecords());
        return status.getMaxOffset();
    }

    public long getProcessedBytes() {
        return processedBytes.get();
    }

    public long getProcessedRecords() {
        return processedRecords.get();
    }

    public static class Status {
        private long maxOffset;
        private long processedRecords = 0;
        private long processedBytes = 0;

        public Status(long maxOffset, long processedRecords, long processedBytes) {
            this.maxOffset = maxOffset;
            this.processedRecords = processedRecords;
            this.processedBytes = processedBytes;
        }

        public long getMaxOffset() {
            return maxOffset;
        }

        public long getProcessedRecords() {
            return processedRecords;
        }

        public long getProcessedBytes() {
            return processedBytes;
        }
    }
}
