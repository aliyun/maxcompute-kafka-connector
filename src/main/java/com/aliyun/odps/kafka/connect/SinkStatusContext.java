package com.aliyun.odps.kafka.connect;

import java.util.concurrent.atomic.AtomicLong;

import com.aliyun.odps.kafka.connect.sink.BufferedWriter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkStatusContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(SinkStatusContext.class);

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

    /**
     * 尝试把缓冲落盘，以便安全释放该分区的写入状态。
     *
     * @return true 表示已经没有未持久化的记录（状态可以丢弃）；false 表示落盘失败、缓冲区仍完整保留，
     *         调用方要么继续持有该状态、交给下一次 preCommit 重试，要么确认这些记录会被重新投递。
     */
    public boolean tryFlushForRelease() {
        if (!writer.hasPendingData()) {
            return true;
        }
        try {
            flush();
            return true;
        } catch (RuntimeException e) {
            LOGGER.warn("Thread({}) failed to flush buffered records while releasing partition state",
                Thread.currentThread().getId(), e);
            return false;
        }
    }

    public boolean hasPendingData() {
        return writer.hasPendingData();
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
