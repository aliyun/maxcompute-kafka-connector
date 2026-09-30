package com.aliyun.odps.kafka.connect;

import java.util.concurrent.atomic.AtomicLong;

import com.aliyun.odps.kafka.connect.sink.BufferedWriter;
import org.apache.kafka.connect.sink.SinkRecord;

public class SinkStatusContext {
    private final BufferedWriter writer;
    private final AtomicLong processedBytes; // 当前分区写入mc的字节数
    private final AtomicLong processedRecords;
    /** 累计"没落盘也没去处"的记录条数，随每次 flush 结转，供提交位点处说明水位越过了什么。 */
    private final AtomicLong droppedRecords = new AtomicLong(0);
    /** 累计转换失败但已交给错误上报通道的记录条数。 */
    private final AtomicLong reportedRecords = new AtomicLong(0);

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
        droppedRecords.addAndGet(status.getDroppedRecords());
        reportedRecords.addAndGet(status.getReportedRecords());
        return status.getMaxOffset();
    }

    public long getProcessedBytes() {
        return processedBytes.get();
    }

    public long getProcessedRecords() {
        return processedRecords.get();
    }

    public long getDroppedRecords() {
        return droppedRecords.get();
    }

    public long getReportedRecords() {
        return reportedRecords.get();
    }

    public static class Status {
        private long maxOffset;
        private long processedRecords = 0;
        private long processedBytes = 0;
        private long droppedRecords = 0;
        private long reportedRecords = 0;

        public Status(long maxOffset, long processedRecords, long processedBytes) {
            this(maxOffset, processedRecords, processedBytes, 0, 0);
        }

        public Status(long maxOffset, long processedRecords, long processedBytes, long droppedRecords,
            long reportedRecords) {
            this.maxOffset = maxOffset;
            this.processedRecords = processedRecords;
            this.processedBytes = processedBytes;
            this.droppedRecords = droppedRecords;
            this.reportedRecords = reportedRecords;
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

        public long getDroppedRecords() {
            return droppedRecords;
        }

        public long getReportedRecords() {
            return reportedRecords;
        }
    }
}
