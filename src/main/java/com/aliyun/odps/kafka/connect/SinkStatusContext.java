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

    /**
     * 为"释放或继续持有该分区状态"这一步把缓冲落盘。
     *
     * <p>没有未持久化数据时直接返回（不去碰 tunnel，也不做无谓的网络 flush）；
     * 有数据且落盘成功时返回；落盘失败则把异常原样上抛——调用方不能把这座 writer 留在内存里继续用：
     * tunnel 的 stream pack 一旦 flush 失败就拒绝再 append（接口自带的报错原话是"There's an unsuccessful
     * flush called..."），留着它等于让这个分区永久卡死。抛出去由 worker 杀掉 task，位点没提交，
     * 记录会重放。
     */
    public void flushForRelease() {
        if (!writer.hasPendingData()) {
            return;
        }
        flush();
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
