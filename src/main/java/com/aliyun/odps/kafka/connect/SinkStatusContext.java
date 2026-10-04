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

    /** 最近一次 flush 时仍未确认投递的记录条数（不累计：它描述的是"现在"挡着几条）。 */
    private volatile long unconfirmedReportedRecords = 0;
    /** 最近一次 flush 时水位被迫停在哪条记录之前；-1 表示没有被挡住。 */
    private volatile long heldAtOffset = -1;

    public long flush() {
        Status status = writer.flushAndReset();
        processedBytes.addAndGet(status.getProcessedBytes());
        processedRecords.addAndGet(status.getProcessedRecords());
        droppedRecords.addAndGet(status.getDroppedRecords());
        reportedRecords.addAndGet(status.getReportedRecords());
        unconfirmedReportedRecords = status.getUnconfirmedReportedRecords();
        heldAtOffset = status.getHeldAtOffset();
        return status.getMaxOffset();
    }

    public long getUnconfirmedReportedRecords() {
        return unconfirmedReportedRecords;
    }

    public long getHeldAtOffset() {
        return heldAtOffset;
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
        /** 交给了上报通道但还没有投递凭据的记录条数；水位已按它停在更早的位置。 */
        private long unconfirmedReportedRecords = 0;
        /** 水位被挡在哪条记录之前，-1 表示没挡。 */
        private long heldAtOffset = -1;

        public Status(long maxOffset, long processedRecords, long processedBytes) {
            this(maxOffset, processedRecords, processedBytes, 0, 0, 0, -1);
        }

        public Status(long maxOffset, long processedRecords, long processedBytes, long droppedRecords,
            long reportedRecords) {
            this(maxOffset, processedRecords, processedBytes, droppedRecords, reportedRecords, 0, -1);
        }

        public Status(long maxOffset, long processedRecords, long processedBytes, long droppedRecords,
            long reportedRecords, long unconfirmedReportedRecords, long heldAtOffset) {
            this.maxOffset = maxOffset;
            this.processedRecords = processedRecords;
            this.processedBytes = processedBytes;
            this.droppedRecords = droppedRecords;
            this.reportedRecords = reportedRecords;
            this.unconfirmedReportedRecords = unconfirmedReportedRecords;
            this.heldAtOffset = heldAtOffset;
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

        public long getUnconfirmedReportedRecords() {
            return unconfirmedReportedRecords;
        }

        public long getHeldAtOffset() {
            return heldAtOffset;
        }
    }
}
