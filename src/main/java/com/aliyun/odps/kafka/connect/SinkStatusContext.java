package com.aliyun.odps.kafka.connect;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.connect.sink.SinkRecord;

public class SinkStatusContext {

  private final int batchSize;
  private List<SinkRecord> recordQueue;// 当前分区拉取到的记录数
  private final ConcurrentSkipListMap<Long, Long> offsetRangeMap;// 记录多线程写入的offset区间值集合,需要保证线程安全

  private final AtomicLong totalBytesSentByWriter; // 当前分区写入mc的字节数
  private final AtomicLong processedRecords;

  public SinkStatusContext(int batchSize) {
    this.batchSize = batchSize;
    this.recordQueue = new ArrayList<>(batchSize);
    this.offsetRangeMap = new ConcurrentSkipListMap<>();
    this.totalBytesSentByWriter = new AtomicLong(0);
    this.processedRecords = new AtomicLong(0);
  }

  public void addOffsetRange(long left, long right) {
    offsetRangeMap.put(left, right);
  }

  public void addTotalBytesSentByWriter(Long bytes) {
    totalBytesSentByWriter.addAndGet(bytes);
  }

  public Long getTotalBytesSentByWriter() {
    return totalBytesSentByWriter.get();
  }

  public boolean containsOffset(long offset) {
    // 判断offset区间是否包含某个record的offset
    Long floorKey = offsetRangeMap.floorKey(offset);
    return (floorKey != null && offset >= floorKey && offset <= offsetRangeMap.get(floorKey));
  }

  public List<SinkRecord> getRecordQueue() {
    return recordQueue;
  }

  public void resetRecordQueue() {
    this.recordQueue = new ArrayList<>(batchSize);
  }

  public long mergeOffset() {
    if (offsetRangeMap.isEmpty()) {
      return -1;
    }
    // 针对offset的区间更新 consumedOffset
    Entry<Long, Long> firstEntry = offsetRangeMap.pollFirstEntry();
    long consumedOffset = firstEntry.getValue();
    while (!offsetRangeMap.isEmpty()) {
      firstEntry = offsetRangeMap.firstEntry();
      // if pre_tail equals cur_head
      if (consumedOffset + 1 == firstEntry.getKey()) {
        consumedOffset = firstEntry.getValue();
        offsetRangeMap.pollFirstEntry();
      } else {
        break;
      }
    }
    return consumedOffset;
    }

  public boolean isEmpty() {
    return offsetRangeMap.isEmpty();
  }

  public void addProcessedRecords(int records) {
    this.processedRecords.addAndGet(records);
  }

  public long getProcessedRecords() {
    return processedRecords.get();
  }

  public ConcurrentSkipListMap<Long, Long> getOffsetRangeMap() {
    return offsetRangeMap;
  }
}
