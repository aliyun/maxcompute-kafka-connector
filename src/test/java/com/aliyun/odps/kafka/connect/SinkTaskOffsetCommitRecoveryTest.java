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

package com.aliyun.odps.kafka.connect;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.kafka.connect.converter.RecordConverter;
import com.aliyun.odps.kafka.connect.sink.BufferedWriter;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Assert;
import org.junit.Test;

/**
 * flush / offset 提交 / 重平衡故障矩阵：不读云凭据、不启动 broker，属于 PR 门禁的 unit 档。
 *
 * <p>{@link SinkTaskImpl} 被放进一个最小的 worker 协议模拟器（本文件的 {@link FakeWorker}）里驱动。
 * 该协议按 apache/kafka 3.4.0
 * {@code connect/runtime/src/main/java/org/apache/kafka/connect/runtime/WorkerSinkTask.java} 建模，
 * 三条关键事实（行号取自该 tag 的该文件）：
 * <ul>
 *   <li>提交前回调 {@code task.preCommit(offsets)}，只有它返回的 offset 才会被提交；它抛异常时整次提交作废，
 *       非 closing 分支才把位点 rewind 回 last committed（{@code WorkerSinkTask.java:389-420}）；</li>
 *   <li>分区被 revoke 时先 {@code commitOffsets(..., closing=true, ...)}（即先落盘提交），其 finally 再回调
 *       {@code task.close(partitions)}（{@code WorkerSinkTask.java:389-425, 643-665}）；
 *       分区是 lost 时只回调 {@code task.close(partitions)}，不提交（{@code WorkerSinkTask.java:752-779}）；
 *       两种情况 worker 都会把这些分区的位点重置回已提交位置，记录一定会重放；</li>
 *   <li>{@code onPartitionsAssigned} 之后回调 {@code task.open(partitions)}
 *       （{@code WorkerSinkTask.java:638-641, 696-744}），传入的是本次新增的分区：cooperative 重平衡下
 *       仍留在本 task 上的分区不会被 revoke，它们的消费位点也不会回退。</li>
 * </ul>
 *
 * <p>每条用例跑同一组不变式：
 * <ol>
 *   <li><b>安全性</b>：任一分区的已提交水位之下不允许存在从未落盘的 offset；</li>
 *   <li><b>不丢数</b>：位点已前进、且不会被重放的记录必须已落盘；</li>
 *   <li><b>重复范围</b>：同一 offset 多次落盘只能出现在"已落盘但未提交、随后重放"的路径上。</li>
 * </ol>
 */
public class SinkTaskOffsetCommitRecoveryTest {

  private static final String TOPIC = "mc_sink_topic";

  // ------------------------------------------------------------------ 用例

  /** 上传前失败（建 session 抛错）：不推进任何水位；恢复后从已提交位点重放，不产生重复。 */
  @Test
  public void preUploadFailureDoesNotAdvanceCommittedWatermark() {
    FakeSink sink = new FakeSink();
    FakeWorker worker = new WorkerBuilder(sink).withTaskPartitions(0).build();

    worker.failNextWriteOn(0);
    expectThrows(RuntimeException.class, () -> worker.deliver(0, 0, 5));

    worker.expectTaskFailureAndRestart();
    worker.clearFaults();
    worker.deliver(0, 0, 5);
    Assert.assertTrue(worker.commit());

    Assert.assertEquals(5L, worker.committed(0));
    sink.assertInvariants(worker);
    sink.assertDuplicates(0, 0);
  }

  /** 数据已落盘、offset 没提交成功：语义是 at-least-once，重复上界就是这一批。 */
  @Test
  public void offsetNotCommittedAfterPersistDuplicatesOneBatchOnly() {
    FakeSink sink = new FakeSink();
    FakeWorker worker = new WorkerBuilder(sink).withTaskPartitions(0).build();

    worker.deliver(0, 0, 5);
    Assert.assertTrue(worker.commit());
    Assert.assertEquals(5L, worker.committed(0));

    // 下一批落盘成功，但 offset 提交在 broker 侧失败（或进程在提交前退出）
    worker.deliver(0, 5, 5);
    Assert.assertFalse(worker.commitFailingAtBroker());
    Assert.assertEquals("提交失败不得推进水位", 5L, worker.committed(0));

    worker.expectTaskFailureAndRestart();
    worker.deliver(0, 5, 5);
    Assert.assertTrue(worker.commit());

    Assert.assertEquals(10L, worker.committed(0));
    sink.assertInvariants(worker);
    // offsets 5..9 落了两次：这就是本项承诺的重复上界——最后一个成功提交之后已落盘未提交的数据
    sink.assertDuplicates(0, 5);
  }

  /** flush 报错但服务端其实已收下数据：与上一条同界，不新增未证实的 exactly-once 承诺。 */
  @Test
  public void acknowledgedButFailedFlushKeepsWatermarkBehindPersistedData() {
    FakeSink sink = new FakeSink();
    FakeWorker worker = new WorkerBuilder(sink).withTaskPartitions(0).build();

    worker.deliver(0, 0, 3);
    Assert.assertTrue(worker.commit());

    worker.deliver(0, 3, 4);
    worker.failFlushAfterPersistingOn(0);
    Assert.assertFalse("flush 抛错时整次提交作废", worker.commit());
    Assert.assertEquals(3L, worker.committed(0));

    worker.expectTaskFailureAndRestart();
    worker.clearFaults();
    worker.deliver(0, 3, 4);
    Assert.assertTrue(worker.commit());

    Assert.assertEquals(7L, worker.committed(0));
    sink.assertInvariants(worker);
    sink.assertDuplicates(0, 4);
  }

  /** 多分区里有一个 flush 失败：preCommit 抛出后本次提交对所有分区作废，成功分区的落盘也不提交。 */
  @Test
  public void partialFlushFailureCommitsNothingForAnyPartition() {
    FakeSink sink = new FakeSink();
    FakeWorker worker = new WorkerBuilder(sink).withTaskPartitions(0, 1).build();

    worker.deliver(0, 0, 4);
    worker.deliver(1, 0, 6);
    worker.failNextFlushOn(1);

    Assert.assertFalse(worker.commit());
    Assert.assertEquals("失败的分区不提交", 0L, worker.committed(0));
    Assert.assertEquals("同批其他分区也不提交", 0L, worker.committed(1));

    // 重放：P0 那批已经落盘过一次，会重复；P1 从未落盘，不重复
    worker.expectTaskFailureAndRestart();
    worker.clearFaults();
    worker.deliver(0, 0, 4);
    worker.deliver(1, 0, 6);
    Assert.assertTrue(worker.commit());

    Assert.assertEquals(4L, worker.committed(0));
    Assert.assertEquals(6L, worker.committed(1));
    sink.assertInvariants(worker);
    sink.assertDuplicates(0, 4);
    sink.assertDuplicates(1, 0);
  }

  /** 一次成功的重平衡（分区被 revoke 再分配回来）：close 前先落盘并提交，重放不产生重复也不丢数。 */
  @Test
  public void gracefulRebalanceFlushesAndCommitsBeforeClose() {
    FakeSink sink = new FakeSink();
    FakeWorker worker = new WorkerBuilder(sink).withTaskPartitions(0).build();

    worker.deliver(0, 0, 5);
    worker.gracefulRebalanceReassigning(0);

    Assert.assertEquals("revoke 时的 closing commit 已把缓冲落盘并提交", 5L, worker.committed(0));
    sink.assertInvariants(worker);
    sink.assertDuplicates(0, 0);

    worker.deliver(0, 5, 5);
    Assert.assertTrue(worker.commit());
    Assert.assertEquals(10L, worker.committed(0));
    sink.assertInvariants(worker);
    sink.assertDuplicates(0, 0);
  }

  /**
   * 本项核心：cooperative 重平衡只把新增分区交给 {@code open()}，留在本 task 上的分区位点不回退。
   * 若 {@code open()} 直接清空全部分区状态，留下分区缓冲里"已消费、未落盘、也不会再重放"的记录就永久丢了，
   * 而后续提交水位会继续越过它们。
   */
  @Test
  public void cooperativeRebalanceMustNotDropBufferedRecordsOfRetainedPartitions() {
    FakeSink sink = new FakeSink();
    FakeWorker worker = new WorkerBuilder(sink).withTaskPartitions(0).build();

    worker.deliver(0, 0, 5);                // P0 缓冲里有 offsets 0..4，尚未提交

    worker.gainPartitionCooperatively(1);   // 重平衡：只新增 P1，P0 留在本 task
    worker.deliver(1, 0, 3);
    worker.deliver(0, 5, 5);                // P0 继续消费 offsets 5..9

    Assert.assertTrue(worker.commit());
    Assert.assertEquals(10L, worker.committed(0));
    Assert.assertEquals(3L, worker.committed(1));

    // P0 的 0..4 已被消费且不会再重放：要么落盘，要么就是静默丢数
    sink.assertInvariants(worker);
    sink.assertDuplicates(0, 0);
    sink.assertDuplicates(1, 0);
  }

  /**
   * cooperative 重平衡时，保留分区的落盘失败必须原样抛出（让 worker 杀掉 task）：位点还没提交，
   * 记录会重放；而 flush 失败过的 stream pack 之后拒绝继续 append，吞掉异常继续持有这座 writer
   * 会让该分区永久卡死，最终连"提交水位不越过持久数据"都保不住。
   */
  @Test
  public void failedFlushAtRebalanceFailsTaskInsteadOfKeepingUnusableWriter() {
    FakeSink sink = new FakeSink();
    FakeWorker worker = new WorkerBuilder(sink).withTaskPartitions(0).build();

    worker.deliver(0, 0, 5);
    worker.failNextFlushOn(0);
    expectThrows(RuntimeException.class, () -> worker.gainPartitionCooperatively(1));

    Assert.assertEquals("抛出前不得推进任何水位", 0L, worker.committed(0));
    Assert.assertEquals("这批记录没有落盘", 0, sink.writesOf(0));

    // task 重启后从已提交位点重放，故障已恢复：数据完整落盘且不重复
    worker.expectTaskFailureAndRestart();
    worker.clearFaults();
    worker.deliver(0, 0, 5);
    Assert.assertTrue(worker.commit());
    Assert.assertEquals(5L, worker.committed(0));
    sink.assertInvariants(worker);
    sink.assertDuplicates(0, 0);
  }

  /**
   * 分区 lost（无法提交）时只需释放状态：记录会由新持有者从已提交位点重放，此处再落盘只会制造重复。
   */
  @Test
  public void lostPartitionsAreReleasedWithoutExtraWrites() {
    FakeSink sink = new FakeSink();
    FakeWorker worker = new WorkerBuilder(sink).withTaskPartitions(0, 1).build();

    worker.deliver(0, 0, 4);
    worker.deliver(1, 0, 2);
    worker.losePartition(1);                // onPartitionsLost：不提交，只 close([P1])

    Assert.assertEquals(0L, worker.committed(1));

    // 之后一次增量 assign：本 task 拿到 P2
    worker.gainPartitionCooperatively(2);
    worker.deliver(2, 0, 2);
    Assert.assertTrue(worker.commit());

    Assert.assertEquals(4L, worker.committed(0));
    Assert.assertEquals(2L, worker.committed(2));
    sink.assertInvariants(worker);
    Assert.assertEquals("已丢失分区的缓冲区不应被写进 MaxCompute", 0, sink.writesOf(1));
    sink.assertDuplicates(0, 0);
    sink.assertDuplicates(2, 0);
  }

  /** preCommit 对本 task 里没有状态的分区不返回 offset——不返回就等于不提交。 */
  @Test
  public void preCommitSkipsPartitionsWithoutState() {
    FakeSink sink = new FakeSink();
    FakeWorker worker = new WorkerBuilder(sink).withTaskPartitions(0, 1).build();

    worker.deliver(0, 0, 4);                // 只给 P0 建立状态，P1 一条都没消费

    Map<TopicPartition, OffsetAndMetadata> returned = worker.preCommitOnly();
    Assert.assertTrue(returned.containsKey(new TopicPartition(TOPIC, 0)));
    Assert.assertFalse("没有状态的分区不得出现在提交集合里",
                       returned.containsKey(new TopicPartition(TOPIC, 1)));
    Assert.assertEquals(0L, worker.committed(1));
    Assert.assertEquals(0, sink.writesOf(1));
    sink.assertInvariants(worker);
  }

  /** 缓冲达到阈值时通过 context.requestCommit() 提前请求提交（是否提交仍由 worker 决定）。 */
  @Test
  public void bufferLimitRequestsCommit() {
    FakeSink sink = new FakeSink();
    FakeWorker worker = new WorkerBuilder(sink).withTaskPartitions(0).withRecordsPerCommitTrigger(3).build();

    worker.deliver(0, 0, 2);
    Assert.assertEquals(0, worker.context().requestCommitCalls);
    worker.deliver(0, 2, 1);
    Assert.assertEquals("达到缓冲阈值应请求提交一次", 1, worker.context().requestCommitCalls);
  }

  /**
   * 评审点名的路径在 task 层的等价断言：一条转投错误 topic 的记录在提交前没拿到投递凭据时，
   * {@code preCommit} 提交的水位必须停在它之前。代价是它之上已经落盘的记录会被重写一次 ——
   * 本连接器本来就是 at-least-once，这个重复有界且可查；而未确认的记录被提交过去就是永久丢数。
   */
  @Test
  public void unconfirmedErrorDeliveryHoldsTheCommittedOffsetAndReplaysFromThere() {
    FakeSink sink = new FakeSink();
    FakeWorker worker = new WorkerBuilder(sink).withTaskPartitions(0).build();

    worker.deliver(0, 0, 5);              // offset 0..4
    worker.holdErrorReportFrom(0, 3L);    // 其中 3 号转投错误 topic，投递未确认

    Assert.assertTrue(worker.commit());
    Assert.assertEquals("水位必须停在未确认的记录之前", 3L, worker.committed(0));

    worker.clearFaults();                 // 补投并确认
    worker.expectTaskFailureAndRestart(); // 重放从已提交位点开始
    worker.deliver(0, 3, 2);              // 3、4 再来一遍
    Assert.assertTrue(worker.commit());
    Assert.assertEquals(5L, worker.committed(0));

    sink.assertInvariants(worker);
    sink.assertDuplicates(0, 2);          // 挡住一条记录的代价是它上面两条重复落盘，不是丢数据
  }

  // ------------------------------------------------------ 模拟器与替身

  private static void expectThrows(Class<? extends Throwable> expected, Runnable body) {
    try {
      body.run();
    } catch (Throwable t) {
      Assert.assertTrue("异常类型不符: " + t, expected.isInstance(t));
      return;
    }
    Assert.fail("期望抛出 " + expected.getSimpleName() + "，实际没有抛异常");
  }

  /** 单个分区的写入行为开关。 */
  private static final class Behavior {
    boolean failNextWrite;
    boolean failNextFlush;
    boolean failFlushAfterPersist;
    /** <=0 表示不触发；否则缓冲达到该条数时 write() 返回 true（模拟 buffer 超阈值）。 */
    int recordsPerCommitTrigger;
    /** >=0 表示这条记录投给错误 topic 后还没有拿到投递凭据，水位必须停在它之前；-1 表示没有。 */
    long holdFromOffset = -1;
  }

  /** 假 MaxCompute：只记录每个 offset 被落盘了几次，用于校验水位、丢失与重复。 */
  private static final class FakeSink {
    final Map<Integer, List<Long>> written = new LinkedHashMap<Integer, List<Long>>();

    synchronized void persist(TopicPartition tp, Collection<Long> offsets) {
      List<Long> log = written.get(tp.partition());
      if (log == null) {
        log = new ArrayList<Long>();
        written.put(tp.partition(), log);
      }
      log.addAll(offsets);
    }

    synchronized int writesOf(int partition) {
      List<Long> log = written.get(partition);
      return log == null ? 0 : log.size();
    }

    synchronized boolean wasWritten(TopicPartition tp, long offset) {
      List<Long> log = written.get(tp.partition());
      return log != null && log.contains(Long.valueOf(offset));
    }

    synchronized int distinctWrites(TopicPartition tp) {
      List<Long> log = written.get(tp.partition());
      return log == null ? 0 : new HashSet<Long>(log).size();
    }

    /** 重复落盘条数 = 落盘次数 - 去重条数。 */
    void assertDuplicates(int partition, int expected) {
      TopicPartition tp = new TopicPartition(TOPIC, partition);
      int duplicated = writesOf(partition) - distinctWrites(tp);
      Assert.assertEquals("分区 " + partition + " 的重复落盘条数", expected, duplicated);
    }

    /** 本项的不变式集合。 */
    void assertInvariants(FakeWorker worker) {
      for (Integer partition : worker.ownedPartitions()) {
        TopicPartition tp = new TopicPartition(TOPIC, partition);
        long committed = worker.committed(partition);
        for (long offset = 0; offset < committed; offset++) {
          Assert.assertTrue(
              "提交水位越过未持久化数据：P" + partition + " offset " + offset
                  + " 未落盘但已提交到 " + committed, wasWritten(tp, offset));
        }
        long position = worker.position(partition);
        for (long offset = 0; offset < position; offset++) {
          if (worker.willBeRedelivered(partition, offset)) {
            continue;
          }
          Assert.assertTrue(
              "静默丢数：P" + partition + " offset " + offset + " 已被消费（位点 " + position
                  + "）且不会再重放，但没有落盘", wasWritten(tp, offset));
        }
      }
    }
  }

  /** 记录 connector 对 worker 调用的最小 SinkTaskContext。 */
  private static final class FakeContext implements SinkTaskContext {
    int requestCommitCalls;
    final Set<TopicPartition> assignment = new LinkedHashSet<TopicPartition>();

    @Override
    public Map<String, String> configs() {
      return Collections.emptyMap();
    }

    @Override
    public void offset(Map<TopicPartition, Long> offsets) {
      // 连接器不使用 rewind
    }

    @Override
    public void offset(TopicPartition tp, long offset) {
      // 连接器不使用 rewind
    }

    @Override
    public void timeout(long timeoutMs) {
    }

    @Override
    public Set<TopicPartition> assignment() {
      // 真实 worker 里这里返回 consumer.assignment()：open() 回调时已包含保留分区与新增分区
      return new LinkedHashSet<TopicPartition>(assignment);
    }

    @Override
    public void pause(TopicPartition... partitions) {
    }

    @Override
    public void resume(TopicPartition... partitions) {
    }

    @Override
    public void requestCommit() {
      requestCommitCalls++;
    }

    @Override
    public ErrantRecordReporter errantRecordReporter() {
      return null;
    }
  }

  private static final class NoOpConverter implements RecordConverter {
    @Override
    public void convert(SinkRecord in, Record out) {
      // 用例不经过转换层
    }
  }

  /**
   * 可注入故障的 BufferedWriter。只复刻任务层依赖的语义：
   * 记录入缓冲、maxOffset 取历史最大值（与真实实现一样不在 reset 里清零）、flush 成功才把缓冲计入落盘。
   */
  private static final class FakeWriter extends BufferedWriter {
    private final FakeSink sink;
    private final Map<TopicPartition, Behavior> behaviors;
    private final List<Long> buffer = new ArrayList<Long>();
    private TopicPartition partition;
    private long maxOffset = -1;

    FakeWriter(FakeSink sink, Map<TopicPartition, Behavior> behaviors) {
      // 父类构造只做本地对象装配（建 TableTunnel），不访问网络
      super(fakeOdps(), fakeConfig(), "project_placeholder", "table_placeholder", new NoOpConverter(), null);
      this.sink = sink;
      this.behaviors = behaviors;
    }

    private static Odps fakeOdps() {
      Odps odps = new Odps(new AliyunAccount("placeholder-id", "placeholder-key"));
      odps.setEndpoint("http://service.example.com/api");
      return odps;
    }

    private static ConnectorConfig fakeConfig() {
      Map<String, String> props = new HashMap<String, String>();
      props.put(ConfigParameter.MAXCOMPUTE_ENDPOINT.getName(), "http://service.example.com/api");
      props.put(ConfigParameter.MAXCOMPUTE_PROJECT.getName(), "project_placeholder");
      props.put(ConfigParameter.MAXCOMPUTE_TABLE.getName(), "table_placeholder");
      props.put(ConfigParameter.ACCESS_ID.getName(), "placeholder-id");
      props.put(ConfigParameter.ACCESS_KEY.getName(), "placeholder-key");
      return new ConnectorConfig(props);
    }

    private Behavior behavior(TopicPartition tp) {
      Behavior behavior = behaviors.get(tp);
      if (behavior == null) {
        behavior = new Behavior();
        behaviors.put(tp, behavior);
      }
      return behavior;
    }

    @Override
    public synchronized boolean write(SinkRecord record) {
      TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
      if (partition != null) {
        Assert.assertEquals("一个 writer 只服务一个分区", partition, tp);
      }
      partition = tp;
      Behavior behavior = behavior(tp);
      if (behavior.failNextWrite) {
        behavior.failNextWrite = false;
        // 与真实实现一致：首批第一条建 session 失败时抛 RuntimeException，该记录不入缓冲
        throw new RuntimeException("injected: init stream upload session failed");
      }
      buffer.add(Long.valueOf(record.kafkaOffset()));
      maxOffset = Math.max(maxOffset, record.kafkaOffset());
      return behavior.recordsPerCommitTrigger > 0 && buffer.size() >= behavior.recordsPerCommitTrigger;
    }

    @Override
    public synchronized SinkStatusContext.Status flushAndReset() {
      if (buffer.isEmpty()) {
        // 与真实实现一致：无待落盘数据时 preCommit 拿到 -1 或历史水位，不会推进提交
        return new SinkStatusContext.Status(maxOffset, 0L, 0L);
      }
      Behavior behavior = behavior(partition);
      if (behavior.failNextFlush && !behavior.failFlushAfterPersist) {
        behavior.failNextFlush = false;
        // 抛错时缓冲区保留（真实实现同样不会 reset），数据没有落盘
        throw new RuntimeException("injected: flush stream pack failed");
      }
      long records = buffer.size();
      sink.persist(partition, new ArrayList<Long>(buffer));
      buffer.clear();
      if (behavior.failFlushAfterPersist) {
        behavior.failFlushAfterPersist = false;
        // 服务端已收下数据但客户端看到失败：靠重放产生重复，而不是靠提交赌它成功
        throw new RuntimeException("injected: flush reported failure after persisting");
      }
      if (behavior.holdFromOffset >= 0) {
        // 与真实实现同构：数据已经落盘，但转投错误 topic 的那条记录没有投递凭据，水位只能停在它之前
        // （对应 BufferedWriter.flushAndReset() 里的 settlePendingReports() + clamp）。
        long heldAt = behavior.holdFromOffset;
        return new SinkStatusContext.Status(Math.min(maxOffset, heldAt - 1), records, records * 8L,
                                            0L, 1L, 1L, heldAt);
      }
      return new SinkStatusContext.Status(maxOffset, records, records * 8L);
    }

    @Override
    public synchronized boolean hasPendingData() {
      return !buffer.isEmpty();
    }
  }

  /** 只替换"需要连云"的两处：start 不取 schema，writer 换成可注入故障的替身。 */
  private static final class OfflineSinkTask extends SinkTaskImpl {
    private final FakeSink sink;
    private final Map<TopicPartition, Behavior> behaviors;

    OfflineSinkTask(FakeSink sink, Map<TopicPartition, Behavior> behaviors) {
      this.sink = sink;
      this.behaviors = behaviors;
    }

    @Override
    public void start(Map<String, String> props) {
      // 不建 Odps、不取 schema：本用例只覆盖 offset 提交与重平衡协议
    }

    @Override
    protected BufferedWriter newBufferedWriter() {
      return new FakeWriter(sink, behaviors);
    }
  }

  /** 建模 WorkerSinkTask 的投递/提交/重平衡调用序。 */
  private static final class FakeWorker {
    private final FakeSink sink;
    private final Map<TopicPartition, Behavior> behaviors;
    private final FakeContext context = new FakeContext();
    private final Map<Integer, Long> position = new LinkedHashMap<Integer, Long>();
    private final Map<Integer, Long> committed = new LinkedHashMap<Integer, Long>();
    /** 位点会被重置回已提交位置的分区：这些分区上未提交的记录会被重放。 */
    private final Set<Integer> redelivering = new HashSet<Integer>();
    private SinkTaskImpl task;

    FakeWorker(FakeSink sink, SinkTaskImpl task, Map<TopicPartition, Behavior> behaviors) {
      this.sink = sink;
      this.task = task;
      // 与 OfflineSinkTask 共用同一张行为表，注入的故障才能被 writer 读到
      this.behaviors = behaviors;
      task.initialize(context);
    }

    FakeContext context() {
      return context;
    }

    Collection<Integer> ownedPartitions() {
      return new ArrayList<Integer>(position.keySet());
    }

    long committed(int partition) {
      Long value = committed.get(Integer.valueOf(partition));
      return value == null ? 0L : value.longValue();
    }

    long position(int partition) {
      Long value = position.get(Integer.valueOf(partition));
      return value == null ? 0L : value.longValue();
    }

    /** offset 之后是否还会被重新投递给本 task：只有位点会被重置回它之前才会。 */
    boolean willBeRedelivered(int partition, long offset) {
      return redelivering.contains(Integer.valueOf(partition)) && offset >= committed(partition);
    }

    void addOwnedPartition(int partition) {
      if (!position.containsKey(Integer.valueOf(partition))) {
        position.put(Integer.valueOf(partition), Long.valueOf(0L));
        committed.put(Integer.valueOf(partition), Long.valueOf(0L));
      }
    }

    void failNextWriteOn(int partition) {
      behavior(new TopicPartition(TOPIC, partition)).failNextWrite = true;
    }

    void failNextFlushOn(int partition) {
      behavior(new TopicPartition(TOPIC, partition)).failNextFlush = true;
    }

    void failFlushAfterPersistingOn(int partition) {
      behavior(new TopicPartition(TOPIC, partition)).failFlushAfterPersist = true;
    }

    void setRecordsPerCommitTrigger(int partition, int records) {
      behavior(new TopicPartition(TOPIC, partition)).recordsPerCommitTrigger = records;
    }

    /** 模拟"转投错误 topic 的那条记录尚未确认投递"：writer 必须把水位停在它之前。 */
    void holdErrorReportFrom(int partition, long offset) {
      behavior(new TopicPartition(TOPIC, partition)).holdFromOffset = offset;
    }

    private Behavior behavior(TopicPartition tp) {
      Behavior behavior = behaviors.get(tp);
      if (behavior == null) {
        behavior = new Behavior();
        behaviors.put(tp, behavior);
      }
      return behavior;
    }

    void clearFaults() {
      for (Behavior behavior : behaviors.values()) {
        behavior.failNextWrite = false;
        behavior.failNextFlush = false;
        behavior.failFlushAfterPersist = false;
        behavior.holdFromOffset = -1;
      }
    }

    /** 消费 [firstOffset, firstOffset+count) 并推进位点；put 抛错时异常原样上抛。 */
    long deliver(int partition, long firstOffset, int count) {
      addOwnedPartition(partition);
      Assert.assertEquals("用例必须从当前消费位点开始投递", position(partition), firstOffset);
      List<SinkRecord> records = new ArrayList<SinkRecord>();
      for (long offset = firstOffset; offset < firstOffset + count; offset++) {
        records.add(new SinkRecord(TOPIC, partition, null, null, null, "v" + offset, offset));
      }
      position.put(Integer.valueOf(partition), Long.valueOf(firstOffset + count));
      task.put(records);
      return count;
    }

    Map<TopicPartition, OffsetAndMetadata> preCommitOnly() {
      return task.preCommit(currentOffsets());
    }

    private Map<TopicPartition, OffsetAndMetadata> currentOffsets() {
      Map<TopicPartition, OffsetAndMetadata> offsets = new LinkedHashMap<TopicPartition, OffsetAndMetadata>();
      for (Map.Entry<Integer, Long> entry : position.entrySet()) {
        offsets.put(new TopicPartition(TOPIC, entry.getKey().intValue()),
                    new OffsetAndMetadata(entry.getValue().longValue()));
      }
      return offsets;
    }

    /** 正常提交周期：preCommit 返回什么就提交什么；抛异常则整次提交作废。 */
    boolean commit() {
      Map<TopicPartition, OffsetAndMetadata> toCommit;
      try {
        toCommit = task.preCommit(currentOffsets());
      } catch (RuntimeException e) {
        return false;
      }
      applyCommit(toCommit);
      return true;
    }

    /** preCommit 成功、但 offset 提交在 broker 侧失败：不推进任何水位。 */
    boolean commitFailingAtBroker() {
      try {
        task.preCommit(currentOffsets());
      } catch (RuntimeException e) {
        return false;
      }
      return false;
    }

    private void applyCommit(Map<TopicPartition, OffsetAndMetadata> toCommit) {
      for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : toCommit.entrySet()) {
        int partition = entry.getKey().partition();
        committed.put(Integer.valueOf(partition), Long.valueOf(entry.getValue().offset()));
        redelivering.remove(Integer.valueOf(partition));
      }
    }

    /** task 被杀后重启：所有分区从已提交位点重新读取（等价于重新 assign 后位点回到 committed）。 */
    void expectTaskFailureAndRestart() {
      task = new OfflineSinkTask(sink, behaviors);
      task.initialize(context);
      context.assignment.clear();
      position.clear();
      redelivering.clear();
      for (Map.Entry<Integer, Long> entry : committed.entrySet()) {
        position.put(entry.getKey(), entry.getValue());
        redelivering.add(entry.getKey());
        context.assignment.add(new TopicPartition(TOPIC, entry.getKey().intValue()));
      }
    }

    /** cooperative 增量分配：只把新分区交给 open()，已有分区的位点不动。 */
    void gainPartitionCooperatively(int partition) {
      TopicPartition tp = new TopicPartition(TOPIC, partition);
      addOwnedPartition(partition);
      context.assignment.add(tp);
      task.open(Collections.singletonList(tp));
    }

    /** 正常 revoke + 重新分配回来：closing commit 先落盘提交，再 close，位点重置到已提交位置。 */
    void gracefulRebalanceReassigning(int partition) {
      TopicPartition tp = new TopicPartition(TOPIC, partition);
      List<TopicPartition> revoked = Collections.singletonList(tp);
      commitClosing(revoked);
      task.close(revoked);
      position.put(Integer.valueOf(partition), Long.valueOf(committed(partition)));
      redelivering.add(Integer.valueOf(partition));
      context.assignment.add(tp);
      task.open(revoked);
    }

    /** onPartitionsLost：不提交，只 close；这些分区由别的持有者从已提交位点重放。 */
    void losePartition(int partition) {
      TopicPartition tp = new TopicPartition(TOPIC, partition);
      task.close(Collections.singletonList(tp));
      context.assignment.remove(tp);
      position.remove(Integer.valueOf(partition));
      redelivering.remove(Integer.valueOf(partition));
    }

    private void commitClosing(Collection<TopicPartition> partitions) {
      Map<TopicPartition, OffsetAndMetadata> offsets = new LinkedHashMap<TopicPartition, OffsetAndMetadata>();
      for (TopicPartition tp : partitions) {
        Long value = position.get(Integer.valueOf(tp.partition()));
        if (value != null) {
          offsets.put(tp, new OffsetAndMetadata(value.longValue()));
        }
      }
      if (offsets.isEmpty()) {
        return;
      }
      try {
        applyCommit(task.preCommit(offsets));
      } catch (RuntimeException e) {
        // closing 分支不 rewind，直接放弃这次提交（WorkerSinkTask.java:407-411）
      }
    }
  }

  private static final class WorkerBuilder {
    private final FakeSink sink;
    private final Map<TopicPartition, Behavior> behaviors = new HashMap<TopicPartition, Behavior>();
    private final List<Integer> partitions = new ArrayList<Integer>();
    private int recordsPerCommitTrigger = -1;

    WorkerBuilder(FakeSink sink) {
      this.sink = sink;
    }

    WorkerBuilder withTaskPartitions(int... partitions) {
      for (int partition : partitions) {
        this.partitions.add(Integer.valueOf(partition));
      }
      return this;
    }

    WorkerBuilder withRecordsPerCommitTrigger(int records) {
      this.recordsPerCommitTrigger = records;
      return this;
    }

    FakeWorker build() {
      SinkTaskImpl task = new OfflineSinkTask(sink, behaviors);
      FakeWorker worker = new FakeWorker(sink, task, behaviors);
      List<TopicPartition> assigned = new ArrayList<TopicPartition>();
      for (Integer partition : partitions) {
        worker.addOwnedPartition(partition.intValue());
        TopicPartition tp = new TopicPartition(TOPIC, partition.intValue());
        assigned.add(tp);
        worker.context.assignment.add(tp);
        if (recordsPerCommitTrigger > 0) {
          worker.setRecordsPerCommitTrigger(partition.intValue(), recordsPerCommitTrigger);
        }
      }
      if (!assigned.isEmpty()) {
        task.open(assigned);
      }
      return worker;
    }
  }
}
