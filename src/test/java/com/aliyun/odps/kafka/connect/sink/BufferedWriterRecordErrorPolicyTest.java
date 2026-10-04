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

package com.aliyun.odps.kafka.connect.sink;

import static com.aliyun.odps.kafka.connect.ConfigParameter.ACCESS_ID;
import static com.aliyun.odps.kafka.connect.ConfigParameter.ACCESS_KEY;
import static com.aliyun.odps.kafka.connect.ConfigParameter.MAXCOMPUTE_ENDPOINT;
import static com.aliyun.odps.kafka.connect.ConfigParameter.MAXCOMPUTE_PROJECT;
import static com.aliyun.odps.kafka.connect.ConfigParameter.MAXCOMPUTE_TABLE;
import static com.aliyun.odps.kafka.connect.ConfigParameter.SKIP_ERROR;
import static com.aliyun.odps.kafka.connect.fixtures.Fixtures.col;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import com.aliyun.odps.Odps;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.kafka.connect.ConnectorConfig;
import com.aliyun.odps.kafka.connect.SinkStatusContext;
import com.aliyun.odps.kafka.connect.converter.FlattenRecordConverter;
import com.aliyun.odps.kafka.connect.converter.JsonRecordConverter;
import com.aliyun.odps.kafka.connect.converter.RecordConverter;
import com.aliyun.odps.kafka.connect.converter.RecordConverterBuilder.Mode;
import com.aliyun.odps.kafka.connect.fixtures.Fixtures;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.type.TypeInfoFactory;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;

/**
 * 坏记录（转换失败 / 落盘失败 / 墓碑记录 / 缺字段）的处理策略回归：不读云凭据、不启动 broker，
 * 属于 PR 门禁的 unit 档。
 *
 * <p>替身只替换 tunnel 侧的三个接口（{@code StreamUploadSession} / {@code StreamRecordPack} 在 SDK 里
 * 都是接口），驱动的是 {@link BufferedWriter} 自己的控制流：一条记录到底 append 没 append、
 * {@code flushAndReset()} 交给 {@code preCommit} 的水位推进到哪里、以及水位越过的记录有没有去向。
 */
public class BufferedWriterRecordErrorPolicyTest {

  /** JSON 格式的 value 列必须是 JSON 类型（STRING 列会在写入载体时被 SDK 拒掉）。 */
  private static final TableSchema SCHEMA = Fixtures.schemaWithFixedColumns(
      col("value", TypeInfoFactory.JSON));

  private static final TableSchema FLATTEN_SCHEMA = Fixtures.schemaWithFixedColumns(
      col("id", TypeInfoFactory.BIGINT),
      col("name", TypeInfoFactory.STRING));

  private static final int VALUE_COL = 4;
  private static final int OFFSET_COL = 2;
  private static final int FLATTEN_ID_COL = 4;
  private static final int FLATTEN_NAME_COL = 5;

  // ------------------------------------------------------------------ 用例

  /** skip_error=false（默认）：坏记录抛错，水位不前进，交给 worker 决定重试或失败。 */
  @Test
  public void failFastKeepsWatermarkBeforeTheBadRecord() {
    FakePack pack = new FakePack();
    BufferedWriter writer = writer(false, null, pack, new ThrowingConverter());

    expectRuntimeException(writer, sinkRecord(1L, null, "{\"a\":1}"));

    SinkStatusContext.Status status = writer.flushAndReset();
    Assert.assertEquals("抛错路径不得把水位推到这条记录之后", -1L, status.getMaxOffset());
    Assert.assertEquals(0L, status.getDroppedRecords());
    Assert.assertEquals(0L, status.getReportedRecords());
    Assert.assertTrue(pack.appended.isEmpty());
  }

  /**
   * skip_error=true 且没配 runtime.error.topic.*：这条记录既没落盘也没有去处。水位照旧前进
   * （这就是"跳过"的语义），但必须被单独记账 —— 修复前这个分支连一行日志都没有，
   * 提交位点就越过了一条查无此据的 offset。
   */
  @Test
  public void skipWithoutReporterAdvancesWatermarkButIsAccounted() {
    FakePack pack = new FakePack();
    BufferedWriter writer = writer(true, null, pack, new ThrowingConverter());

    writer.write(sinkRecord(9L, null, "junk"));

    SinkStatusContext.Status status = writer.flushAndReset();
    Assert.assertEquals(0, pack.appended.size());
    Assert.assertEquals("水位确实越过了这条记录", 9L, status.getMaxOffset());
    Assert.assertEquals("但这条丢弃必须被记账，供 preCommit 说明", 1L, status.getDroppedRecords());
    Assert.assertEquals(0L, status.getReportedRecords());
  }

  /** 配了错误上报通道：坏记录有去处，记为 reported 而不是 dropped。 */
  @Test
  public void reportedRecordHasADestination() {
    FakePack pack = new FakePack();
    RecordingReporter reporter = new RecordingReporter();
    BufferedWriter writer = writer(false, reporter, pack, new ThrowingConverter());

    writer.write(sinkRecord(3L, null, "junk"));

    SinkStatusContext.Status status = writer.flushAndReset();
    Assert.assertEquals(1, reporter.reported.size());
    Assert.assertEquals(3L, status.getMaxOffset());
    Assert.assertEquals(0L, status.getDroppedRecords());
    Assert.assertEquals(1L, status.getReportedRecords());
  }

  /** 上报通道自己抛错（DLQ 生产者不可用）+ 容错开启：这条记录依然没有去处，按丢弃记账而不是悄悄成功。 */
  @Test
  public void reporterFailureFallsBackToAccountedDrop() {
    FakePack pack = new FakePack();
    BufferedWriter writer = writer(true, new ThrowingReporter(), pack, new ThrowingConverter());

    writer.write(sinkRecord(5L, null, "junk"));

    SinkStatusContext.Status status = writer.flushAndReset();
    Assert.assertEquals(1L, status.getDroppedRecords());
    Assert.assertEquals(0L, status.getReportedRecords());
  }

  /** 上报通道抛错 + 容错关闭：必须抛出去，不能因为"配了 reporter"就把它当成已处理。 */
  @Test
  public void reporterFailureWithoutSkipPropagates() {
    FakePack pack = new FakePack();
    BufferedWriter writer = writer(false, new ThrowingReporter(), pack, new ThrowingConverter());

    expectRuntimeException(writer, sinkRecord(5L, null, "junk"));
    Assert.assertEquals(-1L, writer.flushAndReset().getMaxOffset());
  }

  /**
   * 转换成功但落盘失败（超长值、坏编码这类只在序列化阶段暴露的问题）：走同一套策略，
   * 容错开启时同样是"有记账的丢弃"。修复前它与转换失败共用同一个静默分支。
   */
  @Test
  public void appendFailureIsTreatedAsADroppedRecord() {
    FakePack pack = new FakePack();
    pack.appendFailure = new IOException("Value exceeds the maximum allowed size");
    BufferedWriter writer = writer(true, null, pack, new JsonRecordConverter(Mode.VALUE));

    writer.write(sinkRecord(7L, null, "{\"a\":1}"));

    SinkStatusContext.Status status = writer.flushAndReset();
    Assert.assertEquals(0, pack.appended.size());
    Assert.assertEquals(7L, status.getMaxOffset());
    Assert.assertEquals(1L, status.getDroppedRecords());
  }

  /**
   * 墓碑记录（value 为 NULL）：JSON 转换器不写 value 列。修复前 BufferedWriter 把上一条记录用过的
   * 同一个 Record 载体再交给它，于是这条记录落成"带着上一条内容、offset 是新的"的行 ——
   * 表里多出一行没人投递过的数据，且没有任何错误痕迹。
   */
  @Test
  public void tombstoneDoesNotInheritThePreviousRecordsPayload() {
    FakePack pack = new FakePack();
    BufferedWriter writer = writer(false, null, pack, new JsonRecordConverter(Mode.VALUE));

    writer.write(sinkRecord(1L, null, "{\"order\":\"first\"}"));
    writer.write(sinkRecord(2L, null, null));
    writer.flushAndReset();

    Assert.assertEquals(2, pack.appended.size());
    Assert.assertNotNull("第一条照常落盘", pack.appended.get(0)[VALUE_COL]);
    Object[] tombstone = pack.appended.get(1);
    Assert.assertEquals(Long.valueOf(2L), tombstone[OFFSET_COL]);
    Assert.assertNull("value 为 NULL 的记录不得复用上一条的载荷", tombstone[VALUE_COL]);
  }

  /** 同上，FLATTEN：JSON 里缺字段时那一列必须是 NULL，不能留着上一条记录的值。 */
  @Test
  public void flattenedRecordWithoutAFieldWritesNullNotThePreviousValue() {
    FakePack pack = new FakePack();
    BufferedWriter writer = writer(false, null, pack,
        new FlattenRecordConverter(Mode.VALUE, FLATTEN_SCHEMA), FLATTEN_SCHEMA);

    writer.write(sinkRecord(1L, null, "{\"id\":100,\"name\":\"x\"}"));
    writer.write(sinkRecord(2L, null, "{\"name\":\"y\"}"));
    writer.flushAndReset();

    Assert.assertEquals(2, pack.appended.size());
    Assert.assertEquals(Long.valueOf(100L), pack.appended.get(0)[FLATTEN_ID_COL]);
    Assert.assertNull("本条 JSON 里没有 id 这一列", pack.appended.get(1)[FLATTEN_ID_COL]);
    Assert.assertEquals("y", pack.appended.get(1)[FLATTEN_NAME_COL]);
  }

  /** 显式的 JSON null 字段值：应当写成 NULL，而不是把整条记录变成一条 NullPointerException。 */
  @Test
  public void explicitJsonNullFieldWritesNullColumn() {
    FakePack pack = new FakePack();
    BufferedWriter writer = writer(false, null, pack,
        new FlattenRecordConverter(Mode.VALUE, FLATTEN_SCHEMA), FLATTEN_SCHEMA);

    writer.write(sinkRecord(1L, null, "{\"id\":null,\"name\":\"x\"}"));
    writer.flushAndReset();

    Assert.assertEquals("显式 null 不是坏记录，应照常落盘", 1, pack.appended.size());
    Assert.assertNull(pack.appended.get(0)[FLATTEN_ID_COL]);
    Assert.assertEquals("x", pack.appended.get(0)[FLATTEN_NAME_COL]);
  }

  /**
   * 落盘条数必须在 flush 之后仍然可读：{@code reset()} 会清零 processedRecords，
   * 原来 {@code flushAndReset()} 在 reset 之后才读它，于是交给 SinkStatusContext 结转的条数恒为 0，
   * "Total write ... records" 与 preCommit 那句"写了多少条"一起失真（真实服务端跑出来的现象）。
   */
  @Test
  public void flushedRecordCountSurvivesTheReset() {
    FakePack pack = new FakePack();
    BufferedWriter writer = writer(false, null, pack, new JsonRecordConverter(Mode.VALUE));

    writer.write(sinkRecord(1L, null, "{\"a\":1}"));
    writer.write(sinkRecord(2L, null, "{\"a\":2}"));
    Assert.assertEquals(2L, writer.flushAndReset().getProcessedRecords());

    // 结转是一次性的：下一次 flush 只报这一段新落的条数
    writer.write(sinkRecord(3L, null, "{\"a\":3}"));
    SinkStatusContext.Status second = writer.flushAndReset();
    Assert.assertEquals(1L, second.getProcessedRecords());
    Assert.assertEquals(3L, second.getMaxOffset());
  }

  /** 丢弃/上报计数一次性结转：一次 flush 交给调用方之后，下一次 flush 只说这一段的事。 */
  @Test
  public void dropCountersAreHandedOffOncePerFlush() {
    FakePack pack = new FakePack();
    BufferedWriter writer = writer(true, null, pack, new ThrowingConverter());

    writer.write(sinkRecord(1L, null, "junk"));
    writer.write(sinkRecord(2L, null, "junk"));
    Assert.assertEquals(2L, writer.flushAndReset().getDroppedRecords());

    writer.write(sinkRecord(3L, null, "junk"));
    SinkStatusContext.Status second = writer.flushAndReset();
    Assert.assertEquals(1L, second.getDroppedRecords());
    Assert.assertEquals(3L, second.getMaxOffset());
  }

  /**
   * 评审点名的那条时间线：坏记录被交给错误 topic，投递是异步的，broker 在 commit 之前拒收。
   *
   * <p>修复前 {@code handleFailedRecord()} 在入队那一刻就记为"有去处"，水位照样越过这条 offset：
   * 记录既不在 MaxCompute（转换失败没写进去），也不在错误 topic（投递被拒），而已提交的位点让它
   * 无法重放。现在水位停在它之前，并且下一次 flush 会重投一次。
   */
  @Test
  public void asyncDeliveryFailureHoldsWatermarkBeforeTheRecord() {
    FakePack pack = new FakePack();
    DeferredReporter reporter = new DeferredReporter();
    BufferedWriter writer = writer(true, reporter, pack, new ThrowingConverter());
    writer.setReportSettleTimeoutMs(50L);

    writer.write(sinkRecord(7L, null, "junk"));
    // 异步的结局在 commit 之前揭晓：broker 拒收了这一次投递
    reporter.settle(0, new IllegalStateException("broker rejected the batch"));

    SinkStatusContext.Status held = writer.flushAndReset();
    Assert.assertEquals("未确认的记录是本次唯一的记录，水位不能越过它", 6L, held.getMaxOffset());
    Assert.assertEquals(1L, held.getUnconfirmedReportedRecords());
    Assert.assertEquals("水位被挡在这条记录之前", 7L, held.getHeldAtOffset());
    Assert.assertEquals("投递被拒后应重投一次", 2, reporter.reports());

    // 重投成功之后，水位才允许越过它
    reporter.settle(1, null);
    SinkStatusContext.Status released = writer.flushAndReset();
    Assert.assertEquals(7L, released.getMaxOffset());
    Assert.assertEquals(0L, released.getUnconfirmedReportedRecords());
    Assert.assertEquals(-1L, released.getHeldAtOffset());
  }

  /** 还在途（没有最终结局）同样不算送达：不能靠"没报错"就把位点推过去。 */
  @Test
  public void inFlightDeliveryHoldsWatermarkUntilItCompletes() {
    FakePack pack = new FakePack();
    DeferredReporter reporter = new DeferredReporter();
    BufferedWriter writer = writer(true, reporter, pack, new ThrowingConverter());
    writer.setReportSettleTimeoutMs(50L);

    writer.write(sinkRecord(4L, null, "junk"));
    SinkStatusContext.Status held = writer.flushAndReset();
    Assert.assertEquals(3L, held.getMaxOffset());
    Assert.assertEquals("还在途的记录不重投，否则一份数据在 DLQ 里出现两次", 1, reporter.reports());

    reporter.settle(0, null);
    Assert.assertEquals(4L, writer.flushAndReset().getMaxOffset());
  }

  /** 投递被拒之后的重投又被拒：继续挡着，并且不无限重投（每次 flush 最多补投一次）。 */
  @Test
  public void repeatedlyFailedDeliveryKeepsHoldingWithoutUnboundedRetries() {
    FakePack pack = new FakePack();
    DeferredReporter reporter = new DeferredReporter();
    BufferedWriter writer = writer(true, reporter, pack, new ThrowingConverter());
    writer.setReportSettleTimeoutMs(50L);

    writer.write(sinkRecord(2L, null, "junk"));
    reporter.settle(0, new IllegalStateException("broker rejected the batch"));
    SinkStatusContext.Status first = writer.flushAndReset();
    Assert.assertEquals(1L, first.getMaxOffset());
    Assert.assertEquals(2, reporter.reports());

    // 补投又被拒：水位继续挡着，但不再每轮 flush 都重投
    reporter.settle(1, new IllegalStateException("broker still rejecting"));
    SinkStatusContext.Status again = writer.flushAndReset();
    Assert.assertEquals(1L, again.getMaxOffset());
    Assert.assertEquals("补投只到第二次为止，之后交给位点不推进 + 日志，而不是每轮再试", 2, reporter.reports());

    // 已经补投过一次的记录不会每轮 flush 都重投，位点则一直停在它前面
    writer.flushAndReset();
    Assert.assertEquals(2, reporter.reports());
  }

  /** 上报通道没交出任何投递凭据（返回 null）：这等价于"不知道去哪了"，不能算有去处。 */
  @Test
  public void reporterWithoutDeliveryHandleHoldsWatermark() {
    FakePack pack = new FakePack();
    BufferedWriter writer = writer(true, new HandlelessReporter(), pack, new ThrowingConverter());
    writer.setReportSettleTimeoutMs(50L);

    writer.write(sinkRecord(8L, null, "junk"));
    SinkStatusContext.Status status = writer.flushAndReset();
    Assert.assertEquals(7L, status.getMaxOffset());
    Assert.assertEquals(1L, status.getUnconfirmedReportedRecords());
  }

  /**
   * 挡水位不是把整个窗口扣住：它下面已经落盘的记录照旧提交，只有从被挡那条开始重放。
   * 重放会让上面已落盘的记录再写一次（本连接器本来就是 at-least-once），这比静默丢数好。
   */
  @Test
  public void recordsBelowTheHeldOneStillAdvance() {
    FakePack pack = new FakePack();
    DeferredReporter reporter = new DeferredReporter();
    BufferedWriter writer = writer(true, reporter, pack, new JsonRecordConverter(Mode.VALUE));
    writer.setReportSettleTimeoutMs(50L);

    writer.write(sinkRecord(10L, null, "{\"a\":1}"));
    writer.write(sinkRecord(11L, null, "not json"));
    writer.write(sinkRecord(12L, null, "{\"a\":3}"));

    SinkStatusContext.Status status = writer.flushAndReset();
    Assert.assertEquals("10 已经落盘，可以提交到它之后", 10L, status.getMaxOffset());
    Assert.assertEquals(11L, status.getHeldAtOffset());
    Assert.assertEquals("落盘的条数只算真写进去的两条", 2L, status.getProcessedRecords());
    Assert.assertEquals("只有两条真写进了 record pack", 2, pack.appended.size());

    reporter.settle(0, null);
    SinkStatusContext.Status released = writer.flushAndReset();
    Assert.assertEquals("补投确认后水位一次推到本窗口最高", 12L, released.getMaxOffset());
  }

  // ------------------------------------------------------------------ 替身

  private static BufferedWriter writer(boolean skipError, ErrantRecordReporter reporter, FakePack pack,
      RecordConverter converter) {
    return writer(skipError, reporter, pack, converter, SCHEMA);
  }

  private static BufferedWriter writer(boolean skipError, ErrantRecordReporter reporter, FakePack pack,
      RecordConverter converter, final TableSchema schema) {
    Odps odps = new Odps(new AliyunAccount("placeholder-id", "placeholder-key"));
    odps.setEndpoint("http://service.example.com/api");
    return new BufferedWriter(odps, config(skipError), "project_placeholder", "table_placeholder", converter,
                              reporter, new FakeTunnel(odps, pack, schema));
  }

  private static ConnectorConfig config(boolean skipError) {
    Map<String, String> props = new HashMap<String, String>();
    props.put(MAXCOMPUTE_ENDPOINT.getName(), "http://service.example.com/api");
    props.put(MAXCOMPUTE_PROJECT.getName(), "project_placeholder");
    props.put(MAXCOMPUTE_TABLE.getName(), "table_placeholder");
    props.put(ACCESS_ID.getName(), "placeholder-id");
    props.put(ACCESS_KEY.getName(), "placeholder-key");
    props.put(SKIP_ERROR.getName(), String.valueOf(skipError));
    return new ConnectorConfig(props);
  }

  private static SinkRecord sinkRecord(long offset, Object key, Object value) {
    return Fixtures.sinkRecord(Fixtures.TOPIC_NAME, Fixtures.PARTITION_ID, offset,
                               Long.valueOf(Fixtures.TIMESTAMP_MS), key, value);
  }

  private static void expectRuntimeException(BufferedWriter writer, SinkRecord record) {
    try {
      writer.write(record);
      Assert.fail("Expected a RuntimeException to be thrown");
    } catch (RuntimeException expected) {
      // expected
    }
  }

  private static final class ThrowingConverter implements RecordConverter {
    @Override
    public void convert(SinkRecord in, Record out) throws IOException {
      throw new IOException("cannot parse record");
    }
  }

  /** append 时做列值快照：BufferedWriter 复用的是同一个 Record 对象，只存引用看不出上一条的残留。 */
  private static final class FakePack implements TableTunnel.StreamRecordPack {
    private final List<Object[]> appended = new ArrayList<Object[]>();
    private long size = 0;
    private IOException appendFailure;
    private int flushes = 0;

    @Override
    public void append(Record record) throws IOException {
      if (appendFailure != null) {
        IOException failure = appendFailure;
        appendFailure = null;
        throw failure;
      }
      Object[] snapshot = new Object[record.getColumnCount()];
      for (int i = 0; i < snapshot.length; i++) {
        snapshot[i] = record.get(i);
      }
      appended.add(snapshot);
      size += 1024;
    }

    @Override
    public long getRecordCount() {
      return appended.size();
    }

    @Override
    public long getDataSize() {
      return size;
    }

    @Override
    public String flush() {
      flushes++;
      size = 0;
      return "fake-trace-id";
    }

    @Override
    public TableTunnel.FlushResult flush(TableTunnel.FlushOption option) {
      flush();
      return null;
    }

    @Override
    public void reset() {
      size = 0;
    }
  }

  private static final class FakeSession implements TableTunnel.StreamUploadSession {
    private final FakePack pack;
    private final TableSchema schema;

    private FakeSession(FakePack pack, TableSchema schema) {
      this.pack = pack;
      this.schema = schema;
    }

    @Override
    public void setP2pMode(boolean isPeerToPeer) {
    }

    @Override
    public String getId() {
      return "fake-session";
    }

    @Override
    public TableSchema getSchema() {
      return schema;
    }

    @Override
    public String getSchemaVersion() {
      return "1";
    }

    @Override
    public String getQuotaName() {
      return "";
    }

    @Override
    public TableTunnel.StreamRecordPack newRecordPack() {
      return pack;
    }

    @Override
    public TableTunnel.StreamRecordPack newRecordPack(CompressOption compressOption) {
      return pack;
    }

    @Override
    public Record newRecord() {
      return new ArrayRecord(schema);
    }
  }

  /** 只替换建 session 这一步：setPartitionSpec / setCreatePartition / build() 的调用形状保持原样。 */
  private static final class FakeTunnel extends TableTunnel {
    private final FakeSession session;

    private FakeTunnel(Odps odps, FakePack pack, TableSchema schema) {
      super(odps);
      this.session = new FakeSession(pack, schema);
    }

    @Override
    public StreamUploadSession.Builder buildStreamUploadSession(String project, String table) {
      return new StreamUploadSession.Builder() {
        @Override
        public StreamUploadSession build() {
          return session;
        }
      };
    }
  }

  /**
   * 确认投递的上报通道：返回一个已完成且不带异常的 Future，等价于"这条记录已经落到错误 topic"。
   *
   * <p>修复前它可以返回 null 也照样被当作"有去处"。现在没有投递凭据就不算送达（见
   * {@link #reporterWithoutDeliveryHandleHoldsWatermark()}），所以这个替身必须交出凭据。
   */
  private static final class RecordingReporter implements ErrantRecordReporter {
    private final List<SinkRecord> reported = new ArrayList<SinkRecord>();

    @Override
    public Future<Void> report(SinkRecord record, Throwable error) {
      reported.add(record);
      return completedDelivery();
    }
  }

  private static Future<Void> completedDelivery() {
    CompletableFuture<Void> delivered = new CompletableFuture<Void>();
    delivered.complete(null);
    return delivered;
  }

  /**
   * 可以事后决定成败的上报通道：{@code report()} 先返回一个未完成的 Future，测试再决定 broker 是
   * 收下还是拒收 —— 这正是评审指出的那条时间线（异步投递的结局在 commit 之前才揭晓）。
   */
  private static final class DeferredReporter implements ErrantRecordReporter {
    private final List<SinkRecord> reported = new ArrayList<SinkRecord>();
    private final List<CompletableFuture<Void>> deliveries = new ArrayList<CompletableFuture<Void>>();

    @Override
    public Future<Void> report(SinkRecord record, Throwable error) {
      reported.add(record);
      CompletableFuture<Void> delivery = new CompletableFuture<Void>();
      deliveries.add(delivery);
      return delivery;
    }

    /** 第 index 次上报的最终结局。 */
    private void settle(int index, Throwable failure) {
      CompletableFuture<Void> delivery = deliveries.get(index);
      if (failure == null) {
        delivery.complete(null);
      } else {
        delivery.completeExceptionally(failure);
      }
    }

    private int reports() {
      return reported.size();
    }
  }

  /** 交出 null 的上报通道：调用方拿不到任何投递凭据。 */
  private static final class HandlelessReporter implements ErrantRecordReporter {
    @Override
    public Future<Void> report(SinkRecord record, Throwable error) {
      return null;
    }
  }

  private static final class ThrowingReporter implements ErrantRecordReporter {
    @Override
    public Future<Void> report(SinkRecord record, Throwable error) {
      throw new IllegalStateException("error topic producer is down");
    }
  }
}
