package com.aliyun.odps.kafka.connect.sink;

import static com.aliyun.odps.kafka.connect.ConfigParameter.SKIP_EMPTY_FLUSH;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.kafka.connect.ConfigParameter;
import com.aliyun.odps.kafka.connect.ConnectorConfig;
import com.aliyun.odps.kafka.connect.converter.RecordConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;

/**
 * "空窗口要不要打一次 tunnel"的判据回归：不访问 MaxCompute，也不启动 broker，属于 PR 门禁的 unit 档。
 *
 * <p>这条判据值得单独钉：一个窗口里只要真放过记录就必须落盘，开关也不能把它吞掉；
 * 反过来，开关打开时省掉的必须是"什么都没有"的那种往返——真服务端实测那种往返每分区 2.4-2.7 秒，
 * 而 preCommit 是逐分区串行的（数据见工作项 evidence 的 C/D 段）。
 */
public class BufferedWriterEmptyWindowFlushTest {

  private static ConnectorConfig config(Boolean skipEmptyFlush) {
    Map<String, String> props = new HashMap<String, String>();
    props.put(ConfigParameter.MAXCOMPUTE_ENDPOINT.getName(), "http://service.example.com/api");
    props.put(ConfigParameter.MAXCOMPUTE_PROJECT.getName(), "project_placeholder");
    props.put(ConfigParameter.MAXCOMPUTE_TABLE.getName(), "table_placeholder");
    props.put(ConfigParameter.ACCESS_ID.getName(), "placeholder-id");
    props.put(ConfigParameter.ACCESS_KEY.getName(), "placeholder-key");
    if (skipEmptyFlush != null) {
      props.put(SKIP_EMPTY_FLUSH.getName(), skipEmptyFlush.toString());
    }
    return new ConnectorConfig(props);
  }

  private static BufferedWriter writer(Boolean skipEmptyFlush) {
    Odps odps = new Odps(new AliyunAccount("placeholder-id", "placeholder-key"));
    odps.setEndpoint("http://service.example.com/api");
    RecordConverter converter = new RecordConverter() {
      @Override
      public void convert(SinkRecord in, Record out) throws IOException {
      }
    };
    return new BufferedWriter(odps, config(skipEmptyFlush), "project_placeholder", "table_placeholder", converter,
                              null);
  }

  @Test
  public void defaultStillFlushesAnEmptyWindow() {
    BufferedWriter w = writer(null);
    Assert.assertFalse("默认必须与修复前一致：空窗口也照样打一次 tunnel", w.skipEmptyThisWindow(0L, 0L));
    Assert.assertFalse(w.skipEmptyThisWindow(0L, 4096L));
  }

  @Test
  public void flagSkipsOnlyAWindowThatHoldsNothing() {
    BufferedWriter w = writer(Boolean.TRUE);
    Assert.assertTrue("开关打开时，零记录且零字节的窗口才省掉这次往返", w.skipEmptyThisWindow(0L, 0L));
    Assert.assertFalse("放过记录就必须落盘", w.skipEmptyThisWindow(1L, 0L));
    Assert.assertFalse("包里还有字节（append 过但没冲出去）就必须落盘", w.skipEmptyThisWindow(0L, 1L));
  }

  @Test
  public void theFlagIsOffByDefault() {
    Assert.assertEquals(Boolean.FALSE, SKIP_EMPTY_FLUSH.getDefaultValue());
    Assert.assertFalse(SKIP_EMPTY_FLUSH.getBoolean(config(null)));
    Assert.assertTrue(SKIP_EMPTY_FLUSH.getBoolean(config(Boolean.TRUE)));
  }
}
