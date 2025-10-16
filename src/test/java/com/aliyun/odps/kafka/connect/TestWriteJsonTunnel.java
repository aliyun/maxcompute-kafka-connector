package com.aliyun.odps.kafka.connect;

import java.util.Map;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.data.SimpleJsonValue;
import com.aliyun.odps.kafka.connect.utils.ConfigHelper;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;

public class TestWriteJsonTunnel {

  // 测试将JSON数据通过Tunnel API写入MaxCompute表的功能
  // 验证Tunnel写入功能是否正常工作

  @Test
  @Ignore
  public void TestTunnel() {
    Map<String, String> props = ConfigHelper.getTestConfigFromEnv("ALIYUN", null);
    String accessId = props.get("access_id");
    String accessKey = props.get("access_key");
    String odpsUrl = props.get("endpoint");
    String project = props.get("project");
    String table = props.get("table");
    Account account = new AliyunAccount(accessId, accessKey);
    Odps odps = new Odps(account);
    odps.setEndpoint(odpsUrl);
    odps.setDefaultProject(project);
    try {
      TableTunnel tunnel = new TableTunnel(odps);
      UploadSession uploadSession = tunnel.createUploadSession(project, table);
      ArrayRecord record = new ArrayRecord(uploadSession.getSchema());
      String curTime = String.valueOf(System.currentTimeMillis());
      System.out.println("curTime {} " + curTime);
      String jsonValue = "{\"root2\":{\"key\":\"" + curTime + "\"}}";
      record.setJsonValue(0, new SimpleJsonValue(jsonValue));
      RecordWriter w1 = uploadSession.openRecordWriter(0);
      Assert.assertNotNull(w1);
      w1.write(record);
      w1.close();
      uploadSession.commit(new Long[]{0L});
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
