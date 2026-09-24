package com.aliyun.odps.kafka.connect;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 集成测试的凭据前置检查。
 *
 * <p>缺少云凭据时集成用例必须以失败结束，不能被报成"集成测试通过"。
 * 纯逻辑回归（{@code mvn test}）不读取这些变量。
 */
public final class TestCredentials {

  public static final String ACCESS_ID = "ALIBABA_CLOUD_ACCESS_KEY_ID";
  public static final String ACCESS_KEY = "ALIBABA_CLOUD_ACCESS_KEY_SECRET";
  public static final String ENDPOINT = "odps_endpoint";
  public static final String PROJECT = "MAXCOMPUTE_PROJECT";

  public static final List<String> REQUIRED_ENV =
      Arrays.asList(ACCESS_ID, ACCESS_KEY, ENDPOINT, PROJECT);

  private TestCredentials() {
  }

  public static List<String> missingEnv() {
    List<String> missing = new ArrayList<String>();
    for (String name : REQUIRED_ENV) {
      String value = System.getenv(name);
      if (value == null || value.trim().isEmpty()) {
        missing.add(name);
      }
    }
    return missing;
  }

  /**
   * 缺少任一必需环境变量时抛 AssertionError，使集成回归以失败而不是"未运行=通过"结束。
   */
  public static void requireMaxComputeEnv() {
    List<String> missing = missingEnv();
    if (!missing.isEmpty()) {
      throw new AssertionError(
          "MaxCompute 集成测试缺少环境变量 " + missing + "。集成用例未运行，不应被记为通过。"
              + "请先配置上述变量再执行 `mvn test -Pintegration-tests`；"
              + "只跑无需云凭据的回归请用 `mvn test`。");
    }
  }
}
