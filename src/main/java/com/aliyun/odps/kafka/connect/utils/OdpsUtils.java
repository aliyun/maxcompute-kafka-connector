package com.aliyun.odps.kafka.connect.utils;

import com.aliyun.credentials.Client;
import com.aliyun.credentials.models.Config;
import com.aliyun.credentials.provider.AlibabaCloudCredentialsProvider;
import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AklessAccount;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.kafka.connect.MaxComputeSinkConnectorConfig;
import com.aliyun.odps.kafka.connect.account.StsCredentialsProvider;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;

public class OdpsUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(OdpsUtils.class);

  public static Odps getOdps(MaxComputeSinkConnectorConfig config) {
    String
        accountType =
        config.getString(MaxComputeSinkConnectorConfig.BaseParameter.ACCOUNT_TYPE.getName());
    Account account;
    if (accountType.equalsIgnoreCase(Account.AccountProvider.STS.toString())) {
      account = new AklessAccount(new StsCredentialsProvider(config));
    } else if (accountType.equalsIgnoreCase(Account.AccountProvider.ALIYUN.toString())) {
      String accessId =
        config.getString(MaxComputeSinkConnectorConfig.BaseParameter.ACCESS_ID.getName());
      String accessKey =
        config.getString(MaxComputeSinkConnectorConfig.BaseParameter.ACCESS_KEY.getName());
      account = new AliyunAccount(accessId, accessKey);
    } else {
      LOGGER.info("use akless account to get credencial.");
      Config credencialConfig = Config.build(config.getConfigMap());
      try {
        Client client = new Client(credencialConfig);
        Field field = Client.class.getDeclaredField("credentialsProvider");
        field.setAccessible(true); // 破除 private 限制

        AlibabaCloudCredentialsProvider provider =
                (AlibabaCloudCredentialsProvider) field.get(client);
        account = new AklessAccount(provider);

      } catch (Exception e) {
        LOGGER.error("get akless account failed!", e);
        throw new RuntimeException(e);
      }
    }

    Odps odps = new Odps(account);
    String
        endpoint =
        config.getString(MaxComputeSinkConnectorConfig.BaseParameter.MAXCOMPUTE_ENDPOINT.getName());
    String
        project =
        config.getString(MaxComputeSinkConnectorConfig.BaseParameter.MAXCOMPUTE_PROJECT.getName());
    odps.setDefaultProject(project);
    if (!StringUtils.isNullOrEmpty(config.getString(
        MaxComputeSinkConnectorConfig.BaseParameter.MAXCOMPUTE_SCHEMA.getName()))) {
      odps.setCurrentSchema(config.getString(
          MaxComputeSinkConnectorConfig.BaseParameter.MAXCOMPUTE_SCHEMA.getName()));
    }
    odps.setEndpoint(endpoint);
    odps.setUserAgent("aliyun-maxc-kafka-connector");
    odps.getRestClient().setRetryTimes(1);
    odps.getRestClient().setReadTimeout(20);

    return odps;
  }

  public static class RetryLogger extends RestClient.RetryLogger {

    private static final Logger LOG = LoggerFactory.getLogger(RetryLogger.class);

    @Override
    public void onRetryLog(Throwable e, long retryCount, long retrySleepTime) {
      // Log the exception and retry details
      LOG.warn(
        "Retry attempt #" + retryCount + " failed. " +
        "Exception: " + e.getMessage() + ". " +
        "Sleeping for " + retrySleepTime + "ms before next attempt."
      );
    }
  }

  public static TableTunnel getTableTunnel(Odps odps, MaxComputeSinkConnectorConfig config) {
    com.aliyun.odps.tunnel.Configuration configuration = com.aliyun.odps.tunnel.Configuration.builder(odps)
      .withRetryLogger(new RetryLogger())
      .withCompressOptions(new CompressOption())
      .build();
    TableTunnel tunnel = new TableTunnel(odps, configuration);
    if (!StringUtils.isNullOrEmpty(config.getString(
      MaxComputeSinkConnectorConfig.BaseParameter.TUNNEL_ENDPOINT.getName()))) {
      tunnel.setEndpoint(config.getString(
        MaxComputeSinkConnectorConfig.BaseParameter.TUNNEL_ENDPOINT.getName()));
    }
    return tunnel;
  }
}
