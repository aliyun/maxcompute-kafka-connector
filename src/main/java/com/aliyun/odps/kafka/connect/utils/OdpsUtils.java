package com.aliyun.odps.kafka.connect.utils;

import com.aliyun.credentials.Client;
import com.aliyun.credentials.models.Config;
import com.aliyun.credentials.provider.AlibabaCloudCredentialsProvider;
import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AklessAccount;
import com.aliyun.odps.kafka.connect.MaxComputeSinkConnectorConfig;
import com.aliyun.odps.kafka.connect.account.AccountFactory;
import com.aliyun.odps.kafka.connect.account.AccountGenerator;
import com.aliyun.odps.kafka.connect.account.IAccountFactory;
import com.aliyun.odps.kafka.connect.account.impl.AliyunAccountGenerator;
import com.aliyun.odps.kafka.connect.account.impl.STSAccountGenerator;
import com.aliyun.odps.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;

public class OdpsUtils {

  private static final IAccountFactory<AccountGenerator<?>> accountFactory = new AccountFactory<>();

  private static final Logger LOGGER = LoggerFactory.getLogger(OdpsUtils.class);

  public static Odps getOdps(MaxComputeSinkConnectorConfig config) {
    String
        accountType =
        config.getString(MaxComputeSinkConnectorConfig.BaseParameter.ACCOUNT_TYPE.getName());
    Account account;
    if (accountType.equalsIgnoreCase(Account.AccountProvider.STS.toString())) {
      account = accountFactory.getGenerator(STSAccountGenerator.class).generate(config);
    } else if (accountType.equalsIgnoreCase(Account.AccountProvider.ALIYUN.toString())) {
      account = accountFactory.getGenerator(AliyunAccountGenerator.class).generate(config);
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
}
