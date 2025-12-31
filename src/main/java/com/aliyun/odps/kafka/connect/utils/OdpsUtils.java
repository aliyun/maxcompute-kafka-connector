package com.aliyun.odps.kafka.connect.utils;

import java.lang.reflect.Field;

import com.aliyun.credentials.Client;
import com.aliyun.credentials.models.Config;
import com.aliyun.credentials.provider.AlibabaCloudCredentialsProvider;
import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.Account.AccountProvider;
import com.aliyun.odps.account.AklessAccount;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.kafka.connect.ConnectorConfig;
import com.aliyun.odps.kafka.connect.account.StsCredentialsProvider;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.CompressOption;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.aliyun.odps.kafka.connect.ConfigParameter.ACCESS_ID;
import static com.aliyun.odps.kafka.connect.ConfigParameter.ACCESS_KEY;
import static com.aliyun.odps.kafka.connect.ConfigParameter.ACCOUNT_TYPE;
import static com.aliyun.odps.kafka.connect.ConfigParameter.MAXCOMPUTE_ENDPOINT;
import static com.aliyun.odps.kafka.connect.ConfigParameter.MAXCOMPUTE_PROJECT;
import static com.aliyun.odps.kafka.connect.ConfigParameter.MAXCOMPUTE_SCHEMA;
import static com.aliyun.odps.kafka.connect.ConfigParameter.TUNNEL_ENDPOINT;

public class OdpsUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(OdpsUtils.class);

    public static Odps getOdps(ConnectorConfig config) {
        String accountType = ACCOUNT_TYPE.getString(config);
        AccountProvider provider = AccountProvider.valueOf(org.apache.commons.lang.StringUtils.upperCase(accountType));
        Account account;

        switch (provider) {
            case STS:
                account = new AklessAccount(new StsCredentialsProvider(config));
                break;
            case ALIYUN:
                String accessId = ACCESS_ID.getString(config);
                String accessKey = ACCESS_KEY.getString(config);
                account = new AliyunAccount(accessId, accessKey);
                break;
            default:
                LOGGER.info("use akLess account to get credentials");
                Config credencialConfig = Config.build(config.getConfigMap());
                try {
                    Client client = new Client(credencialConfig);
                    Field field = Client.class.getDeclaredField("credentialsProvider");
                    field.setAccessible(true); // 破除 private 限制
                    account = new AklessAccount((AlibabaCloudCredentialsProvider)field.get(client));
                } catch (Exception e) {
                    LOGGER.error("get akLess account failed!", e);
                    throw new RuntimeException(e);
                }
        }

        Odps odps = new Odps(account);
        String endpoint = MAXCOMPUTE_ENDPOINT.getString(config);
        String project = MAXCOMPUTE_PROJECT.getString(config);
        odps.setDefaultProject(project);

        String schema = MAXCOMPUTE_SCHEMA.getString(config);
        if (org.apache.commons.lang.StringUtils.isNotEmpty(schema)) {
            odps.setCurrentSchema(schema);
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
            LOG.warn("Retry attempt #{} failed. Exception: {}. Sleeping for {}ms before next attempt.", retryCount,
                e.getMessage(), retrySleepTime);
        }
    }

    public static TableTunnel getTableTunnel(Odps odps, ConnectorConfig config) {
        com.aliyun.odps.tunnel.Configuration configuration = com.aliyun.odps.tunnel.Configuration.builder(odps)
            .withRetryLogger(new RetryLogger())
            .withCompressOptions(new CompressOption())
            .build();
        TableTunnel tunnel = new TableTunnel(odps, configuration);
        String endpoint = TUNNEL_ENDPOINT.getString(config);
        if (StringUtils.isNotEmpty(endpoint)) {
            tunnel.setEndpoint(endpoint);
        }
        return tunnel;
    }
}
