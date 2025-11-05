package com.aliyun.odps.kafka.connect.account;

import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.auth.credentials.Credential;
import com.aliyun.auth.credentials.ICredential;
import com.aliyun.auth.credentials.exception.CredentialException;
import com.aliyun.auth.credentials.provider.ICredentialProvider;
import com.aliyun.odps.kafka.connect.MaxComputeSinkConnectorConfig;
import com.aliyun.odps.kafka.connect.account.sts.StsService;
import com.aliyun.odps.kafka.connect.account.sts.StsUserBo;

/**
 * A credential provider that retrieves temporary credentials from Aliyun STS (Security Token Service).
 * <p>
 * This provider implements caching and automatic refresh logic to avoid excessive API calls.
 * The STS token, typically valid for 60 minutes, is cached and refreshed proactively
 * before it expires. This implementation is thread-safe.
 * </p>
 *
 */
public class StsCredentialsProvider implements ICredentialProvider {

  private static final Logger log = LoggerFactory.getLogger(StsCredentialsProvider.class);

  // STS Token默认有效期为1小时（3600秒）
  private static final long STS_TOKEN_DURATION_SECONDS = 3600;
  // 在Token过期前5分钟开始尝试刷新，作为安全缓冲
  private static final long REFRESH_BUFFER_MS = TimeUnit.MINUTES.toMillis(5);

  private final StsService stsService = new StsService();
  private final MaxComputeSinkConnectorConfig config;

  private volatile ICredential cachedCredential;
  private volatile long expirationTimeMillis;

  // 用于同步刷新操作的锁对象
  private final Object lock = new Object();

  public StsCredentialsProvider(MaxComputeSinkConnectorConfig config) {
    this.config = config;
  }

  @Override
  public ICredential getCredentials() throws CredentialException {
    // 性能优化：首先在无锁状态下检查凭证是否有效
    // 绝大多数情况下，这个检查会通过，直接返回缓存的凭证，避免了锁竞争
    if (cachedCredential != null && System.currentTimeMillis() < expirationTimeMillis) {
      return cachedCredential;
    }

    // 如果凭证为null或已过期，则进入同步块
    synchronized (lock) {
      // 双重检查锁定（DCL）：再次检查凭证状态
      // 防止在等待锁的过程中，其他线程已经完成了刷新操作
      if (cachedCredential == null || System.currentTimeMillis() >= expirationTimeMillis) {
        log.info("STS credentials have expired or are not present. Refreshing now...");
        fetchAndCacheNewCredentials();
      }
    }
    return cachedCredential;
  }

  /**
   * Fetches new credentials from the STS service and updates the cache.
   * This method should only be called within a synchronized block.
   */
  private void fetchAndCacheNewCredentials() throws CredentialException {
    try {
      String ak = config.getString(MaxComputeSinkConnectorConfig.BaseParameter.ACCESS_ID.getName());
      String sk = config.getString(MaxComputeSinkConnectorConfig.BaseParameter.ACCESS_KEY.getName());

      // 如果配置中没有，则从环境变量中获取主账号AK/SK
      if (ak == null || ak.isEmpty() || sk == null || sk.isEmpty()) {
        log.debug("AccessKeyId/Secret not found in config, falling back to environment variables.");
        Map<String, String> env = System.getenv();
        ak = env.get("ALIBABA_CLOUD_ACCESS_KEY_ID");
        sk = env.get("ALIBABA_CLOUD_ACCESS_KEY_SECRET");
      }

      if (ak == null || ak.isEmpty() || sk == null || sk.isEmpty()) {
        throw new CredentialException("Cannot get Aliyun Access Key ID/Secret from config or environment variables.");
      }

      String accountId = config.getString(MaxComputeSinkConnectorConfig.BaseParameter.ACCOUNT_ID.getName());
      String regionId = config.getString(MaxComputeSinkConnectorConfig.BaseParameter.REGION_ID.getName());
      String roleName = config.getString(MaxComputeSinkConnectorConfig.BaseParameter.ROLE_NAME.getName());
      String stsEndpoint = config.getString(MaxComputeSinkConnectorConfig.BaseParameter.STS_ENDPOINT.getName());

      // 调用STS服务获取临时凭证
      StsUserBo stsUserBo = stsService.getAssumeRole(accountId, regionId, stsEndpoint, ak, sk, roleName);

      // 构建新的凭证对象
      this.cachedCredential = Credential.builder()
        .accessKeyId(stsUserBo.getAk())
        .accessKeySecret(stsUserBo.getSk())
        .securityToken(stsUserBo.getToken())
        .build();

      // 计算并设置下一次需要刷新的时间点
      // 注意：AssumeRole返回的Token有效期是固定的，这里我们假设为1小时
      // 如果StsUserBo能返回实际的过期时间，使用那个会更精确
      long now = System.currentTimeMillis();
      this.expirationTimeMillis = now + TimeUnit.SECONDS.toMillis(STS_TOKEN_DURATION_SECONDS) - REFRESH_BUFFER_MS;

      log.info("Successfully refreshed STS credentials. Next refresh will be around {}.",
               Date.from(Instant.ofEpochMilli(this.expirationTimeMillis)));

    } catch (Exception e) {
      // 包装成 CredentialException 抛出，符合接口规范
      throw new CredentialException("Failed to assume STS role and get temporary credentials.", e);
    }
  }

  @Override
  public void close() {
  }
}
