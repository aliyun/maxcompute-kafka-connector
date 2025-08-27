package com.aliyun.odps.kafka.connect.account.sts;

import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.auth.sts.AssumeRoleRequest;
import com.aliyuncs.auth.sts.AssumeRoleResponse;
import com.aliyuncs.http.MethodType;
import com.aliyuncs.http.ProtocolType;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.profile.IClientProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StsService {
    private static final Logger LOGGER = LoggerFactory.getLogger(StsService.class);

    /**
     * we assume others by ownId
     */
    public StsUserBo getAssumeRole(String ownId, String regionId, String stsEndpoint, String ak, String sk,
                                   String roleName) {

        StsUserBo stsUserBo = new StsUserBo();
        try {
            IClientProfile profile = DefaultProfile.getProfile(regionId, ak, sk);
            DefaultAcsClient client = new DefaultAcsClient(profile);

            AssumeRoleRequest request = new AssumeRoleRequest();

            request.setRoleSessionName("kafka-session-" + ownId);
            request.setMethod(MethodType.POST);
            request.setProtocol(ProtocolType.HTTPS);
            request.setEndpoint(stsEndpoint);

            request.setRoleArn(buildRoleArn(ownId, roleName));
            request.setActionName("AssumeRole");
            request.putQueryParameter("AssumeRoleFor", ownId);
            request.setDurationSeconds(1 * 60 * 60L);

            AssumeRoleResponse response = client.getAcsResponse(request);
            String requestId = response.getRequestId();

            String userAk = response.getCredentials().getAccessKeyId();
            String userSk = response.getCredentials().getAccessKeySecret();
            String token = response.getCredentials().getSecurityToken();

            LOGGER.info("Fetch sts token success, request: {}, stsAk: {}", requestId, userAk);

            stsUserBo.setAk(userAk);
            stsUserBo.setSk(userSk);
            stsUserBo.setToken(token);
            stsUserBo.setOwnId(ownId);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return stsUserBo;
    }

    private String buildRoleArn(String uid, String roleName) {
        return String.format("acs:ram::%s:role/%s", uid, roleName);
    }
}
