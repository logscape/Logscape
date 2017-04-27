package com.logscape.meter;

import com.liquidlabs.logserver.LogServer;
import com.liquidlabs.transport.proxy.clientHandlers.Cacheable;
import com.liquidlabs.transport.proxy.clientHandlers.HashableAddressByParam;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 24/10/2014
 * Time: 09:40
 * To change this template use File | Settings | File Templates.
 */
public interface MeterService  {
    void flush();

    void start();

    void stop();

    @HashableAddressByParam
    boolean handle(String securityToken, String fromHost, String filePath, String... message);

    int totalAccounts();

    int activeAccounts();

    int unknownHostCount();

    int totalAccountsNearQuota();

    boolean deleteAccount(String id);

    String addIp(String id, String ip);

    String createAccount(String id, String hostsList, int dailyMb, boolean overwrite, String securityToken, int dataRetentionDays);

    String setQuota(String id, long dailyMb);

    Meter get(String id);

    long eventsToday();

    long bytesToday();

    List<Meter> getAccounts(String filter);

    String getLastMiss();

    String getLastActiveId();


    void deleteInactiveData(String userId);

    Meter activate(String token);

    Meter addHostsList(String userId, String newHosts);

    @Cacheable(ttl=10)
    String getToken(String sourceHost, String destFile);

    LogServer logServer();

    Meter activateByUserId(String id);

    Meter deActivateByUserId(String id);

}