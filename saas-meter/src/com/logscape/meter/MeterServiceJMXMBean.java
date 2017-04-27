package com.logscape.meter;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 22/10/2014
 * Time: 14:37
 * To change this template use File | Settings | File Templates.
 */
public interface MeterServiceJMXMBean {
    int getTotalAccounts();

    int getTotalActive();

    int getUnknownHostCount();

    long getEventsToday();

    String getLogServerEndpoints();

    String getLastMiss();

    int getTotalAccountsNearQuota95Pc();

    String deleteMeter(String id);


    String createMeter_USERID_HOSTS_DAILYMB_OVERWRITE_SEcTOKEnOPTIONAL_RETENTION(String id, String hostsList, int dailyMb, boolean overwrite, String securityTokenOptional, int dataRetentionDays);

    String activateByUserId(String userId);

    String deActivateByUserId(String userId);

    String setQuotaDailyMb(String id, int dailyMb);

    String getMeter(String id);

    String addIpToAccount(String id, String ip);

    String getLastActiveAccountId();

    String emailAccountActivation(String id);

    double getMBytesToday();

    String listMeters(String filter);

}
