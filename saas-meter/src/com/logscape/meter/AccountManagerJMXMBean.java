package com.logscape.meter;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 22/10/2014
 * Time: 14:37
 * To change this template use File | Settings | File Templates.
 */
public interface AccountManagerJMXMBean {

    void setStatus(String status);
    String getStatus();

    String createUserAccount_EMAIL_PWD_HOSTS_DAILYmB_RetentionDays(String email, String pwd, String hostsList, int dailyMb, int dataRetentionDays);

    String getUserAccountDetails(String userId);

    String deleteUserAccount(String userId);

    String addIpToAccount(String uid, String ip);

    String listAccounts();

    String setSystemProperty(String key, String value);
}
