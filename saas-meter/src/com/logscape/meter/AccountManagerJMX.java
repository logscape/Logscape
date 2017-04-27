package com.logscape.meter;

import org.apache.log4j.Logger;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 22/10/2014
 * Time: 14:36
 * To change this template use File | Settings | File Templates.
 */
public class AccountManagerJMX implements AccountManagerJMXMBean {
    private static final Logger LOGGER = Logger.getLogger(AccountManagerJMX.class);
    private AccountManager accountManager;

    public AccountManagerJMX(AccountManager accountManager) {
        this.accountManager = accountManager;
        LOGGER.info("Registering");
        try {
            MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            ObjectName objectName = new ObjectName("com.logscape:Service=" + AccountManager.class.getSimpleName());
            if (mbeanServer.isRegistered(objectName)) return;
            mbeanServer.registerMBean(this, objectName);
        } catch (Exception e) {
            LOGGER.error(e);
        }
    }

    public void setStatus(String status) {
        accountManager.setStatus(status);
    }

    @Override
    public String getStatus() {
        LOGGER.info("GetStatus");
        String status = accountManager.status();
        LOGGER.info("GetStatus:" + status);
        return status;
    }

    public String createUserAccount_EMAIL_PWD_HOSTS_DAILYmB_RetentionDays(String email, String pwd, String hostsList, int dailyMb, int dataRetentionDays) {
        LOGGER.info("createUserAccount_EMAIL_PWD_HOSTS_DAILYmB_RetentionDays:" + email);
         return accountManager.createUserAccount(email, pwd, hostsList, dailyMb, dataRetentionDays);
    }
    public String addIpToAccount(String uid, String ip) {
        LOGGER.info("addIpToAccount:" + uid);
        return accountManager.addIpToAccount(uid, ip);
    }
    public String listAccounts() {
        LOGGER.info("listAccount:");
        return accountManager.listAccounts();
    }

    public String getUserAccountDetails(String userId) {
        LOGGER.info("getUserAccountDetails:" + userId);
        return accountManager.getUserAccount(userId).toString();
    }

    public String deleteUserAccount(String userId) {
        LOGGER.info("deleteUserAccount:" + userId);
        return accountManager.deleteUserAccount(userId);
    }


    public String setSystemProperty(String key, String value) {
        LOGGER.info("setSystemProperty:" + key);
        System.setProperty(key, value);
        return "DONE > System.setProperty(" + key + "," + value + ");";
    }


}
