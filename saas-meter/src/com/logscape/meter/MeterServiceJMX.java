package com.logscape.meter;

import com.liquidlabs.admin.AdminSpace;
import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.file.FileUtil;
import org.apache.log4j.Logger;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 22/10/2014
 * Time: 14:36
 * To change this template use File | Settings | File Templates.
 */
public class MeterServiceJMX implements MeterServiceJMXMBean {
    private static final Logger LOGGER = Logger.getLogger(MeterServiceJMX.class);
    private MeterService meterService;
    private AdminSpace adminSpace;

    public MeterServiceJMX(MeterService meterService, AdminSpace adminSpace) {
        this.adminSpace = adminSpace;
        LOGGER.info("Registering");
        this.meterService = meterService;
        try {
            MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            ObjectName objectName = new ObjectName("com.logscape:Service=MeterService");
            if (mbeanServer.isRegistered(objectName)) return;
            mbeanServer.registerMBean(this, objectName);
        } catch (Exception e) {
            LOGGER.error(e);
        }
    }
    @Override
    public long getEventsToday() {
        return meterService.eventsToday();
    }
    public String getLogServerEndpoints() {
        return meterService.logServer().toString();
    }
    @Override
    public double getMBytesToday() {
        return  StringUtil.roundDouble((double) meterService.bytesToday() / (double) FileUtil.MEGABYTES, 2);
    }

    @Override
    public int getTotalActive() {
        return meterService.activeAccounts();
    }
    @Override
    public int getUnknownHostCount() {
        return meterService.unknownHostCount();
    }
    @Override
    public int getTotalAccounts() {
        return meterService.totalAccounts();
    }
    public String getLastMiss() {
        return meterService.getLastMiss();
    }
    @Override
    public String getLastActiveAccountId() {
        return meterService.getLastActiveId();
    }

    @Override
    public int getTotalAccountsNearQuota95Pc() {
        return meterService.totalAccountsNearQuota();
    }
    @Override
    public String deleteMeter(String id) {
        return "Deleted:" + id + " count:" + meterService.deleteAccount(id);
    }
    @Override
    public String createMeter_USERID_HOSTS_DAILYMB_OVERWRITE_SEcTOKEnOPTIONAL_RETENTION(String id, String hostsList, int dailyMb, boolean overwrite, String securityTokenOptional, int dataRetentionDays) {
        return meterService.createAccount(id, hostsList, dailyMb, overwrite, securityTokenOptional, dataRetentionDays);
    }

    @Override
    public String setQuotaDailyMb(String id, int dailyMb) {
        return meterService.setQuota(id, dailyMb);
    }

    @Override
    public String getMeter(String id) {
        return meterService.get(id).toString();
    }

    public String addIpToAccount(String id, String ip) {
        meterService.addIp(id, ip);
        return "Updated to:" +  meterService.get(id).toString();
    }

    public String listMeters(String filter) {
        List<Meter> meters =  meterService.getAccounts(filter);
        StringBuilder results = new StringBuilder();
        for (int i = 0; i < meters.size(); i++) {
            results.append(i).append(") ").append(meters.get(i).toString()).append("<br>");
        }
        return  results.toString();
    }

    public String activateByUserId(String id) {
        Meter meter = meterService.activateByUserId(id);
        return  meter != null ? meter.toString() : "Account not found";
    }

    public String deActivateByUserId(String id) {
        Meter meter = meterService.deActivateByUserId(id);
        return  meter != null ? meter.toString() : "Account not found";
    }
    public String  emailAccountActivation(String id) {
        Meter meter = meterService.get(id);
        String url = "cloud.logscape.com";
        adminSpace.sendEmail(SAASProperties.getActivationEmailFrom(), Arrays.asList(id), "Logscape Cloud Activation","<html>Click on the following link to activate your Account: http://"+url +"/saas-portal/rest/user/activate?token=" + meter.getSecurityToken() + "</html>");
        return "Email sent to:" + id;

    }
}
