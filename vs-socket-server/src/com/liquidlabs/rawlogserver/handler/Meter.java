package com.liquidlabs.rawlogserver.handler;

import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.UID;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.orm.Id;
import org.joda.time.DateTime;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 22/10/2014
 * Time: 14:09
 * To change this template use File | Settings | File Templates.
 */
public class Meter {

    // natural key, user email/id
    @Id
    String id;
    private String hostsList;
    private long quotaPerDayBytes = FileUtil.MEGABYTES * 200;
    // policy built into this users datasource
    private int retentionDays = 5;
    private long lastActivity = 0;
    private int quotaDate = 0;
    private int dailyEventCount = 0;
    private long lastDailyVolumeBytes = 0;
    private long createdDate = 0;
    private String securityToken;
    public boolean active = false;
    public static String LS = "LOGSCAPETOKEN";
    public static String TAG = "LOGSCAPETAG";

    public Meter() {
    }

    public Meter(String id, String hostsList, int dailyMb) {
        this.id = id;
        this.hostsList = hostsList;
        this.quotaPerDayBytes = dailyMb * FileUtil.MEGABYTES;
        this.securityToken = UID.getUUID();
        this.createdDate = System.currentTimeMillis();
    }

    public boolean isWithinQuota(int length) {
        lastActivity = System.currentTimeMillis();
        if (lastDailyVolumeBytes + length > quotaPerDayBytes) return false;
        else {
            dailyEventCount++;
            lastDailyVolumeBytes += length;
            return true;
        }
    }


    public boolean isNearQuota() {
        return lastDailyVolumeBytes > (quotaPerDayBytes * 0.95);
    }
    public String hostsString() {
        return hostsList;
    }

    public int getRetentionDays() {
        return retentionDays;
    }

    public double getDailyQuotaMBytes() {
        return   StringUtil.roundDouble((double)quotaPerDayBytes/(double)FileUtil.MEGABYTES, 2);
    }

    public String toString() {
        return "Meter id:" + this.id + " active:" + active + " hosts:" + hostsList + " quotaMb:" + quotaPerDayBytes /FileUtil.MEGABYTES + " todayEvents:" + this.dailyEventCount + " Created:" + new DateTime(this.createdDate) + " last:" + new DateTime(lastActivity) + " retentionDays:" + retentionDays + " TodayVolumeMb:" + getDailyMBytes() + " Token:" + this.securityToken;
    }

    public void checkQuotaTimestamp() {
        int dayOfYear = new DateTime().getDayOfYear();
        if (dayOfYear != quotaDate) {
            quotaDate = dayOfYear;
            lastDailyVolumeBytes  = 0;
            dailyEventCount = 0;
        }

    }
    public List<String> hosts() {
        return Arrays.asList(this.hostsList.split(","));

    }

    public Meter addIp(String ip) {

        String[] split = (this.hostsList.replace(" ","") + "," + ip.replace(" ","")).split(",");

        Set<String> ips = new HashSet<String>();
        for (String s : split) {
            s = s.trim();
            if (!ips.contains(s)) ips.add(s);
        }
        this.hostsList = ips.toString().replace("[","").replace("]","");
        return this;
    }


    public int getDailyEventCount(){
        return this.dailyEventCount;
    }

    public long getDailyBytes() {
        return lastDailyVolumeBytes;
    }
    public double getDailyMBytes() {
        return  lastDailyVolumeBytes /FileUtil.MEGABYTES;
    }

    public static String extractToken(String lineData) {
        int index= lineData.indexOf(LS);
        if (index != -1) {
            int to = lineData.indexOf(" ", index);
            if (to == -1) return lineData.substring(index + LS.length()+1);
            else return lineData.substring(index + LS.length()+1, to);
        } else return "";
    }
    public static String extractTag(String lineData) {
        int index= lineData.indexOf(TAG);
        if (index != -1) {
            int to = lineData.indexOf(" ", index);
            if (to == -1) return lineData.substring(index + TAG.length()+1);
            return lineData.substring(index + TAG.length()+1, to);

        } else return "";
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getHostsList() {
        return hostsList;
    }

    public void setHostsList(String hostsList) {
        this.hostsList = hostsList;
    }

    public long getQuotaPerDayBytes() {
        return quotaPerDayBytes;
    }

    public void setQuotaPerDayBytes(long quotaPerDayBytes) {
        this.quotaPerDayBytes = quotaPerDayBytes;
    }

    public void setRetentionDays(int retentionDays) {
        this.retentionDays = retentionDays;
    }

    public long getLastActivity() {
        return lastActivity;
    }

    public void setLastActivity(long lastActivity) {
        this.lastActivity = lastActivity;
    }

    public int getQuotaDate() {
        return quotaDate;
    }

    public void setQuotaDate(int quotaDate) {
        this.quotaDate = quotaDate;
    }

    public void setDailyEventCount(int dailyEventCount) {
        this.dailyEventCount = dailyEventCount;
    }

    public long getLastDailyVolumeBytes() {
        return lastDailyVolumeBytes;
    }

    public void setLastDailyVolumeBytes(long lastDailyVolumeBytes) {
        this.lastDailyVolumeBytes = lastDailyVolumeBytes;
    }

    public String getSecurityToken() {
        return securityToken;
    }

    public void setSecurityToken(String securityToken) {
        this.securityToken = securityToken;
    }

    public long getCreatedDate() {
        return createdDate;
    }

    public void setCreatedDate(long createdDate) {
        this.createdDate = createdDate;
    }

    public static String removeTagAndToken(String lineData) {

        int indexOfToken = lineData.indexOf(LS);

        if (indexOfToken != -1) {
            String result = "";
            result += lineData.substring(0, indexOfToken);
            result += lineData.substring(lineData.indexOf(" ", indexOfToken+1)+1);
            lineData = result;
        }
        int indexOfTag = lineData.indexOf(TAG);
        if (indexOfToken != -1) {
            String result = "";
            result += lineData.substring(0, indexOfTag);
            result += lineData.substring(lineData.indexOf(" ", indexOfToken+1)+1);
            lineData = result;
        }
        return lineData;

    }
}
