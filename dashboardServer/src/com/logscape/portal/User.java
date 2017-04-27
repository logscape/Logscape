package com.logscape.portal;

import org.joda.time.DateTime;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class User {

    private String accountStatus = "InActive";
    private String defaultHost;
    private String id;
    private String created;
    private String securityToken;
    private String hosts;
    private int dailyVolumeMb = 500;
    private int retentionDays = 7;

    public int getDataUsedTodayMb() {
        return dataUsedTodayMb;
    }

    public void setDataUsedTodayMb(int dataUsedTodayMb) {
        this.dataUsedTodayMb = dataUsedTodayMb;
    }

    private int dataUsedTodayMb = 7;

    public User() {
    }

    public String getCreated() {
        return created;
    }

    public void setCreated(String created) {
        this.created = created;
    }

    public User(String id, String securityToken, String hosts, int dailyVolumeMb, int retentionDays, int dataUsedTodayMb, boolean isActive) {
        this.dataUsedTodayMb = dataUsedTodayMb;
        this.created = new DateTime().toString();
        this.id = id;
        this.securityToken = securityToken;
        this.defaultHost = hosts;
        this.hosts = hosts;
        this.dailyVolumeMb = dailyVolumeMb;
        this.retentionDays = retentionDays;
        this.accountStatus = isActive ? "Active" : "Pending / Waiting for email verification";
    }

    public String getId() {
        return id;
    }
    public String getSecurityToken() {
        return  this.securityToken;
    }
    public String getHosts() {
        return this.hosts;
    }
    public void setHosts(String hosts) {
        this.hosts = hosts;
    }
    public void setId(String id) {
        this.id = id;
    }
    public void setSecurityToken(String token) {
        this.securityToken = token;
    }

    public int getDailyVolumeMb() {
        return dailyVolumeMb;
    }

    public void setDailyVolumeMb(int dailyVolumeMb) {
        this.dailyVolumeMb = dailyVolumeMb;
    }

    public int getRetentionDays() {
        return retentionDays;
    }

    public void setRetentionDays(int retentionDays) {
        this.retentionDays = retentionDays;
    }

    public String getDefaultHost() {
        return defaultHost;
    }

    public void setDefaultHost(String defaultHost) {
        this.defaultHost = defaultHost;
    }

    public String getAccountStatus() {
        return accountStatus;
    }

    public void setAccountStatus(String accountStatus) {
        this.accountStatus = accountStatus;
    }
}