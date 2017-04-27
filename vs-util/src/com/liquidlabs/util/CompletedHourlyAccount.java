package com.liquidlabs.util;

import com.liquidlabs.orm.Id;

/**
 * Represents a single hours running cost - audit history
 */
public class CompletedHourlyAccount {

    @Id
    String UID;

    private String date;

    private String bundleId;

    private int units;

    public CompletedHourlyAccount(String bundleId, String date) {
        this.bundleId = bundleId;
        this.date = date;
        this.UID = bundleId + "_" + date;
    }

    public CompletedHourlyAccount() {
    }

    public String getId() {
        return UID;
    }

    public String getDisplayName() {
        return getUID();
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getBundleId() {
        return bundleId;
    }

    public String getUID() {
        return UID;
    }

    public String getDate() {
        return date;
    }

    public String getBundleName() {
        return bundleId;
    }


    public int getUnits() {
        return units;
    }

    public void setUnits(int units) {
        this.units = units;
    }

    public void addUnits(int additionalUnits) {
        this.units += additionalUnits;
    }

}
