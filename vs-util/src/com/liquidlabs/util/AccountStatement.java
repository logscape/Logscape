package com.liquidlabs.util;

import java.util.HashMap;
import java.util.Map;

import com.liquidlabs.common.plot.XYPoint;


/**
 * Represent a total outstanding value of unit costs against a bundle
 *
 */
public class AccountStatement {

	private String bundleId;
	private int completedUnitCosts;
	private int runningAccounts;
	private int runningUnitCosts;
	private long timeMs;
	Map<String, Long> costPerService = new HashMap<String, Long>();
	Map<String, Long> countPerService = new HashMap<String, Long>();
	
	public String getId(){
		return bundleId;
	}

	public AccountStatement() {
	}
	public AccountStatement(String bundleId, long timeMs, int completedUnitCosts, int runningAccounts, int runningUnitCosts) {
				this.bundleId = bundleId;
				this.completedUnitCosts = completedUnitCosts;
				this.runningAccounts = runningAccounts;
				this.runningUnitCosts = runningUnitCosts;
				this.timeMs = timeMs;
	}

	public String getBundleId() {
		return bundleId;
	}


	public int getCompletedUnitCosts() {
		return completedUnitCosts;
	}

	public int getRunningAccounts() {
		return runningAccounts;
	}

	public int getRunningUnitCosts() {
		return runningUnitCosts;
	}
	public String toString() {
		return this.getClass().getSimpleName() + "-" + bundleId + ", completed[" + this.completedUnitCosts + "] running[" + this.runningAccounts + "] value[" + this.runningUnitCosts + "]";
	}

	public void setCompletedUnitCosts(int newValue) {
		this.completedUnitCosts = newValue;
	}

	public void setTime(long timeMs) {
		this.timeMs = timeMs;
	}
	public long getTimeMs() {
		return timeMs;
	}

	public void setCost(String serviceId, long cost) {
		costPerService.put(serviceId, cost);
	}

	public Map<String, Long> getServiceCosts() {
		return costPerService;
		
	}

	public void setCount(String serviceId, long count) {
		countPerService.put(serviceId, count);
	}

	public Map<String, Long> getServiceCount() {
		return countPerService;
	}

}
