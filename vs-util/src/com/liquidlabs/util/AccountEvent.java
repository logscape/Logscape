package com.liquidlabs.util;

import com.liquidlabs.orm.Id;

public class AccountEvent {

	@Id
	String UID;
	private String bundleId;
	private int priority;
	private String serviceName;
	private String resourceId;
	private long startTime;
	private long endTime;
	private double costUnit;
	
	public long getEndTime() {
		return endTime;
	}
	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}
	public AccountEvent(String bundleId, int priority, String serviceName, String resourceId, long startTime, double costUnit) {

		this.bundleId = bundleId;
		this.priority = priority;
		this.serviceName = serviceName;
		this.resourceId = resourceId;
		this.startTime = startTime;
		this.costUnit = costUnit;
		this.UID = getKey(bundleId, serviceName, resourceId, startTime);
	}
	public static String getKey(String bundleId, String serviceName, String resourceId, long startTime) {
		return String.format("%s-%s-%s-%d", bundleId, serviceName, resourceId, startTime);
	}
	public AccountEvent() {
	}

	public long getEventTime() {
		return startTime;
	}

	public void setEventTime(long eventTime) {
		this.startTime = eventTime;
	}

	public void setBundleId(String bundleId) {
		this.bundleId = bundleId;
	}

	public void setPriority(int priority) {
		this.priority = priority;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	public void setResourceId(String resourceId) {
		this.resourceId = resourceId;
	}

	public void setTime(long time) {
		this.startTime = time;
	}

	public String getBundleId() {
		return bundleId;
	}

	public int getPriority() {
		return priority;
	}

	public String getServiceName() {
		return serviceName;
	}

	public String getResourceId() {
		return resourceId;
	}

	public long getStartTime() {
		return startTime;
	}

	public String getId() {
		return UID;
	}

	public String getUID() {
		return UID;
	}

	public void setUID(String uid) {
		UID = uid;
	}
	public void stop(long endTime) {
		this.endTime = endTime;
		
	}
	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}
	public double getCostUnit() {
		return costUnit;
	}
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append("[AccountEvent:");
		buffer.append(" UID: ");
		buffer.append(UID);
		buffer.append(" bundleId: ");
		buffer.append(bundleId);
		buffer.append(" priority: ");
		buffer.append(priority);
		buffer.append(" serviceName: ");
		buffer.append(serviceName);
		buffer.append(" resourceId: ");
		buffer.append(resourceId);
		buffer.append(" startTime: ");
		buffer.append(startTime);
		buffer.append(" endTime: ");
		buffer.append(endTime);
		buffer.append(" costUnit: ");
		buffer.append(costUnit);
		buffer.append("]");
		return buffer.toString();
	}
	
}
