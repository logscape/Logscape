package com.liquidlabs.vso.resource;

import com.liquidlabs.orm.Id;

public class Allocation {
	

	public Allocation() {
	}
	public Allocation(String requestId, String resourceId, AllocType type, String owner, int priority, String intent, long timestamp, String timestampString, int timeoutSeconds) {
		this.requestId = requestId;
		this.id = getId(resourceId, type);
		this.resourceId = resourceId;
		this.type = type;
		this.owner = owner;
		this.priority = priority;
		this.workIdIntent = intent;
		this.timestamp = timestamp;
		this.timestampString = timestampString;
		this.timeoutSeconds = timeoutSeconds;
	}
	public String toString() {
		return "\t[Allocation:" + id + " owner:" + owner + " work:" + workIdIntent + " p:" + priority + " requestId:" + requestId + " ]\n";
	}
	/**
	 * id - is actually the resourceId + allocation type - as that serves as a natural PK i.e. someResourcePENDING
	 */
	@Id
	public String id;
	public AllocType type;
	
	public String resourceId;
	public String workIdIntent = "";
	
	public String owner;
	
	public int priority;
	
	public long timestamp;
	public String timestampString;
	public String requestId;
	int timeoutSeconds;
	
	public static String getId(String resourceId, AllocType pending) {
		return resourceId + "-" + pending.name();
	}
	public void update(AllocType type) {
		this.id = getId(resourceId, type);
		this.type = type;
	}
	public String getOwnerId() {
		return owner;
	}
	public String getResourceId() {
		return resourceId;
	}
	public int getPriority() {
		return priority;
	}
	public String getWorkIdIntent() {
		return workIdIntent;
	}
	public String getRequestId() {
		return requestId;
	}
	public String getTimestamp() {
		return timestampString;
	}
	public String getId() {
		return this.id;
	}
	public int getTimeoutSeconds() {
		return timeoutSeconds;
	}

}
