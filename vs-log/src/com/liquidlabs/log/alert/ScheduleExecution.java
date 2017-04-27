package com.liquidlabs.log.alert;

import org.joda.time.DateTimeUtils;

import com.liquidlabs.orm.Id;

public class ScheduleExecution {
	@Id
	private String name;
	
	private long lastRunMs;
	
	private long lastTriggerMs;
	
	public ScheduleExecution() {
	}

	public ScheduleExecution(String name) {
		this.name = name;
	}
	
	public long getLastRunMs() {
		return lastRunMs;
	}

	public void setLastRunMs(long lastRunMs) {
		this.lastRunMs = lastRunMs;
	}

	public long getLastTriggerMs() {
		return lastTriggerMs;
	}

	public void setLastTriggerMs(long lastTriggerMs) {
		this.lastTriggerMs = lastTriggerMs;
	}

	public void updateLastRun() {
		lastRunMs = DateTimeUtils.currentTimeMillis();
	}
	
	public void updateLastTrigger() {
		lastTriggerMs = DateTimeUtils.currentTimeMillis();		
	}
	
	@Override
	public String toString() {
		return super.toString() + " name:" + name;
	}

}
