package com.liquidlabs.vso;

import org.joda.time.DateTimeUtils;
import org.joda.time.format.DateTimeFormat;

import com.liquidlabs.common.LifeCycle;

public class ServiceStatus {
	LifeCycle.State state = LifeCycle.State.RUNNING;
	
	String msg = "";
	
	
	private long lastRunningTime = 0;
	private long lastWarningTime = 0;
	private long lastErrorTime = 0;
	
	private long runningCount = 0;
	private long warningCount = 0;
	private long errorCount = 0;
	
	public ServiceStatus(){
	}
	public ServiceStatus(long time) {
		this.setRunningTime(time);
		this.setWarningTime(time);
		this.setErrorTime(time);
	}

	public void setError(String string) {
		errorCount++;
		this.lastErrorTime = System.currentTimeMillis();
		set(LifeCycle.State.ERROR, getNowString() + " " + string);
	}
	public void setWarning(String string) {
		this.warningCount++;
		this.lastWarningTime = System.currentTimeMillis();
		set(LifeCycle.State.WARNING, getNowString() + " " + string);
	}
	public void setRunning(String string) {
		this.runningCount++;
		this.lastRunningTime = System.currentTimeMillis();
		set(LifeCycle.State.RUNNING, string);
	}
	public void set(LifeCycle.State status, String msg){
		this.state = status;
		this.msg = msg;
	}
	public LifeCycle.State status() {
		return state;
	}
	public String msg(){
		return msg;
	}

	public void setRunningTime(long time) {
		this.lastRunningTime = time;
	}
	public void setErrorTime(long time) {
		this.lastErrorTime = time;
	}
	public void setWarningTime(long time) {
		this.lastWarningTime = time;
	}

	public long getLastRunningTime() {
		return lastRunningTime;
	}
	public long getLastWarningTime() {
		return lastWarningTime;
	}
	public void setLastAmberTime(long lastWarnTime) {
		this.lastWarningTime = lastWarnTime;
	}
	public long getLastErrorTime() {
		return lastErrorTime;
	}
	public void setLastErrorTime(long lastErrorTime) {
		this.lastErrorTime = lastErrorTime;
	}
	public String toString(){
		return "status:" + state + " msg:" + msg;
	}
	
	public boolean isWarning() {
		return this.state.equals(LifeCycle.State.WARNING);
	}
	public boolean isError() {
		return this.state.equals(LifeCycle.State.ERROR);
	}
	private String getNowString() {
		String now = DateTimeFormat.shortDateTime().print(DateTimeUtils.currentTimeMillis());
		return now;
	}
	
	public String toLongString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append("[ServiceStatus:");
		buffer.append(" state: ");
		buffer.append(state);
		buffer.append(" msg: ");
		buffer.append(msg);
		buffer.append(" lastRunningTime: ");
		buffer.append(DateTimeFormat.shortDateTime().print(lastRunningTime));
		buffer.append(" runningCount: ");
		buffer.append(runningCount);
		buffer.append(" lastWarningTime: ");
		buffer.append(DateTimeFormat.shortDateTime().print(lastWarningTime));
		buffer.append(" warningCount: ");
		buffer.append(warningCount);
		buffer.append(" lastErrorTime: ");
		buffer.append(DateTimeFormat.shortDateTime().print(lastErrorTime));
		buffer.append(" errorCount: ");
		buffer.append(errorCount);
		buffer.append("]");
		return buffer.toString();
	}

}
