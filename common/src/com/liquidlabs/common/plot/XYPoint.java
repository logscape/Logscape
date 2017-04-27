package com.liquidlabs.common.plot;

public class XYPoint {

	private String time;
	public void setTime(String time) {
		this.time = time;
	}
	private long timeMs;
	private long y;

	public XYPoint(){
	}
	public XYPoint(long timeMs, long y) {
		this.y = y;
		this.timeMs = timeMs;
	}
	public XYPoint(String time, long timeMs) {
		this.time = time;
		this.timeMs = timeMs;
	}
	public XYPoint(String time, long timeMs, long y) {
		this.time = time;
		this.y = y;
		this.timeMs = timeMs;
	}
	public XYPoint(XYPoint clone) {
		this(clone.getTimeMs(), clone.getY());
	}
	public String getTime() {
		return time;
	}
	public long getY() {
		return y;
	}
	public long getTimeMs() {
		return timeMs;
	}
	public void setTimeMs(long time) {
		this.timeMs = time;
		
	}
	public void setY(long y) {
		this.y = y;
	}
	
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append("[XYPoint");
		buffer.append(" time:");
		buffer.append(time);
		buffer.append(" timeMs:");
		buffer.append(timeMs);
		buffer.append(" y:");
		buffer.append(y);
		buffer.append("]");
		return buffer.toString();
	}
	
	

}
