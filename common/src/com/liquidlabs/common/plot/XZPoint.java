package com.liquidlabs.common.plot;

public class XZPoint<T> {


	private T z;
	private long timeMs;

	public  XZPoint(long timeMs, T z) {
		this.z = z;
		this.timeMs = timeMs;
	}
	public XZPoint(XZPoint<T> clone) {
		this(clone.getTimeMs(), clone.getZ());
	}

	public T getZ() {
		return z;
	}

	public long getTimeMs() {
		return timeMs;
	}
	public void setTimeMs(long time) {
		this.timeMs = time;
		
	}
	public void setY(T z) {
		this.z = z;
	}
	public int compare(XZPoint<T> o2) {
		return Long.valueOf(timeMs).compareTo(o2.timeMs);
	}

}
