package com.liquidlabs.log.index;

public class LineKey {
	
	public int logId;
	public int number;
	
	public LineKey() {
	}
	
	public LineKey(int logId, int number) {
		this.logId = logId;
		this.number = number;
	}
	
	public String toString() {
		StringBuilder buffer = new StringBuilder();
		buffer.append("[LineKey:");
		buffer.append(" logId:");
		buffer.append(logId);
		buffer.append(" number:");
		buffer.append(number);
		buffer.append("]");
		return buffer.toString();
	}

	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null) {
			return false;
		}
		if (o.getClass() != getClass()) {
			return false;
		}
		LineKey castedObj = (LineKey) o;
		return ((this.logId == castedObj.logId) && (this.number == castedObj.number));
	}

	public int hashCode() {
		int hashCode = 1;
		hashCode = 31 * hashCode + (int) (+logId ^ (logId >>> 32));
		hashCode = 31 * hashCode + number;
		return hashCode;
	}
	
}
