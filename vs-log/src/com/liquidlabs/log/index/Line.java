package com.liquidlabs.log.index;

import java.io.Serializable;

import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.Days;

import com.liquidlabs.common.DateUtil;

public class Line implements Serializable, Comparable<Line>{

	private static final long serialVersionUID = 1L;
	public LineKey pk;
	
	public Long time;
	
	// only needs to be LONG (9,223,372,036,854,775,807) - INT is limited to files of 2GB (2,147,483,647)
	public Long filePos;
	
	public Line() {};
	public Line(int logId, int number, long time, long filePos) {
		this.pk = new LineKey(logId, number);
		this.time = time;
		this.filePos = filePos;
	}
	public void setPK(int logId) {
		this.pk = new LineKey(logId, this.pk.number);
	}

	public Line(Line line, String fileName) {
		this(line.pk.logId, line.pk.number, line.time, line.filePos);
	}
	final public int number() {
		return pk.number;
	}
	
	final public long time() {
		return time;
	}
	public int compareTo(Line o) {
		int tc = time.compareTo(o.time);
		if (tc != 0) {
			return tc;
		}
		int nc = Integer.valueOf(pk.number).compareTo(o.pk.number);
		if (nc != 0) {
			return nc;
		}
		return filePos.compareTo(o.filePos);
	}

	
	
	public String toString() {
		return String.format("line:%s, time:%d/%s", pk, time, DateUtil.shortDateTimeFormat6.print(time));
	}

	final public long position() {
		return filePos;
	}
	
	public boolean timeBetween(long start, long end) {
		return time >= start && time <= end;
	}
	public void clear() {
		this.pk = null;
		this.filePos = null;
		this.time = null;
	}
	public Line copy(String data) {
		Line line = new Line(this.pk.logId, this.pk.number, this.time, this.position());
		return line;
	}
	
	public int hashCode() {
		int hashCode = 1;
		hashCode = 31 * hashCode + (int) (+serialVersionUID ^ (serialVersionUID >>> 32));
		hashCode = 31 * hashCode + (pk == null ? 0 : pk.hashCode());
		hashCode = 31 * hashCode + (time == null ? 0 : time.hashCode());
		hashCode = 31 * hashCode + (filePos == null ? 0 : filePos.hashCode());
		return hashCode;
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
		Line castedObj = (Line) o;
		return ((this.pk == null ? castedObj.pk == null : this.pk
			.equals(castedObj.pk))
			&& (this.time == null ? castedObj.time == null : this.time
				.equals(castedObj.time))
			&& (this.filePos == null ? castedObj.filePos == null : this.filePos
				.equals(castedObj.filePos))
//				&& (this.lineContent == null ? castedObj.lineContent == null 	: this.lineContent.equals(castedObj.lineContent))
				);
	}
	public void setTime(long time) {
		this.time = time;
	}
	public boolean isOlderThanDays(int maxAgeDays) {
		long now = DateTimeUtils.currentTimeMillis();
		if (now - time < DateUtil.DAY) return false;
		return Days.daysBetween(new DateTime(time), new DateTime(now) ).getDays() > maxAgeDays;
	}
}
