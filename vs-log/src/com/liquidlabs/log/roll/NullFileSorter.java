package com.liquidlabs.log.roll;

import java.util.Set;


/**
 *
 */
public class NullFileSorter implements RolledFileSorter {
	private String timeFormat;
	
	public NullFileSorter() {
	}
	
	public String getType() {
		return this.getClass().getSimpleName();
	}
	
	public NullFileSorter(String timeFormat) {
		this.timeFormat = timeFormat;
	}
	
	public String[] sortedFileNames(boolean sss,String logCanonical, String logName,
			String parentName, String parentCanonical,
			Set<String> avoidTheFiles, long lastPos, boolean verbose, String firstFileLine) {
		return new String [0];
	}

	public String getFormat() {
		return timeFormat;
	}

	public void setFormat(String timeFormat) {
	}

	public String toString() {
		StringBuilder buffer = new StringBuilder();
		buffer.append("[NullFileSorter:");
		buffer.append(" timeFormat:");
		buffer.append(timeFormat);
		buffer.append("]");
		return buffer.toString();
	}

	public void setFilenamePatterns(String[] filePattern) {
	}

}
