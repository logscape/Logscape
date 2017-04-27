package com.liquidlabs.log.roll;

import java.io.File;
import java.util.Set;
import java.util.TreeMap;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


public class TimeBasedSorter implements RolledFileSorter {

	private String timeFormat;

	public TimeBasedSorter() {
	}
	
	public TimeBasedSorter(String timeFormat) {
		this.timeFormat = timeFormat;
	}
	public String getType() {
		return this.getClass().getSimpleName();
	}

	@Override
	public String toString() {
		return this.timeFormat;
	}
	
	public String[] sortedFileNames(boolean pause, String logCanonical, String logName, String parentName, String parentCanonical, Set<String> avoidTheFiles, long lastPos, boolean verbose, String firstFileLine) {
		final DateTimeFormatter formatter = DateTimeFormat.forPattern(timeFormat);
		final TreeMap<Long, String> sorted = new TreeMap<Long, String>();
		String[] files = new File(parentCanonical).list();
		for (String name : files) {
			try {
				String timePart = name.substring(name.length() - timeFormat.length());
				long millis = formatter.parseMillis(timePart);
				boolean isValid = name.startsWith(logName);
				if (isValid) {
					sorted.put(millis, parentCanonical + File.separator + name);
				}
			} catch(Throwable t) {
			}
		}

		try {
			return new String [] { sorted.get(sorted.lastKey())};
		} catch (Throwable t) {
			throw new RuntimeException("boooom: sorted:" + sorted);
		}
	}
	public void setFormat(String timeFormat) {
		this.timeFormat = timeFormat;
	}
	public String getFormat(){
		return timeFormat;
	}

	public void setFilenamePatterns(String[] filePattern) {
	}

}
