/**
 * 
 */
package com.liquidlabs.log.roll;

import java.util.Set;

public interface RolledFileSorter {
	String[] sortedFileNames(boolean pause, String logCanonical, String logName, String parentName, String parentCanonical, Set<String> avoidTheFiles, long lastPos, boolean verbose, String firstFileLine) throws InterruptedException;

	String getFormat();

	void setFormat(String timeFormat);

	void setFilenamePatterns(String[] patterns);

	String getType();

}