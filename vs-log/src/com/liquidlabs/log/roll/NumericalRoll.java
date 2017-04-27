/**
 * 
 */
package com.liquidlabs.log.roll;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Set;


public class NumericalRoll implements RolledFileSorter {
	
	public String getType() {
		return this.getClass().getSimpleName();
	}

	
	public String[] sortedFileNames(boolean ppp, String logCanonical, final String logName, String parentName, String parentCanonical, Set<String> avoidTheFiles, long lastPos, boolean verbose, String firstFileLine) {
		String[] files = new File(parentCanonical).list(new FilenameFilter() {
			public boolean accept(File file, String filename) {
				String[] parts = filename.split("\\.");
				return filename.startsWith(logName) && parts[parts.length -1].matches("\\d+");
			}});
		if (files == null) {
			return new String[0];
		}
		String [] sorted = new String [files.length];
		for (int i = 0; i < sorted.length; i++) {
			String[] parts = files[i].split("\\.");
			try {
				sorted[Integer.valueOf(parts[parts.length -1]) -1] = parentCanonical + File.separator + files[i];
			} catch (NumberFormatException e) {
			}
		}
		return sorted;
	}

	public String getFormat() {
		return null;
	}

	public void setFormat(String format) {
	}

	public void setFilenamePatterns(String[] filePattern) {
	}
}