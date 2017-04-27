package com.liquidlabs.log.alert.report;

public class HostLogSummary {
	
	public HostLogSummary() {
	}
	public HostLogSummary(String host, String filename) {
		this.hostname = host;
		this.filename = filename;
	}
	public String hostname;
	public String filename;
	public int hitCount;
	public int hitPercent;
	
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append("[HostLogSummary:");
		buffer.append(" host:");
		buffer.append(hostname);
		buffer.append(" file:");
		buffer.append(filename);
		buffer.append(" hits:");
		buffer.append(hitCount);
		buffer.append(" hitPc:");
		buffer.append(hitPercent);
		buffer.append("]");
		return buffer.toString();
	}
	public String getXMLRow() {
		String file = filename;
		int limit = 60;
		if (file.length() > limit) file = "..." + file.substring(file.length() - limit);

		return String.format("<row host=\"%s\" file=\"%s\" hits=\"%d\" percent=\"%d%%\" />\n", hostname, file, hitCount, hitPercent);
	}
}
