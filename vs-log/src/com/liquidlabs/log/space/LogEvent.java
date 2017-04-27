package com.liquidlabs.log.space;

import java.rmi.server.UID;

import com.liquidlabs.orm.Id;

public class LogEvent {

	@Id
	private String id;
	private String message;
	private String hostname;
	private String filename;
	private String sourceURI;
	private Integer lineNumber;
	private Integer matchIndex;
	private String tag;
	

	public LogEvent(){}
	
	public LogEvent(String sourceURI, String message, String hostname, String filename, Integer lineNumber, Integer matchIndex, String tag) {
		this.sourceURI = sourceURI;
		this.hostname = hostname;
		this.filename = filename;
		this.lineNumber = lineNumber;
		this.matchIndex = matchIndex;
		this.tag = tag;
		this.id = new UID().toString();
		this.message = message;
	}

	public String getMessage() {
		return message;
	}
	

	public String getId() {
		return id;
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}
	public String getTag() {
		return tag;
	}
	
	@Override
	public String toString() {
		return hostname + ":" + filename + " -> " + message;
	}

	public String getSourceURI() {
		return sourceURI;
	}

	public Integer getLineNumber() {
		return lineNumber;
	}
	
	public Integer getMatchIndex() {
		return matchIndex;
	}

	/**
	 * 
	 * @param hostsFilter - comma delimited subname filter
	 * @return
	 */
	public boolean matchesHostFilter(String hostsFilter) {
		if (hostsFilter.trim().length() == 0) return true;
		hostsFilter = hostsFilter.toUpperCase();
		String[] split = hostsFilter.split(",");
		for (String hostPart : split) {
			hostPart = hostPart.trim();
			if (this.hostname.toUpperCase().contains(hostPart)) return true;
		}
		return false;
	}

}
