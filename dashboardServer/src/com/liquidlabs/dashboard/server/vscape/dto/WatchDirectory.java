package com.liquidlabs.dashboard.server.vscape.dto;


public class WatchDirectory {
	public WatchDirectory() {
	}
	public WatchDirectory(com.liquidlabs.log.space.WatchDirectory watchDirectory) {
		this.wdId = watchDirectory.id();
		this.tags = watchDirectory.getTags();
		this.dirName = watchDirectory.getDirName();
		this.filePattern = watchDirectory.filePattern;
		this.timeFormat = watchDirectory.getTimeFormat();
		this.rollFormat = watchDirectory.getFileSorter().getFormat();
		this.rollClass = watchDirectory.getFileSorter().getClass().getName();
		this.hosts = watchDirectory.getHosts();
		this.maxAgeDays = watchDirectory.getMaxAge();
		this.dwEnabled = watchDirectory.isDiscoveryEnabled();
		this.breakRule = watchDirectory.getBreakRule();
		
	}
	
	public String wdId;
	public String tags;
	public String dirName;
	public String filePattern;
	public String timeFormat;
	public boolean recurse;
	public String rollFormat;
	public String rollClass;
	public String hosts;
	public int maxAgeDays;
	public boolean dwEnabled;
	public String breakRule;
	
}
