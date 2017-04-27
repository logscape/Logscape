package com.liquidlabs.replicator;

public class ReplicatorProperties {
	
	
	// cannot be used due to dependency directions in SLAContainer and DashboardService
	static public String getRFSRoot(){
		return System.getProperty("rfs.root", "rfs");
	}
	static public String getDownloadsDir(){
		return System.getProperty("rfs.download", "downloads");
	}

	public static int getChunkSizeKB() {
		return Integer.getInteger("rfs.chunk.kb", 512);
	}

	public static int getDirPollInterval() {
		return Integer.getInteger("rfs.dir.poll.interval", 20);
	}
	public static int getNewFileSafeSeconds() {
		return Integer.getInteger("rfs.new.file.safe.secs", 30);
	}
	public static void setNewFileWaitSecs(int i) {
		System.setProperty("rfs.new.file.safe.secs", Integer.toString(i));
	}
	public static long getPauseSecsBetweenDeleteAndDownload() {
		return Integer.getInteger("rfs.new.file.pause.delete", 3);
	}
	public static void setPauseSecsBetweenDeleteAndDownload(int i) {
		System.setProperty("rfs.new.file.pause.delete", i +"");
	}

	public static int getLeasePeriod() {
		return Integer.getInteger("rfs.lease.period", 7 * 60);
	}
	public static int getLeaseRenewPeriod() {
		return Integer.getInteger("rfs.lease.period", 3 * 60);
	}
	
	public static int getDownloadRetryPause() {
		return Integer.getInteger("rfs.dl.retry.pause", 3 * 1000);
	}
	public static int getDownloadRescheduleTime() {
		return Integer.getInteger("rfs.dl.resched.pause", 10 * 1000);
	}
	
	public static int downloadThreads() {
		return Integer.getInteger("rfs.dl.threads", 5);
	}
}
