package com.liquidlabs.space;

public class VSpaceProperties {

	public static int getReplicationFrequency() {
		return Integer.getInteger("vspace.mcast.repli.frequency", 60);
	}

	public static int getCorePoolSize() {
		return Integer.getInteger("vspace.core.poolsize", 5);
	}
	public static int getMaxPoolSize() {
		return Integer.getInteger("vspace.max.poolsize", 10);
	}

	public static int getMaxSpaceSize() {
		return Integer.getInteger("vspace.max.space.size", 100 * 1024);
	}

	public static int getSnapshotIntervalSecs() {
		return Integer.getInteger("vspace.snapshot.interval.secs", 5 * 60);
	}
	public static void setSnapshotInterval(int interval) {
		System.setProperty("vspace.snapshot.interval", String.valueOf(interval));
	}

	public static long getShortLeaseThresholdSeconds() {
		return Integer.getInteger("vspace.lease.short.threshold.secs", 5);
	}

	public static int getLeaseRegisterFailureWaitSeconds() {
		return Integer.getInteger("vspace.lease.failure.wait.secs", 30);
	}

	public static String baseSpaceDir() {
		return System.getProperty("base.space.dir","space");
	}

	public static void setBaseSpaceDir(String string) {
		System.setProperty("base.space.dir", string);
	}

	public static boolean dropReadEvents() {
		return !Boolean.getBoolean("allow.read.events");
	}

}
