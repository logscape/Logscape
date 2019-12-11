package com.liquidlabs.vso;

import com.liquidlabs.common.NetworkUtils;
import org.joda.time.DateTime;

public class VSOProperties {
	public static final String MANAGEMENT = "Management";
    public static final String FAILOVER = "Failover";

	private static final String VSO_NOISY_LOOKUP = "vso.verbose.lookup";
	public static final String VSCAPE_DEPLOYED_BUNDLES_DIR = "vscape.deployed.bundles.dir";
	public static final String VSCAPE_SYSTEM_BUNDLES_DIR = "vscape.system.bundles.dir";
	public static final String HA_TAG = "_HA_";
	private static long taskNoiseSecs = Long.getLong("vso.task.noice.secs",3);
    private static long agentCountAuditSchedule;
    public static final DateTime startTime = new DateTime();

    public static Object getWebAppRootDir() {
        return System.getProperty("webapp.root", "work/jetty-0.0.0.0-8443-root.war-_-any-/webapp");
    }

    public static long getTaskNoiseSecs() {
        return taskNoiseSecs;
    }

    public static long getAgentCountAuditSchedule() {
        return Integer.getInteger("agent.count.audit.interval", 30);
    }


    public enum ports { LOOKUP, LOOKUP_REPL, LOOUKP_JMX, SHARED, SHARED_REPL, SHARED_JMX, LOOPBACK, DASHBOARD, AGENT_LOG_SPACE, DASHBOARD_REPL, DASHBOARD_JMX, LOGSPACE, LOGSPACE_RPL, LOGSPACE_JMX, AGGSPACE, AGGSPACE_REPL, AGGSPACE_JMX,
		REPLICATOR, REPLICATOR_REPL, REPLICATOR_JMX, HTTPLOGSERVER, HTTPLOGSERVER_REPL, HTTPLOGSERVER_JMX, METER_SVC, METER_SVC_REPL, METER_SVC_JMX};

    private static int foundPort = 0;
	public static int getBasePort() {
        if (foundPort != 0) return foundPort;
        Integer integer = Integer.getInteger("vso.base.port", 11000);
        foundPort = integer;
//        foundPort = NetworkUtils.determinePort(integer,1000);
        System.out.println("BASEPORT:" + foundPort);
        return foundPort;
	}
	
	public static void setBasePort(int value) {
		System.setProperty("vso.base.port", Integer.toString(value));
	}
	public static Integer getLookupPort() {
		return Integer.getInteger("vso.boot.lookup.port", getBasePort());
	}
	public static int getAgentBasePort() {
		return Integer.getInteger("vso.base.port", getBasePort());
	}
	public static Integer getReplicationPort(){
		return Integer.getInteger("vso.boot.lookup.replication.port", getBasePort() + 30);
	}
	public static Integer getJMXPort(){
		return Integer.getInteger("vso.boot.lookup.replication.port", getBasePort() + 40);
	}
	public static void setLookupPort(int luPort) {
		System.setProperty("vso.boot.lookup.port", Integer.toString(luPort));
	}

	
	public static String getSystemBundleDir() {
		return getRootDir() + System.getProperty(VSCAPE_SYSTEM_BUNDLES_DIR, "system-bundles");
	}

	public static void setSystemBundleDir(String string) {
		System.setProperty(VSCAPE_SYSTEM_BUNDLES_DIR, string);
	}
    public static String getDeployedBundleDir() {
		return getRootDir() + System.getProperty(VSCAPE_DEPLOYED_BUNDLES_DIR,"deployed-bundles");
	}
	public static void setDeployedBundleDir(String dir) {
		System.setProperty(VSCAPE_DEPLOYED_BUNDLES_DIR, dir);
	}


	public static int getResourceRequestTimeout() {
		return Integer.getInteger("vso.resource.request.timeout", 10 * 60);
	}

	public static long getSlaSyncInterval() {
		return Integer.getInteger("vso.sla.sync.interval", 5 * 60);
	}

	public static Boolean getResourceFairShare() {
		return !Boolean.getBoolean("vso.fairshare.off");
	}

	public static void setResourceFairShareOff(boolean value) {
		System.setProperty("vso.fairshare.off", Boolean.valueOf(value).toString());
	}

	public static int getResourceRegListenerTimeout() {
		return Integer.getInteger("vso.resourceRegListenerTimeout", 3 * 60);
	}

	public static int getResourceReleasedPendWaitCount() {
		return Integer.getInteger("vso.resource.release.pend.wait", 10);
	}

    public static String getPrimaryLookupReplicationUrl() {
        return System.getProperty("vso.primary.lookup.replication.url");
    }

    public static boolean isHaEnabled() {
        return Boolean.getBoolean("vso.ha.enabled");
    }
	

	public static void setLookupAddress(String string) {
		System.setProperty("lookup.url", string);
	}
	public static String getLookupAddress() {
        return System.getProperty("lookup.url", "stcp://localhost:11000");
	}

	public static Integer getLUAddressLeaseGranularitySecs() {
		return Integer.getInteger("vso.lookup.lease.gran.secs", 60);
	}
	public static int getLUSpaceServiceRenewInterval() {
		return Integer.getInteger("vso.lookup.serNvice.renew.interval", getLUAddressLeaseGranularitySecs());
	}


	public static int getLUSpaceServiceLeaseInterval() {
		return Integer.getInteger("vso.lookup.service.lease.interval", (getLUAddressLeaseGranularitySecs() * 2) + 10);		
	}
	
	public static Integer getLUAddressSyncInterval() {
		return Integer.getInteger("vso.address.sync.interval", 10);
	}

	public static int getAddressSyncherRetryCount() {
		return Integer.getInteger("vso.address.sync.retry", 2);
	}


	public static int getResourceListenerRegInterval() {
		return Integer.getInteger("vso.resource.listener.reg.interval", 10 * 60);		
	}

	public static int agentRebootPingTrySeconds() {
		return Integer.getInteger("vso.resource.reboot.retry.wait", 10);		
	}

	public static String getResourceType() {
		return System.getProperty("vso.resource.type", System.getProperty("agent.role", "Agent"));
	}

	public static void setResourceType(String type) {
		System.setProperty("vso.resource.type", type);		
	}

	public static String getWorkingDir() {
		return System.getProperty("vso.agent.work.dir", "work");		
	}
	public static void setWorkingDir(String dir) {
		System.setProperty("vso.agent.work.dir", dir);		
	}

	public static int getWorkDirDaysToClean() {
		return Integer.getInteger("vso.agent.work.dir.clean.days", 14);
	}

    /**
     * Read the zone from the lead of the agents resource type
     * @return
     */
	public static String getZone() {
        String rt = getResourceType();
        if (!rt.contains(".")) return defaultZone();
		return rt.substring(0, rt.lastIndexOf("."));
	}
    public static String defaultZone() {
        return "dev";
    }

	public static int getLocationX() {
		return Integer.getInteger("vso.loc.X", 0);
	}
	public static int getLocationY() {
		return Integer.getInteger("vso.loc.Y", 0);
	}
	public static int getLocationZ() {
		return Integer.getInteger("vso.loc.Z", 0);
	}

	public static int getLostResourceTimeout() {
		return Integer.getInteger("vso.lost.res.timeout.days", 7) *  24 * (60 * 60);
	}


    public static void setPrimaryLookupReplicationUrl(String url) {
        System.setProperty("vso.primary.lookup.replication.url", url);
    }
	public static boolean isVERBOSELookups() {
		return Boolean.getBoolean(VSO_NOISY_LOOKUP);
	}
	public static int getPort(ports item) {
		int returnPort = getBasePort() + item.ordinal();
//		System.err.println("GetPorts:" + item + " :" + returnPort);
		return returnPort;
	}
	public static int getREPLICATIONPort(ports item) {
		return  getPort(item) + 1;
	}
	public static int getJMXPort(ports item) {
		return getPort(item) + 2;
	}


    public static void setRootDir(String dir) {
        System.setProperty("root.dir",dir);
    }
    public static String getRootDir() {
        return System.getProperty("root.dir","./");
    }
    public static String getDownloadsDir() {
		return getRootDir() + System.getProperty("downloads.dir", "downloads");
    }

	public static void setDownloadsDir(String string) {
		System.setProperty("downloads.dir", string);
	}
	public static boolean getAddrSyncSuppressRepeats() {
		try {
			return Boolean.parseBoolean(System.getProperty("supress.ha.sync.repeats", "true"));
		} catch (Throwable t) {
			return true;
		}
	}
	public static boolean isFailoverNode() {
		return false;
	}
	public static long getClockSyncMins() {
		return Integer.getInteger("clock.sync.mins", 5);
	}
	public static boolean isManager() {
		return getResourceType().contains(MANAGEMENT);
	}
	public static boolean isManagerOnly() {
		return getResourceType().contains(MANAGEMENT);
	}

	public static long getProcessStartingWaitMs() {
		return Long.getLong("vso.proc.wait.ms", 5000);
	}
    public static void setProcessStartingWaitMs(long waitMs) {
        System.setProperty("vso.proc.wait.ms", waitMs+"");
    }

	public static String getLogScriptDelim() {
		return System.getProperty("log.split", "\t");
	}

	public static long getShutdownPoolTimeout() {
		return Integer.getInteger("shutdown.wait.secs", 60);
	}

	public static int getAgentSchedThreads() {
		return Integer.getInteger("agent.sched.threads", 25);
	}

	public static final String SS_MAX_NOTIFY_FAILS = "ss.max.notify.fails";
	public static final int SS_MAX_NOTIFY_FAILS_DEFAULT = 100;

	public static int getMaxNotifyFailures() {
		return Integer.getInteger(SS_MAX_NOTIFY_FAILS, SS_MAX_NOTIFY_FAILS_DEFAULT);
	}
	public static void setMaxNotifyFailures(int count) {
		System.setProperty(SS_MAX_NOTIFY_FAILS, ""+count);
	}
	public static void resetMaxNotifyFailures() {
		System.setProperty(SS_MAX_NOTIFY_FAILS, ""+SS_MAX_NOTIFY_FAILS_DEFAULT);
	}


	public static String getManagerReplicationPort() {
		return System.getProperty("manager.replication.port", "15000");
	}

	public static long getAllocDelay() {
		return Integer.getInteger("alloc.delay", 400);
	}
}
