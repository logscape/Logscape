package com.liquidlabs.transport;

import org.joda.time.DateTime;

import java.util.concurrent.atomic.AtomicInteger;

public class TransportProperties {

	public static final String VSO_CLIENT_PORT_RESTRICT = "vso.client.port.restrict";

	public static final String SSL_CERT = System.getProperty("vscape.home") != null ?  System.getProperty("vscape.home").replace("\\","/") + "/ssl/p2p.crt" : "ssl/p2p.crt";
	public static final String SSL_KEY = System.getProperty("vscape.home") != null ?  System.getProperty("vscape.home").replace("\\","/") + "/ssl/p2p.key" : "ssl/p2p.key";


	/**
	 * try and choose a timebased random offset
	 */
	private static AtomicInteger currentPortOffset = new AtomicInteger(new DateTime().getSecondOfMinute());
//	private static AtomicInteger currentPortOffset = new AtomicInteger(1);
	private static int clientBsePort = Integer.getInteger("vso.client.port", 62000); 
	public static int clientMaxPorts = Integer.getInteger("vso.client.port.range", 1000);
	private static String SSLDomain;

	public static int getClientBasePort() {
		synchronized (currentPortOffset) {
			if (currentPortOffset.get() > clientMaxPorts) {
				currentPortOffset.set(0);
			}
			return  clientBsePort + currentPortOffset.getAndIncrement();
		}
	}
	public static int getClientMaxPorts() { 
		return clientMaxPorts;
	}
	public static void updateBasePort(int port) {
		synchronized (currentPortOffset) {
			int newValue = port - clientBsePort;
			if (newValue > clientMaxPorts) newValue = 0;
			currentPortOffset.set(newValue);
		}
	}
	
	public static boolean isClientPortsRestricted() {
		return Boolean.getBoolean(VSO_CLIENT_PORT_RESTRICT);
	}
	public static void setClientPortsRestricted() {
		System.setProperty(VSO_CLIENT_PORT_RESTRICT, "true");
	}

	public static int getMCastPacketSize() {
		return Integer.getInteger("vscape.udp.packet.size", 1024 * 4);
	}

	public static Integer getMCastTTL() {
		return Integer.getInteger("vscape.mcast.ttl", 0);
	}
	public static void setMCastTTL(int ttl) {
		System.setProperty("vscape.mcast.ttl", Integer.toString(ttl));
	}

	public static int getMCastDiscoFrequency() {
		return Integer.getInteger("vscape.mcast.pub.frequency", 60);
	}

    public static int getReplicationPort() {
        return Integer.getInteger("vscape.replication.port", 11010);
    }


	public static int getProxySchedulerPoolSize() {
		return Integer.getInteger("vscape.proxy.scheduler.poolsize", 5);
	}
	public static void setProxySchedulerPoolSize(int size) {
		System.setProperty("vscape.proxy.scheduler.poolsize", Integer.toString(size));
	}


	public static int getInvocationTimeoutSecs() {
		return Integer.getInteger("vscape.proxy.inv.timeout.secs", 40);
	}
	
	public static int getConnectionEstablishTimeout() {
		return Integer.getInteger("vscape.conn.est.timeout", 5);
	}
	public static void setInvocationTimeoutSecs(int timeout) {
		System.setProperty("vscape.proxy.inv.timeout.secs", Integer.toString(timeout));
	}

	public static Boolean isMCastEnabled() {
		return getMCastTTL() > 0;
	}
	static Boolean connectDebug;
	public static Boolean isConnectionDebug() {
		if (connectDebug == null) {
			connectDebug = Boolean.parseBoolean(System.getProperty("vscape.connection.debug", "false"));
		}
		return connectDebug;
	}
	public static void setMCastEnabled(Boolean enabled) {
		System.setProperty("vscape.mcast.enabled", enabled.toString());
	}

	public static int getTcpSendRetryCount() {
		return Integer.getInteger("vscape.tcp.send.retry.count", 100);
	}

	public static int getConnectionOutstandingLimit() {
		return Integer.getInteger("vscape.tcp.client.connection.outstanding.limit", 5);
	}

	public static int getMaxUDPSize() {
		return Integer.getInteger("vscape.udp.max", 6 * 1024);
	}

	public static int getConnectionWaitTimeSeconds() {
		return Integer.getInteger("vscape.tcp.wait.secs", 10);
	}

	public static int getObjectGraphDepth() {
		return Integer.getInteger("vscape.obj.depth", 3);
	}
	public static int oneWayThreadPoolSize() {
		return Integer.getInteger("vscape.oneway.pool.size", 50);
	}
	public static int getReplicationPortOffset() {
		return Integer.getInteger("space.repl.port.offset", 20);
	}
	public static int getNWMinPool() {
		return Integer.getInteger("transport.nw.min.pool", 5);
	}
	public static int getNWMaxPool() {
		return Integer.getInteger("transport.nw.max.pool", 40);
	}
	public static int getNioWorkerThreads() {
		return Integer.getInteger("transport.nw.nio.worker.threads", 3);
	}
	private static long connectionTimeOut = Long.getLong("connection.timeout.ms", 1000);
	public static long getConnecionTimeOut() {
		return connectionTimeOut;
	}


    static int secureEndpointPort = Integer.getInteger("endpoint.security.port", 11000);
    public static int getSecureEndpointPort() {
        return secureEndpointPort;
    }

	public static String getSSLDomain() {
		return System.getProperty("ssl.selfsign.domain","logscape.com");
	}
}
