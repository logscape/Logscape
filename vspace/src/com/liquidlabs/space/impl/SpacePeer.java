package com.liquidlabs.space.impl;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.log4j.Logger;
import org.joda.time.format.DateTimeFormat;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.concurrent.NamingThreadFactory;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.space.Space;
import com.liquidlabs.space.VSpaceProperties;
import com.liquidlabs.transport.EndPoint;
import com.liquidlabs.transport.Receiver;
import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.TransportFactoryImpl;
import com.liquidlabs.transport.TransportProperties;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.transport.proxy.ProxyFactoryImpl;

public class SpacePeer implements LifeCycle {
	
	private static final Logger LOGGER = Logger.getLogger(SpacePeer.class);
	
	public static final String DEFAULT_SPACE = "defaultSpace";
	
	int snapshotInterval = VSpaceProperties.getSnapshotIntervalSecs();
	

	private NamingThreadFactory schedulerThreadFactory  = new NamingThreadFactory("scheduler");
	private ScheduledExecutorService scheduler = com.liquidlabs.common.concurrent.ExecutorService.newScheduledThreadPool(1, schedulerThreadFactory);

	private ProxyFactory proxyFactory;
	private ExecutorService generalExecutor = com.liquidlabs.common.concurrent.ExecutorService.newDynamicThreadPool("worker", "spacePeer");

	private SpaceFactory spaceFactory;

	private TransportFactory transportFactory;

	private EndPoint replicationServer;
	private long startTime = System.currentTimeMillis();
	String sTime = DateTimeFormat.forPattern("yyyyMMdd_HHmmss").print(startTime);

	private URI replicationURI;

	private URI replicationUri;
	private boolean initialised = false;
	
	/**
	 * Use only when exposing a Space (mostly testing)
	 * @param replicationUri
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public SpacePeer(URI uri) throws IOException, URISyntaxException {
		this.replicationUri = uri;
		this.transportFactory = new TransportFactoryImpl(generalExecutor, "peer");
		this.transportFactory.start();
		this.proxyFactory = new ProxyFactoryImpl(transportFactory, getURIWithPort(uri, uri.getPort() + TransportProperties.getReplicationPortOffset()), generalExecutor, "");
	}
	
	public SpacePeer(URI replicationUri, TransportFactory transportFactory, ScheduledExecutorService scheduler, ExecutorService executor, ProxyFactoryImpl proxyFactory, boolean isClustered, boolean reuseClusterPort) throws IOException {
		this.init(transportFactory, replicationUri,  scheduler, executor, proxyFactory, isClustered, reuseClusterPort);
	}
	
	public void init(TransportFactory transportFactory, URI replicationUri, ScheduledExecutorService scheduler, ExecutorService executor, ProxyFactory proxyFactory, boolean isClustered, boolean reuseClusterPort) {
		
		if (initialised) return;
		
		this.initialised = true;
		
		this.generalExecutor = executor;
		this.scheduler = scheduler;
		this.transportFactory = transportFactory;
		this.replicationUri = reuseClusterPort == true? replicationUri : fixURIPortIfInUse(replicationUri);

		int retryCount = 0;
		boolean started = false;
		Throwable th = null;
		while (!started && retryCount++ < 10) {
			try {
				if (isClustered) {
					this.replicationUri = reuseClusterPort == true? replicationUri : fixURIPortIfInUse(replicationUri);
					this.replicationServer = transportFactory.getEndPoint(replicationUri, new StubReceiver(), true);
					LOGGER.info(" Replicating on : " + replicationUri);
				}
                
                started = true;
			} catch (Throwable t) {
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
				}
				th = t;
			}
		}
		if (!started)
			throw new RuntimeException("Failed to start", th); 
		if (isClustered) {
			this.replicationURI = replicationUri;
		}
		this.spaceFactory = new SpaceFactory(proxyFactory == null ? scheduler : proxyFactory.getScheduler(), transportFactory);
		
		this.proxyFactory = proxyFactory;
		
		schedulerThreadFactory.setNamePrefix("REPscheduler-"+replicationUri.getPort()+"-");

	}

	private URI fixURIPortIfInUse(URI uri) {
		int port = NetworkUtils.determinePort(uri.getPort());
		return getURIWithPort(uri, port);
	}

	private URI getURIWithPort(URI uri, int port) {
		try {
			return new URI(uri.getScheme(),uri.getUserInfo(), uri.getHost(), port, uri.getPath(), uri.getQuery(), uri.getFragment());
		} catch (URISyntaxException e) {
			LOGGER.error(e);
		}
		return uri;
	}
	
	public Space createSpace(String partitionName, boolean isClustered, boolean reuseClusterPort){
		this.init(transportFactory, replicationUri, scheduler, generalExecutor, proxyFactory, isClustered, reuseClusterPort);
		return createSpace(partitionName, 20 * 1024, isClustered, false, reuseClusterPort);
	}
	
	public Space createSpace(String partitionName, int spaceSize, boolean isClustered, boolean persistentMap, boolean reuseClusterPort) {
		try {
			if (spaceFactory == null) this.init(transportFactory, replicationUri, scheduler, generalExecutor, proxyFactory, isClustered, reuseClusterPort);
			final Space newSpace = spaceFactory.createSpace(partitionName, scheduler, spaceSize, proxyFactory, isClustered, persistentMap, replicationServer, this.replicationURI, this.generalExecutor);
			proxyFactory.registerMethodReceiver(partitionName, newSpace);
			return newSpace;
		} catch (URISyntaxException e) {
			LOGGER.warn(e.getMessage(), e);
			throw new RuntimeException("Failed to createSpace:" + partitionName, e);
		} catch (PersistentSpaceException e) {
			LOGGER.warn(e.getMessage(), e);
			throw new RuntimeException("Failed to createSpace:" + partitionName, e);
		} catch (InterruptedException e) {
			throw new RuntimeException("Failed to createSpace:" + partitionName, e);
		}
	}
	public void addReceiver(String partitionName, Object instance, Class<?> targetClass){
		proxyFactory.registerMethodReceiver(partitionName, instance);
	}
	
	public void start() {
		spaceFactory.start();
		proxyFactory.start();
	}

	public void stop() {
		spaceFactory.stop();
		transportFactory.stop();
		proxyFactory.stop();
		if (!Boolean.getBoolean("test.mode") && !generalExecutor.isShutdown()) generalExecutor.shutdownNow();
		if (!scheduler.isShutdown()) scheduler.shutdownNow();
	}
	public URI getClientAddress(){
		return this.proxyFactory.getAddress();
	}

	public EndPoint getEndPointServer() {
		return this.proxyFactory.getEndPointServer();
	}

	public ProxyFactory getProxyFactory() {
		return proxyFactory;
	}
	public URI getReplicationURI() {
		return replicationURI;
	}
	public String toString() {
		return super.toString() + " address:" + getClientAddress();
	}
	public static class StubReceiver implements Receiver {

		public boolean isForMe(Object payload) {
			return false;
		}

		public byte[] receive(byte[] payload, String remoteAddress,
				String remoteHostname) throws InterruptedException {
			return null;
		}

		public byte[] receive(Object payload, String remoteAddress,
				String remoteHostname) {
			return null;
		}

		public void start() {
		}

		public void stop() {
		}
	}
}
