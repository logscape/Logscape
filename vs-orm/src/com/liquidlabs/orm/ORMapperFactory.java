package com.liquidlabs.orm;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import com.liquidlabs.common.LifeCycle;
import org.apache.log4j.Logger;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.concurrent.NamingThreadFactory;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.space.Space;
import com.liquidlabs.space.VSpaceProperties;
import com.liquidlabs.space.impl.SpacePeer;
import com.liquidlabs.transport.Config;
import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.TransportFactoryImpl;
import com.liquidlabs.transport.proxy.ProxyFactoryImpl;

public class ORMapperFactory implements LifeCycle {
	
	private static final int MAX_SPACE_SIZE = VSpaceProperties.getMaxSpaceSize();
	Integer threadPoolSize = VSpaceProperties.getCorePoolSize();

	private static final Logger LOGGER = Logger.getLogger(ORMapperFactory.class);
	
	List<SpacePeer> spacePeers = new ArrayList<SpacePeer>();
	ProxyFactoryImpl proxyFactory;
	NamingThreadFactory threadFactory = new NamingThreadFactory("prefix");
	
	int spaceSize = MAX_SPACE_SIZE;

	private TransportFactory transportFactory;

	private ExecutorService executor;
	private final int servicePort;
	private int replicationPort;
	
	public ORMapperFactory() {
		 this(NetworkUtils.determinePort(Config.TEST_PORT), "", NetworkUtils.determinePort(Config.TEST_PORT));
	}
	public ORMapperFactory(int port) {
		this(port, "", port);
	}
	public ORMapperFactory(int port, String serviceName, int replicationPort) {
		this(port, serviceName, MAX_SPACE_SIZE, replicationPort);
	}
	public ORMapperFactory(int port, String serviceName, int spaceSize, int replicationPort) {
		this(port, serviceName, spaceSize, getExecutor(VSpaceProperties.getMaxPoolSize(), serviceName, port), replicationPort);
	}
	private static ExecutorService getExecutor(int maxPoolSize, String serviceName, int port) {
		return com.liquidlabs.common.concurrent.ExecutorService.newDynamicThreadPool("worker", serviceName +":ORM:"+port);
	}
	public ORMapperFactory(int port, String serviceName, int spaceSize, ExecutorService executor, int replicationPort) {
		try {
			this.servicePort = port;
			if (spaceSize > 0) this.spaceSize = spaceSize;
			this.executor = executor;
			transportFactory = new TransportFactoryImpl(com.liquidlabs.common.concurrent.ExecutorService.newScheduledThreadPool(1,new NamingThreadFactory("orm-" + serviceName)), serviceName);
			transportFactory.start();
			proxyFactory = new ProxyFactoryImpl(transportFactory, port, executor, serviceName);
			
			LOGGER.info(String.format("Starting Service[%s] port[%d] size[%d] pid[%s] ", serviceName, port, spaceSize, ManagementFactory.getRuntimeMXBean().getName()));
			
//			replicationPort = NetworkUtils.determinePort(port + 20);
			replicationURI = TransportFactoryImpl.getDefaultProtocolURI(null, NetworkUtils.getIPAddress(), replicationPort, serviceName);

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		proxyFactory.start();
	}
	
	/**
	 * Points to a remote ORMapper Space
	 * @throws URISyntaxException 
	 */
	public ORMapperClient getORMapperClient(String remoteURI, String partition) throws IOException, URISyntaxException{
		ORMapper remoteService = proxyFactory.getRemoteService(partition, ORMapper.class, new String[] { remoteURI });
		return new ORMapperClientImpl(remoteService, proxyFactory);
	}

	/**
	 * Runs an embedded ORMapperImpl
	 */
	public ORMapperClient getORMapperClient() throws IOException{
		return new ORMapperClientImpl(getORMapper(ORMapper.NAME, this.servicePort), proxyFactory);
	}

	/**
	 * Runs an embedded ORMapperImpl
	 */
	public ORMapperClient getORMapperClient(String name) throws IOException {
		return new ORMapperClientImpl(getORMapper(name, this.servicePort), proxyFactory);
	}


	int portOffset = 0;
//	public ORMapperClient getORMapperClient(String name, Object serviceImpl, boolean isClustered, boolean persistent) {
//        return createORMapperClient(name, serviceImpl, isClustered, persistent, this.servicePort + portOffset++);
//	}
	private URI replicationURI;

    public ORMapperClient getORMapperClient(String name, Object serviceImpl, boolean isClustered, boolean persistent) {
        return createORMapperClient(name, serviceImpl, isClustered, persistent);
	}

    private ORMapperClient createORMapperClient(String name, Object serviceImpl, boolean isClustered, boolean persistent) {
        ORMapperClientImpl mapperClientImpl = new ORMapperClientImpl(getORMapper(name, spaceSize, isClustered, persistent), proxyFactory);
		addReceiver(name, serviceImpl);
        return mapperClientImpl;
    }

    public void addReceiver(String name, Object serviceImpl) {
		LOGGER.info("Service:" + name + " available on:" + proxyFactory.getAddress(name));
		if (serviceImpl != null) this.proxyFactory.registerMethodReceiver(name, serviceImpl);
	}


	public ORMapper getORMapper(String serviceName, int server_port) {
		return getORMapper(serviceName, spaceSize, true, false);
	}
	
	synchronized public ORMapper getORMapper(String serviceName, int spaceSize, boolean isClustered, boolean persistent) {
		
		SpacePeer spacePeer = null;
		try {
			LOGGER.info("REPLICATION PORT:" + replicationURI + " SERVICE:" + serviceName + " PORT:" + replicationPort);
			URI repURI = new URI(replicationURI.getScheme() + "://" + replicationURI.getHost() + ":" + replicationURI.getPort() + "?svc=" + serviceName);
			spacePeer = new SpacePeer(repURI, transportFactory, proxyFactory.getScheduler(), executor, proxyFactory, isClustered, true);
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage() + ":"+ replicationURI, e);
		}
		spacePeers.add(spacePeer);
		Space space = creatSpace(serviceName, spaceSize, persistent, isClustered, spacePeer);
		ORMapper orMapper = new ORMapperImpl(space);
		spacePeer.start();
		return orMapper;
	}
	
	private Space creatSpace(String serviceName, int spaceSize, boolean persistent, boolean isClustered, SpacePeer spacePeer) {
		String partitionName = serviceName + "-SPACE";
		try {
			return spacePeer.createSpace(partitionName, spaceSize, isClustered, persistent, true);
		} catch (Exception e) {
			LOGGER.warn("Unable to create persistent space - reverting to default", e);
			return spacePeer.createSpace(partitionName, spaceSize, isClustered, false, true);
		}
	}
	
	public void start(){
		proxyFactory.start();
	}
	public void stop(){
		for (SpacePeer spacePeer : spacePeers){
			spacePeer.stop();
		}
		proxyFactory.stop();
		if (executor != null) executor.shutdownNow();
		if (transportFactory != null) transportFactory.stop();
	}

	public URI getClientAddress() {
		return proxyFactory.getAddress();
	}

	public ProxyFactoryImpl getProxyFactory() {
		return proxyFactory;
	}
	public void publishServiceAvailability(String name) {
		proxyFactory.publishAvailability(name);
	}
	public TransportFactory getTransportFactory() {
		return transportFactory;
	}


	public ExecutorService getExecutor() {
		return executor;
	}
	public ScheduledExecutorService getScheduler() {
		return proxyFactory.getScheduler();
	}
	public int getPort() {
		return servicePort;
	}
}
