package com.liquidlabs.vso.monitor;

import java.net.URISyntaxException;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.transport.proxy.ProxyFactoryImpl;
import com.liquidlabs.transport.proxy.events.Event;
import com.liquidlabs.transport.proxy.events.Event.Type;
import com.liquidlabs.vso.Notifier;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.LookupSpaceImpl;

public class MonitorSpaceImpl implements MonitorSpace {

	private static final Logger LOGGER = Logger.getLogger(MonitorSpaceImpl.class);
	private final SpaceServiceImpl spaceService;

	public MonitorSpaceImpl(SpaceServiceImpl spaceService) {
		this.spaceService = spaceService;
	}
	
	public void start() {
		spaceService.start(this, "boot-1.0");
	}
	
	public void stop() {
		spaceService.stop();
	}
	
	public void registerMetricsListener(final MetricListener listener, String filter, String listenerId) {
		spaceService.registerListener(Metrics.class, filter, new Notifier<Metrics>() {
			public void notify(Type event, Metrics result) {
				listener.handle(result);
			}}, listenerId, 3 * 60, new Event.Type[] { Type.WRITE, Type.UPDATE });
	}

	public void unregisterMetricsListener(String listenerId) {
		spaceService.unregisterListener(listenerId);
	}

	public void write(Metrics metrics) {
		spaceService.store(metrics, 0);

	}
	public static void main(String[] args) {
		try {
			MonitorSpaceImpl.boot(args[0]);
		} catch (Throwable t) {
			LOGGER.error(t.getMessage(), t);
			throw new RuntimeException(t.getMessage() ,t);
		}
		
	}
	public static MonitorSpace boot(String lookupAddress) throws URISyntaxException, UnknownHostException{
		LOGGER.info("Starting");
		try {
		
			return bootIt(lookupAddress);
		} catch (Throwable t) {
			LOGGER.info("Starting Failed:" + t.getMessage() + " so trying again....");
			return bootIt(lookupAddress);
		}
	}
	
	private static MonitorSpaceImpl bootIt(String lookupAddress) {
		ORMapperFactory mapperFactory = LookupSpaceImpl.getSharedMapperFactory();
		ProxyFactoryImpl proxyFactory = mapperFactory.getProxyFactory();
		
		LookupSpace lookupSpace = LookupSpaceImpl.getRemoteService(lookupAddress, mapperFactory.getProxyFactory(),"MonSpaceBootLU");
		MonitorSpaceImpl monitorSpace = new MonitorSpaceImpl(new SpaceServiceImpl(lookupSpace,mapperFactory, MonitorSpace.NAME,  mapperFactory.getScheduler(), true, false, true));
		proxyFactory.registerMethodReceiver(MonitorSpace.NAME, monitorSpace);
		monitorSpace.start();
		LOGGER.info("Started");
		return monitorSpace;
	}

}
