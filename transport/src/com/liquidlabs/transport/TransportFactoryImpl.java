package com.liquidlabs.transport;

import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import com.liquidlabs.transport.rabbit.RabbitEndpointFactory;
import org.apache.log4j.Logger;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Log4JLoggerFactory;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.netty.NettyEndPointFactory;

/**
 * Used to get EndPoints for any supported protocol stack
 * 
 */
public class TransportFactoryImpl implements TransportFactory {
	TRANSPORT chosenTransport = TRANSPORT.valueOf(System.getProperty("transport", TRANSPORT.NETTY.name()));

	private static boolean initializedLogging = false;
	private static final Logger LOGGER = Logger.getLogger(TransportFactory.class);
	
	Map<String, EndPointFactory> protocolEPFactoryMap = new ConcurrentHashMap<String, EndPointFactory>();
	
	static String defaultProtocol = System.getProperty("com.liquidlabs.transport.default", "stcp");

	private LifeCycle.State state = LifeCycle.State.STOPPED;
	
	private final ScheduledExecutorService scheduler;

	public TransportFactoryImpl(ExecutorService executor, String serviceName) {
		this(com.liquidlabs.common.concurrent.ExecutorService.newScheduledThreadPool("services"), serviceName);
	}
	public TransportFactoryImpl(ScheduledExecutorService scheduler, String serviceName) {
		if (!initializedLogging) {
			initializedLogging = true;
			InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());
		}

		this.scheduler = scheduler;

		EndPointFactory epFactory = null;

		if (chosenTransport == null) throw new RuntimeException("TRANSPORT is NULL!");

		if (chosenTransport == TRANSPORT.NETTY) {
			epFactory = new NettyEndPointFactory(scheduler, serviceName);
		}
		if (chosenTransport == TRANSPORT.RABBIT) {
			epFactory = new RabbitEndpointFactory(RabbitEndpointFactory.getURL());
		}

		protocolEPFactoryMap.put("stcp", epFactory);
		protocolEPFactoryMap.put("tcp", epFactory);
	}

    Map<URI, EndPoint> endpoints = new HashMap<URI, EndPoint>();
	public synchronized EndPoint getEndPoint(URI receiverURI, Receiver receiver, boolean reuseEndpoint) {
		
		try {
		
			URI cleanURI = cleanURI(receiverURI);
			
			LOGGER.info("Opening:" + receiverURI + " shared:" + reuseEndpoint);
			//new RuntimeException("Opening:" + receiverURI).printStackTrace();
			System.err.println(this.hashCode() + " Opening:" + receiverURI + " shared:" + reuseEndpoint);
			if (reuseEndpoint) {
				EndPoint endPoint = endpoints.get(cleanURI);
				if (endPoint != null) {
					Receiver receiver2 = endPoint.getReceiver();
					if (receiver2 instanceof MultiReceiver) {
						MultiReceiver mr = (MultiReceiver) receiver2;
						LOGGER.info("Sharing PORTS:" + cleanURI + " ADDING/" + receiverURI  + " R:" + mr);
						mr.addReceiver(receiver);
						return endPoint;
					}
				}
			}
			
			EndPointFactory endPointFactory = protocolEPFactoryMap.get(cleanURI.getScheme());
			if (endPointFactory == null) {
				endPointFactory = protocolEPFactoryMap.get(defaultProtocol);
			}
			if (endPointFactory == null) {
				throw new RuntimeException("Did not recognise protocol:" + cleanURI.getScheme() + " - also failed on default scheme:" + defaultProtocol + " for URI:" + receiverURI + " available:" + protocolEPFactoryMap.keySet());
			}
			endPointFactory.start();
			
			if (reuseEndpoint) {
				LOGGER.info("Sharing PORTS:" + cleanURI + " /" + receiverURI);
				MultiReceiver mr = new MultiReceiver(cleanURI.toString());
				mr.addReceiver(receiver);
				EndPoint endPoint = endPointFactory.getEndPoint(receiverURI, mr);
				endpoints.put(cleanURI, endPoint);
				return endPoint;
			} else {
				return endPointFactory.getEndPoint(receiverURI, receiver);
			}
		} catch (Throwable t) {
			LOGGER.fatal("Failed create EndPoint:" + receiverURI + " ex:" + t.getMessage(), t);
			throw new RuntimeException(t);
			
		}
	}

	private URI cleanURI(URI uri) {
		try {
			return new URI(uri.getScheme() + "://" + uri.getHost() + ":" + uri.getPort());
		} catch (URISyntaxException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	public void start() {
		if (state == State.STARTED) return;
		state = State.STARTED;
		
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				try {
					LOGGER.info(TransportFactoryImpl.this.toString() + " Shutdown hook called");
					TransportFactoryImpl.this.stop();
				} catch (Throwable t) {
					LOGGER.error(t);
				}
			}
		});
	}

	public void stop() {
		if (state == State.STOPPED) return;
		synchronized (this) {
			state = State.STOPPED;
		}
		LOGGER.info("Shutting down now...");
		scheduler.shutdownNow();
		Collection<EndPointFactory> epFactories = this.protocolEPFactoryMap.values();
		for (EndPointFactory endPointFactory : epFactories) {
			try {
				endPointFactory.stop();
			} catch (Throwable t) {
				LOGGER.error(t);
			}
		}
		LOGGER.info("EPs killed:" + epFactories.size());
	}
	static public URI getDefaultProtocolURI(String path, String ipAddress, int port, String serviceName) throws URISyntaxException {
		if (path == null || path.length() == 0) path = "/";
		return new URI(defaultProtocol,null, ipAddress, port, path,"svc="+serviceName,null);
	}
	
	public Set<String> supportedProtocols() {
		return protocolEPFactoryMap.keySet();
	}
	
}
