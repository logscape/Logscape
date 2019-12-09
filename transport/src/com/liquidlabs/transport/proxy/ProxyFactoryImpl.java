package com.liquidlabs.transport.proxy;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.log4j.Logger;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.EndPoint;
import com.liquidlabs.transport.Receiver;
import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.TransportFactoryImpl;
import com.liquidlabs.transport.TransportProperties;
import com.liquidlabs.transport.proxy.addressing.AddressHandler;
import com.liquidlabs.transport.proxy.addressing.KeepFirstShuffleRestAddresser;
import com.liquidlabs.transport.proxy.addressing.KeepOrderedAddresser;
import com.liquidlabs.transport.proxy.addressing.AddressHandler.RefreshAddrs;
import com.liquidlabs.transport.serialization.Convertor;

@SuppressWarnings("serial")
public class ProxyFactoryImpl implements ProxyFactory {
	private static final Logger LOGGER = Logger.getLogger(ProxyFactoryImpl.class);
	private ScheduledExecutorService scheduler;
	protected ExecutorService executor;


	private EndPoint endPointServer;
	private PeerHandler peerHandler;
	private URI address;
	private Convertor convertor;
	private State state = LifeCycle.State.STOPPED;
	private long startTime = System.currentTimeMillis();
	
	public enum ADDRESSING { KEEP_ORDERED, KEEP_FIRST_RANDOM }
	
	public Map<ADDRESSING, Class<? extends AddressHandler>> addressHandlers = new HashMap<ADDRESSING, Class<? extends AddressHandler>>(){
	{
		put(ADDRESSING.KEEP_ORDERED, KeepOrderedAddresser.class);
		put(ADDRESSING.KEEP_FIRST_RANDOM, KeepFirstShuffleRestAddresser.class);
	}};
	protected TransportFactory transportFactory;
	
	Map<String, ProxyClient<?>> clients = new ConcurrentHashMap<String, ProxyClient<?>>();
	Set<String> clientString = new CopyOnWriteArraySet<String>();
	int clientHitCount;
	
	private ProxyFactoryAdminImpl admin;

	synchronized public boolean stopProxy(Object proxyObject) {
		
		try {
			String id = "";
			boolean unregistered = false;
			ProxyClient<?> givenProxyClient = null;
			if (proxyObject instanceof String) {
				givenProxyClient = clients.get((String) proxyObject);
				id = (String) proxyObject;
				unregisterMethodReceiver((String) proxyObject);
			} else {
				givenProxyClient = getProxyClient(proxyObject);
				try {
					if (givenProxyClient != null) {
						id = givenProxyClient.getId();
					} else {
						id = getIdMethod(proxyObject); 
					}
					
					if (id != null && id.length() > 0) {
						unregistered = unregisterMethodReceiver(id);
					}
				} catch (Throwable t) {
					LOGGER.error("Unregister Failed:" + proxyObject.toString() ,t);
				}
			}
			
			if (id == null || id.length() == 0) {
				LOGGER.warn("Didnt find client:" + proxyObject.toString() + " P:" + givenProxyClient + " \n\tClients:" + this.clients.keySet());
				return false;
			}
			
			ProxyClient<?> proxyClient = clients.get(id);
			
			if (proxyClient == null) {
				LOGGER.warn(String.format("ProxyClient[%s] NotFound%s", id, clients.keySet()));
				// if it was unregistered then the value can be null
				return unregistered;
			} else {
				LOGGER.debug("Stopping:" + id);
				proxyClient.stop();
				clients.remove(id);
				boolean remove2 = clientString.remove(id);
				if (LOGGER.isDebugEnabled()) LOGGER.debug("ProxyFactory Removing Proxy:" + id + " removed:" + remove2);
				return unregistered || remove2;
			}
		} catch (Throwable t) {
			return false;
		}
	}

	private String getIdMethod(Object proxyObject) {
		try {
			Method method = Remotable.class.getMethod("getId");
			return (String) method.invoke(proxyObject);
		} catch (Exception e) {
		}
		return null;
	}

	private ProxyClient<?> getProxyClient(Object proxyObject) {
		try {
			if (proxyObject instanceof ProxyClient) return (ProxyClient<?>) proxyObject;
			return (ProxyClient<?>) Proxy.getInvocationHandler(proxyObject);
			// throws this when a non-proxy object is being gotten
		} catch (IllegalArgumentException ex) {
			return null;
		}
	}

	public ProxyFactoryImpl(int port, ExecutorService executor, String serviceName) throws URISyntaxException {
		this(new TransportFactoryImpl(executor, serviceName),
				TransportFactoryImpl.getDefaultProtocolURI(null, NetworkUtils.getIPAddress(), NetworkUtils.determinePort(port), serviceName), 
				executor, serviceName);
	}
	public ProxyFactoryImpl(TransportFactory transportFactory, int port, ExecutorService executor, String serviceName) throws UnknownHostException, URISyntaxException {
		this(transportFactory, TransportFactoryImpl.getDefaultProtocolURI(null, NetworkUtils.getIPAddress(), port, serviceName), executor, serviceName);
	}
	public ProxyFactoryImpl(TransportFactory transportFactory, URI uri, ExecutorService executor, String serviceName) {
		if (uri.getHost().equals("localhost")) {
			try {
				uri = new URI(uri.getScheme(), uri.getUserInfo(), NetworkUtils.getIPAddress(), uri.getPort(), uri.getPath(), uri.getQuery(), uri.getFragment());
			} catch (URISyntaxException e) {
				LOGGER.error(e);
			}
		}

		this.transportFactory = transportFactory;
		this.executor = executor;
		scheduler = com.liquidlabs.common.concurrent.ExecutorService.newScheduledThreadPool(TransportProperties.getProxySchedulerPoolSize(), "PF-" + serviceName);
		initEndPointServer(uri, serviceName);
		
		admin = null;// new ProxyFactoryAdminImpl(this);
		
	}

	private void initEndPointServer(URI address, String serviceName) {
		int retryCount = 0;
		boolean started = false;
		Throwable lastException = null;
		
		int port = address.getPort();
		while (retryCount++ < 6 && !started) {
			try {
				lastException = null;
				this.address = new URI(address.getScheme(), address.getUserInfo(), address.getHost(), port, address.getPath(), address.getQuery() + "&host=" +NetworkUtils.getHostname(), address.getFragment());
				
				LOGGER.info("Starting EPService:" + this.address);
				//new RuntimeException(this.address.toString()).printStackTrace();
				
				if (address.getHost().contains("127.0.0.1")) {
					String msg = String.format("**** Cannot resolve hostname(%s) to valid IP address, it resolves to 127.0.0.1. \n\n\t **Check this machines DNS resolution of the hostname**", address.getHost());
					LOGGER.fatal(msg);
					LOGGER.error(msg);
					System.err.println(msg);
					//System.exit(0);
				}
				
				convertor = new Convertor();
				peerHandler = new PeerHandler(convertor, scheduler, executor, address.toString(), serviceName);
				peerHandler.setClientNotifier(new ClientNotifier() {
					public void notify(ProxyClient client) {
						if (LOGGER.isDebugEnabled()) LOGGER.debug(this.hashCode() + " Attaching to:" + client.toSimpleString());
						clients.put(client.getId(), client);
					}
				});
				endPointServer = transportFactory.getEndPoint(this.address, peerHandler, true);
				
				peerHandler.setSender(endPointServer);
				started = true;
			} catch (Throwable t){
				LOGGER.warn("InitEPServerFailed", t);
				lastException = t;
				
				LOGGER.info("Failing to setup Server, going to retry:" +t);
				try {
					Thread.sleep((long) (100 * Math.random()));
				} catch (InterruptedException e) {
				}
			}
		}
		if (endPointServer == null || lastException != null) {
			throw new RuntimeException("EndPointServer is null, retryCount:" + retryCount + " lastException:" + lastException, lastException);
			
		}
		if (endPointServer.getAddress() == null) {
			LOGGER.error("Failing to start Server - address is null - UDPServerAddress");
		}
	}

	public <T> T getRemoteService(final String listenerId, Class<T> interfaceClass, String...endPoints) {
		return this.getRemoteService(listenerId, interfaceClass, endPoints, null, ADDRESSING.KEEP_ORDERED, null);
	}
	public <T> T getRemoteService(String listenerId, Class<T> interfaceClass, String[] endPoints, AddressUpdater updater) {
		return this.getRemoteService(listenerId, interfaceClass, endPoints, updater, ADDRESSING.KEEP_ORDERED,  null);
	}
	public <T> T getRemoteService(final String listenerId, Class<T> interfaceClass, ADDRESSING addressing, String...endPoints) {
		return this.getRemoteService(listenerId, interfaceClass, endPoints, null, addressing, null);
	}
	public <T> T getRemoteService(final String listenerId, Class<T> interfaceClass, String[] endPoints, AddressUpdater updater, RefreshAddrs refreshTask) {
		return this.getRemoteService(listenerId, interfaceClass, endPoints, updater, ADDRESSING.KEEP_ORDERED, refreshTask);
	}
	public void registerProxyClient(String givenId, Object object) {
		ProxyClient<?> proxyClient = getProxyClient(object);
		this.clients.put(givenId + "_" + proxyClient.getId(), proxyClient);
	}
	synchronized public <T> T getRemoteService(String listenerId, Class<T> interfaceClass, String[] endPoints, AddressUpdater updater, ADDRESSING addressHandler, RefreshAddrs refreshTask) {
		if (state != State.STARTED) throw new RuntimeException("ProxyFactoryImpl State != STARTED, state:" + state);

        if (endPoints.length > 0) resolveHost(endPoints[0]);

//        LOGGER.warn("------------------" + listenerId);

		AddressHandler addresserHandla = getAddresser(addressHandler, listenerId + "/" + interfaceClass);
		addresserHandla.registerAddressRefresher(refreshTask);
		final ProxyClient<T> proxyClient = new ProxyClient<T>(interfaceClass, endPointServer.getAddress(), endPoints, listenerId, peerHandler, convertor, addresserHandla, executor, this);
		if (updater != null) {
			updater.setProxyClient(proxyClient);
			proxyClient.setId(updater.getId());
//			updater.setId(proxyClient.getId());
//			LOGGER.warn("     22------------------" + updater.getId());
			peerHandler.addMethodReceiver(updater.getId(), updater);
		}
		
		this.clients.put(proxyClient.getId(), proxyClient);
		proxyClient.start();
		T client = proxyClient.getClient();
		this.clientString.add(proxyClient.getId());
		
		removeOldClientsIfTheyLeak();
		
		return client;
	}

	private void removeOldClientsIfTheyLeak() {
		Set<String> keySet = this.clients.keySet();
		if (keySet.size() > Integer.getInteger("proxy.max", 5000)) {
			try {
				LOGGER.info("Got too many clients:" + keySet.size());
				ArrayList<ProxyClient<?>> clients = new ArrayList<ProxyClient<?>>(this.clients.values());
				Collections.sort(clients, new Comparator<ProxyClient>(){
	
					public int compare(ProxyClient arg0, ProxyClient arg1) {
						return arg1.lastUsed().compareTo(arg0.lastUsed());
					}
				});
				ProxyClient<?> proxyClient = clients.get(0);
				LOGGER.info("Killing Old Proxy:" + proxyClient);
				this.stopProxy(proxyClient);
			} catch (Throwable t) {
				LOGGER.warn("Failed to kill client:", t);
			}
		}
		
	}

	private void resolveHost(String endPoint) {
		try {
			URI uri = new URI(endPoint);
			InetAddress.getAllByName(uri.getHost());
		} catch (Throwable t) {
			throw new RuntimeException("Failed to resolveHost:" + endPoint, t);
		}
	}

	private AddressHandler getAddresser(ADDRESSING addressHandler, String serviceName){
		try {
			return addressHandlers.get(addressHandler).getConstructor(String.class).newInstance(serviceName);
		} catch (Throwable t) {
			LOGGER.warn("Using DefaultAddressing, Failed to created addresses:" + addressHandler, t);
			return new KeepOrderedAddresser(serviceName);
		}
	}

	public void registerContinuousEventListener(String listenerId, ContinuousEventListener eventListener) {
		peerHandler.addContinuousEventListener(listenerId, eventListener);
	}

	/**
	 * Allow peers to invoke on this object using the listenerId
	 * 
	 * @param listenerId
	 * @param target
	 */
	public void registerMethodReceiver(String listenerId, Object target) {
		if (LOGGER.isDebugEnabled()) LOGGER.debug(this.address + " ****** ADDING: listenerId:" + listenerId);
		peerHandler.addMethodReceiver(listenerId, target);
	}
	public void registerMethodReceiver(Remotable target) {
		String listenerId = getObjectIdFromRemotable(target);
		if (LOGGER.isDebugEnabled()) LOGGER.debug("ADDING: listenerId:" + listenerId);
		peerHandler.addMethodReceiver(listenerId, target);
	}

	
	public boolean unregisterMethodReceiver(String listenerId) {
		if (listenerId != null) {
			if (peerHandler != null) peerHandler.removeMethodReceiver(listenerId);
			ProxyClient<?> proxyClient = clients.remove(listenerId);
			if (proxyClient != null) proxyClient.stop();
		}

		return true;
	}
	public void unregisterMethodReceiver(Remotable remotable) {
		if (remotable == null) return;
        String objectId = getObjectIdFromRemotable(remotable);
        peerHandler.removeMethodReceiver(objectId);
        clientString.remove(objectId);
        ProxyClient<?> proxyClient = clients.remove(objectId);
        if (proxyClient != null) proxyClient.stop();
    }
	
	/**
	 * Do NOT Use lots of these! they chew up NW bandwidth
	 * @param listenerId
	 */
	public void publishAvailability(String listenerId){
		if (LOGGER.isInfoEnabled()) LOGGER.info("Publishing availability:" + getAddress() + "/" + listenerId);
	}
	
	public void start() {
		if (this.state == State.STARTED) return;
		this.state = State.STARTED;
		endPointServer.start();
		transportFactory.start();
	}
	

	@SuppressWarnings("unchecked")
	synchronized public void stop() {
		
		if (this.state == State.STOPPED) return;
		if (LOGGER.isInfoEnabled()) LOGGER.info(">> Stopping:" + toString());
		this.state = State.STOPPED;
		
		for (ProxyClient client : clients.values()) {
			client.stop();
		}
		clients.clear();
		clients = null;
		peerHandler.stop();
		peerHandler = null;
		if (endPointServer != null) {
			endPointServer.stop();
			endPointServer = null;
		}
		// dont shutdown injected deps
//		if (transportFactory != null) {
//			transportFactory.stop();
//			transportFactory = null;
//		}
		
		if (executor != null && !executor.isShutdown()) executor.shutdownNow();
		if (scheduler != null && !scheduler.isShutdown()) scheduler.shutdownNow();
		if (LOGGER.isInfoEnabled()) LOGGER.info("<< Stopped:" + toString());
	}

	public URI getAddress() {
		return getAddress("");
	}
	public URI getAddress(String path) {
		try {
			String query = "";//String.format("&_startTime=%s", DateUtil.shortDateTimeFormat5.print(startTime));
			URI uri = new URI(address.getScheme(), address.getUserInfo(), NetworkUtils.getIPAddress(), address.getPort(), address.getPath() + "/" + path, address.getQuery() + query, address.getFragment());
			return uri;
			//return new URI(String.format("%s?%s_startTime=%s&udp=%d", address, path, DateUtil.shortDateTimeFormat5.print(startTime), udpPort));
		} catch (Exception e) {
			LOGGER.error("Path:" + path, e);
		}
		return address;
	}

	public EndPoint getEndPointServer() {
		return endPointServer;
	}

	public String getEndPoint() {
		return getAddress().toString();
	}


	public ScheduledExecutorService getScheduler() {
		return scheduler;
	}
	
	public ExecutorService getExecutor() {
		return executor;
	}
	
	public Set<String> supportedProtocols() {
		return transportFactory.supportedProtocols();
	}
	public String toString() {
		return getClass().getSimpleName() + "@" + super.hashCode() + " " +  getEndPoint();
	}

	@SuppressWarnings("unchecked")
	public <T extends Remotable> T makeRemoteable(final Remotable userObject) {
		

		String objectId = getObjectIdFromRemotable(userObject);
		
		registerMethodReceiver(objectId, userObject);
		
		Class<?>[] interfaces = userObject.getClass().getInterfaces();
		if (interfaces.length == 0) {
			interfaces = userObject.getClass().getSuperclass().getInterfaces();
		}
		if (interfaces.length == 0) {
			throw new RuntimeException("Cannot extract interface from:" + userObject.getClass());
		}
		Class<T> interfase = (Class<T>) getRemotableInterface(interfaces);
		try {
			return (T) getRemoteService(objectId, interfase, this.getAddress().toString());
		} catch (Throwable t) {
			throw new RuntimeException("Failed to make Remotable:" + userObject + " Interface:" + interfase);
		}
	}

	private String getObjectIdFromRemotable(final Remotable userObject) {
		String objectId = null;//userObject.getClass().getSimpleName() + userObject.hashCode() + "_" + PIDGetter.getPID();
		
		// allow the Id() to be set.overridden by a getId() implementation
		try {
			Class<?>[] interfaces = userObject.getClass().getInterfaces();
			for (Class<?> class1 : interfaces) {
				try {
					Method method = class1.getMethod("getId");
					method.setAccessible(true);
					objectId = (String) method.invoke(userObject);
					return objectId;
				} catch (Throwable t) {
				}
				
			}
			try {
				Method method = userObject.getClass().getMethod("getId");
				method.setAccessible(true);
				objectId = (String) method.invoke(userObject);
				return objectId;
			} catch (Throwable t) {
			}

			//new Method(c, objectId, null, c, null, clientHitCount, clientHitCount, objectId, null, null, null);
		} catch (Throwable t) {
		}
		return objectId;
	}

	private Class<?> getRemotableInterface(Class<?>[] interfaces) {
		for (Class<?> class1 : interfaces) {
			if (Remotable.class.isAssignableFrom(class1)) return class1;
		}
		throw new RuntimeException("Failed to extract remotable interface from:" + interfaces);
	}

	public Receiver getReceiver() {
		return peerHandler;
	}

	public PeerHandler getPeerHandler() {
		return peerHandler;
	}
}
