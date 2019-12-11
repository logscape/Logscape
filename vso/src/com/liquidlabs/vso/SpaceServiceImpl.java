package com.liquidlabs.vso;

import java.io.File;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.liquidlabs.common.Logging;
import com.thoughtworks.xstream.XStream;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.PIDGetter;
import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.common.concurrent.NamingThreadFactory;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.jmx.JmxHtmlServerImpl;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.orm.ORMEventListener;
import com.liquidlabs.orm.ORMapperClient;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.space.ClientLeaseManager;
import com.liquidlabs.space.Space;
import com.liquidlabs.space.lease.Renewer;
import com.liquidlabs.transport.proxy.AddressUpdater;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.transport.proxy.Remotable;
import com.liquidlabs.transport.proxy.addressing.AddressHandler;
import com.liquidlabs.transport.proxy.addressing.AddressHandler.RefreshAddrs;
import com.liquidlabs.transport.proxy.events.Event;
import com.liquidlabs.transport.proxy.events.Event.Type;
import com.liquidlabs.transport.serialization.ObjectTranslator;
import com.liquidlabs.vso.lookup.AddressEventListener;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.ServiceInfo;

public class SpaceServiceImpl implements SpaceService {
	
	private Logger LOGGER = Logger.getLogger(SpaceServiceImpl.class);
	private static Logger SS_LOGGER = Logger.getLogger(SpaceServiceImpl.class);
    static Logging auditLogger = Logging.getAuditLogger("VAuditLogger", "SpaceService");
	
	private final LookupSpace lookup;
	private final ORMapperFactory mapperFactory;
	private ORMapperClient orMapper;
	private final ObjectTranslator query = new ObjectTranslator();
	private final String name;
	private final ClientLeaseManager leaseManager = new ClientLeaseManager();

	private ServiceInfo serviceInfo;
	LifeCycle.State state = State.ASSIGNED;

	private int timeout = VSOProperties.getLUSpaceServiceLeaseInterval();
	private final boolean persistent;
	boolean isClustered = true;
	private final ScheduledExecutorService scheduler;
	private SpaceServiceAdmin spaceServiceAdmin;
	private final String zone =  VSOProperties.getZone();
	
	volatile int puts;
	volatile int gets;
	volatile int takes;
	
	public SpaceServiceImpl(LookupSpace lookup, ORMapperFactory mapperFactory, String name, ScheduledExecutorService scheduler, boolean isClustered, boolean persistent, boolean isLastORMClientSoShutdown) {

        LOGGER.info("Creating:" + name + " hash:" + this.hashCode());
		this.lookup = lookup;
		this.mapperFactory = mapperFactory;
		this.name = name;
		this.scheduler = scheduler;
		this.isClustered = isClustered;

		this.persistent = persistent;
		leaseManager.setScheduler(ExecutorService.newScheduledThreadPool(5, new NamingThreadFactory("SpaceService-leasor")));
		LOGGER = Logger.getLogger(SpaceService.class.getCanonicalName()+"." + name);
	}
	
	public void addPeer(URI uri) {
		if (this.orMapper != null && uri.toString().length() > 0) {
			URI clientAddress = mapperFactory.getClientAddress();
			if (clientAddress.equals(uri)) {
				LOGGER.info("Ignoring (self) peer:" + uri);
				return;
			}
			LOGGER.info(this.name + "/" + mapperFactory.getClientAddress() + " Added Peer peer:" + uri);
			this.orMapper.addPeer(uri);
		}
	}

    public void removePeer(URI uri) {
        if (orMapper != null) {
            orMapper.removePeer(uri);
        }
    }

    public void start() {
        if (this.state == State.STARTED) return;
    	LOGGER.info("Start:" + this.name + " hash:" + this.hashCode());
		mapperFactory.start();
		this.state = State.STARTED;
	}

	public void start(Object object, String bundleName) {
		this.start();
		
		spaceServiceAdmin = new SpaceServiceAdmin(name, this, mapperFactory.getProxyFactory());
		
		LOGGER.info("Starting:" + name);
		orMapper = mapperFactory.getORMapperClient(name, object, isClustered, persistent);
        URI uri = mapperFactory.getClientAddress();
        final String address = uri.toString();
        serviceInfo = new ServiceInfo(name, address, null, JmxHtmlServerImpl.locateHttpUrL(), bundleName, spaceServiceAdmin.getUID(), zone, VSOProperties.getResourceType());
        
		if (isClustered){
            String replicationAddress = orMapper.getReplicationURI().toString();
            serviceInfo.setReplicationAddress(replicationAddress);
            try {
                SpaceServiceAddressListener listener = new SpaceServiceAddressListener(name);
                listener.setSpaceService(this);
                lookup.registerUpdateListener(listener.getId(), listener, name, name + "-" + uri.getHost() + "-" + uri.getPort(), name, zone, false);
            } catch (Exception e) {
                LOGGER.warn(String.format("Unable to register update listener for SpaceService %s. Replication to/from this node will be disabled", e));
            }
        }
        LOGGER.info(String.format("Registering [%s] %s cwd:%s", name, serviceInfo, FileUtil.getPath(new File("."))));
		final String lease = lookup.registerService(serviceInfo, timeout);
		
		leaseManager.manage(new Renewer(lookup, this, lease, timeout, name, LOGGER), VSOProperties.getLUSpaceServiceRenewInterval());
		
		// Bandaid to overcome partition events which can wipe out the ServiceInfo....somehow - need to dig deeper. 
		scheduler.scheduleAtFixedRate(new Runnable() {
			public void run() {
				try {
					lookup.registerService(serviceInfo, timeout);
				} catch (Throwable t) {
					LOGGER.warn("Register Failed:" + serviceInfo, t);
				}
			}
		}, 60 - new DateTime().getSecondOfMinute(), VSOProperties. getLUSpaceServiceLeaseInterval() * 10, TimeUnit.SECONDS);
		
	}

	public void stop() {
		if (this.state == State.STOPPED) return;
		this.state = State.STOPPED;
		LOGGER.info("Stopped:" + name + " hash:" + this.hashCode());
		try {
			leaseManager.stop();
			if (lookup != null && serviceInfo != null) {
				lookup.unregisterService(serviceInfo);
			}
			
		} catch (Throwable t) {
			LOGGER.warn(t.toString(), t);
		}
		// want to always make sure we stop otherwise if the process doesn't exist properly it may replicate with other spaces etc
//		if (isLastORMClientSoShutdown) {
//			LOGGER.info(name + " isLast == TRUE, so stopping ORM");
//			mapperFactory.stop();
//		}
	}
	public void setClustered(boolean isClustered) {
		this.isClustered = isClustered;
	}
	
	public <T> String registerListener(final Class<T> eventType, String filter, final Notifier<T> notifier, final String notifierId, int leasePeriod, final Event.Type [] types){
		if (orMapper == null) throw new RuntimeException("SpaceService:" + name + " was not STARTED, orMapper is null");
		try {
			ORMEventListener ormListener = new ORMEventListener(){
				int failure;
				public String getId() {
					return notifierId;
				}
				
				public void notify(String key, String payload, Type event, String source) {
					for (Type type : types) {
						if (event == type) {
							try {
								T result = query.getObjectFromFormat(eventType, payload);
								notifier.notify(event, result);
								failure = 0;
							} catch (Throwable t){
								LOGGER.error(String.format("Failed to notify listener[%s] Type[%s] event[%s] Msg[%s]", notifierId, eventType, event, t.getMessage(), t));
								if (++failure >= VSOProperties.getMaxNotifyFailures()) {
									LOGGER.error(String.format("UNRegisterListener because to too-many-failures [%s] Type[%s] event[%s] Msg[%s]", notifierId, eventType, event, t.getMessage(), t));
									orMapper.unregisterEventListener(notifierId);
								}
							}
						}
					}
				}
			};
			return orMapper.registerEventListener(eventType, filter, ormListener, types, leasePeriod);
		} catch (Throwable t) {
			LOGGER.warn(t.getMessage(), t);
			throw new RuntimeException(String.format("Failed to Register[%s]", notifierId), t);
		}
	}
	public void stopProxy(String id){
		if (orMapper == null) throw new RuntimeException("SpaceService:" + name + " was not STARTED, orMapper is null");
		mapperFactory.getProxyFactory().stopProxy(id);
	}

	public boolean unregisterListener(String listenerId) {
		if (orMapper == null) throw new RuntimeException("SpaceService:" + name + " was not STARTED, orMapper is null");
		boolean result = false;
		if (listenerId != null) { 
			result = orMapper.unregisterEventListener(listenerId);
			mapperFactory.getProxyFactory().unregisterMethodReceiver(listenerId);
		}

		return result;
	}
	
	public Set<String> keySet(Class<?> type) {
		gets++;
		String[] findIds = orMapper.findIds(type, "", -1);
		return new HashSet<String>(Arrays.asList(findIds));
	}

	public <T> String store(T object, int timeOutSecs) {
		if (!this.isStarted()) {
			LOGGER.error("NotRunning:" + name + ".store()");
            throw new RuntimeException("SpaceService:" + name + " hash:" + this.hashCode() + " is not STARTED");
		}
		puts++;
		if (orMapper == null) throw new RuntimeException("SpaceService:" + name + " was not STARTED, orMapper is null");
		return orMapper.store(object, timeOutSecs);
	}
	public <T> String silentStore(T object, int timeOut) {
		puts++;
		if (orMapper == null) throw new RuntimeException("SpaceService:" + name + " was not STARTED, orMapper is null");
		return orMapper.silentStore(object, timeOut);
	}

	public <T> List<T> findObjects(Class<T> type, String filter, boolean cascade, int max) {
		gets++;
		if (orMapper == null) throw new RuntimeException("SpaceService:" + name + " was not STARTED, orMapper is null");
		return orMapper.findObjects(type, filter, cascade, max);
	}

	public <T> T findById(Class<T> type, String id) {
		gets++;
		if (orMapper == null) throw new RuntimeException("SpaceService:" + name + " was not STARTED, orMapper is null");
		try {
			return orMapper.retrieve(type, id, false);
		} catch (Throwable t) {
			return null;
		}
	}
	public <T> String[] findIds(Class<T> type, String query) {
		gets++;
		if (orMapper == null) throw new RuntimeException("SpaceService:" + name + " was not STARTED, orMapper is null");
		return orMapper.findIds(type, query, -1);
	}

	public ProxyFactory proxyFactory() {
		return this.mapperFactory.getProxyFactory();
	}

    public String rawById(Class<?> type, String id) {
    	gets++;
    	if (orMapper == null) throw new RuntimeException("SpaceService:" + name + " was not STARTED, orMapper is null");
        return orMapper.stringValue(type, id);
    }

    public void importData(Class<?> type, String item) {
    	if (orMapper == null) throw new RuntimeException("SpaceService:" + name + " was not STARTED, orMapper is null");
        orMapper.importData(type, item);
    }

    public void cancelLease(String leaseKey) {
    	if (orMapper == null) throw new RuntimeException("SpaceService:" + name + " was not STARTED, orMapper is null");
    	orMapper.cancelLease(leaseKey);
	}
	
	public int updateMultiple(Class<?> type, String query, String updateStatement, int limit, int timeoutLease, String leaseKey) {
		if (orMapper == null) throw new RuntimeException("SpaceService:" + name + " was not STARTED, orMapper is null");
		return orMapper.updateMultiple(type, query, updateStatement, limit, timeoutLease, leaseKey);
	}
	public String update(Class<?> type, String id, String updateStatement, int timeoutLease) {
		if (orMapper == null) throw new RuntimeException("SpaceService:" + name + " was not STARTED, orMapper is null");
		puts++;
		return orMapper.update(type, id, updateStatement, timeoutLease);
	}
	public boolean containsKey(Class<?> type, String id) {
		gets++;
		if (orMapper == null) throw new RuntimeException("SpaceService:" + name + " was not STARTED, orMapper is null");
		return orMapper.containsKey(type, id);
	}

	public void renewLease(String leaseKey, int timeoutSeconds) {
		if (orMapper == null) throw new RuntimeException("SpaceService:" + name + " was not STARTED, orMapper is null");
		orMapper.renewLease(leaseKey, timeoutSeconds);
	}
	public void assignLeaseOwner(String leaseKey, String owner) {
		if (orMapper == null) throw new RuntimeException("SpaceService:" + name + " was not STARTED, orMapper is null");
		orMapper.assignLeaseOwner(leaseKey, owner);
	}
	public int renewLeaseForOwner(String owner, int timeoutSeconds) {
		if (orMapper == null) throw new RuntimeException("SpaceService:" + name + " was not STARTED, orMapper is null");
		return orMapper.renewLeaseForOwner(owner, timeoutSeconds);
	}

	public <T> T remove(Class<T> type, String id) {
		if (!this.isStarted()) {
			LOGGER.warn("NotRunning:" + name + ".remove()");	
			return null;
		}
		takes++;
		if (orMapper == null) throw new RuntimeException("SpaceService:" + name + " was not STARTED, orMapper is null");
		return orMapper.remove(type, id, false, -1, -1);
	}
	public <T> List<T> remove(Class<T> type, String query, int limit) {
		if (!this.isStarted()) {
			LOGGER.warn("NotRunning:" + name + ".remove()");
			return null;
		}
		takes++;
		if (orMapper == null) throw new RuntimeException("SpaceService:" + name + " was not STARTED, orMapper is null");
		return orMapper.removeObjects(type, query, false, -1, -1, limit);
	}
	public <T> int purge(Class<T> type, String template) {
		if (!this.isStarted()) {
			LOGGER.warn("NotRunning:" + name + ".purge()");
			return 0;
		}
		takes++;
		if (orMapper == null) throw new RuntimeException("SpaceService:" + name + " was not STARTED, orMapper is null");
		return orMapper.purge(type, template);
	}
	public int purge(List<?> entries) {
		takes++;
		if (orMapper == null) throw new RuntimeException("SpaceService:" + name + " was not STARTED, orMapper is null");
		return orMapper.purge(entries);
	}

	public String register() {
		return lookup.registerService(serviceInfo, timeout);
	}
	public String info() {
		return lookup.toString();
	}

	public int size() {
		return orMapper.size();
	}
	public int count(Class<?> type, String template) {
		gets++;
		if (orMapper == null) throw new RuntimeException("SpaceService:" + name + " was not STARTED, orMapper is null");
		return orMapper.count(type, template);
	}

	public URI getClientAddress() {
		return mapperFactory.getClientAddress();
	}
	public URI getReplicationURI() {
		if (orMapper == null) throw new RuntimeException("SpaceService:" + name + " was not STARTED, orMapper is null");
		return orMapper.getReplicationURI();
	}

    public Map<String, String> exportData() {
    	if (orMapper == null) throw new RuntimeException("SpaceService:" + name + " was not STARTED, orMapper is null");
        return orMapper.exportData();
    }
    
    public Map<String, Object> exportObjects(String filter) {
    	if (orMapper == null) throw new RuntimeException("SpaceService:" + name + " was not STARTED, orMapper is null");
    	Set<String> keySet = orMapper.keySet();
    	HashMap<String, Object> result = new HashMap<String, Object>();
    	for (String key : keySet) {
    		if (filter != null && filter.length() > 0 && !StringUtil.containsIgnoreCase(key, filter)) continue;
			Object o = orMapper.getObject(key);
			result.put(key, o);
		}
        return result;
    }
    public void importData2(Map<String, Object> fromXML, boolean merge, boolean overwrite) {
    	for (String key : fromXML.keySet()) {
            try {
                boolean exists = orMapper.getObject(key) != null;
                if (!exists || overwrite) orMapper.store(fromXML.get(key));
            } catch (Throwable t) {
                LOGGER.warn("Failed to Import:" + key, t);
            }
		}
    }

    public void importData(Map<String, String> data, boolean merge, boolean overwrite) {
    	if (orMapper == null) throw new RuntimeException("SpaceService:" + name + " was not STARTED, orMapper is null");
        orMapper.importData(data, merge, overwrite);
    }

	public void addReceiver(String id, Object listener) {
		if (mapperFactory == null) throw new RuntimeException("SpaceService:" + name + " was not STARTED, orMapper is null");
		this.mapperFactory.addReceiver(id, listener);
	}
	@SuppressWarnings("unchecked")
	public <Y extends Remotable> Y makeRemoteable(final Y object) {
		if (mapperFactory == null) throw new RuntimeException("SpaceService:" + name + " was not STARTED, orMapper is null");
		return (Y) mapperFactory.getProxyFactory().makeRemoteable(object);
	}


	// could change this to a notify
	public static String [] waitForAddress(String whoAmI, LookupSpace lookupSpace, String serviceName, String location, boolean isStrict) {
		String[] addresses = null;
        int waitCount = 0;
		do {
            // dont spam with wait messages
            if (waitCount++ < 10 || waitCount % 100 == 0) {
			    SS_LOGGER.info(String.format("%s is Waiting to get an address for %s, No Service(s) found for Zone:%s", whoAmI, serviceName, location));
            }
			try {
				Thread.sleep((long) (1000 + (Math.random() * 3000)));
			} catch (InterruptedException e) {
			}
			addresses = lookupSpace.getServiceAddresses(serviceName, location, isStrict);
			if (VSOProperties.isVERBOSELookups()) {
				if (addresses == null) {
					SS_LOGGER.info(String.format("VERBOSE_LOOKUP from %s / Service %s GOT NULL_Addresses", whoAmI, serviceName));
				} else {
					SS_LOGGER.info(String.format("VERBOSE_LOOKUP from %s / Service %s GOT Addresses:%s", whoAmI, serviceName, Arrays.toString(addresses)));
				}
			}
		} while(addresses == null || addresses.length == 0);
		SS_LOGGER.info(String.format("Got address:%s", Arrays.toString(addresses)));
		return addresses;
	}

	public static <T> T getRemoteService(String whoAmI, Class<T> type, final LookupSpace lookupSpace, ProxyFactory proxyFactory, final String serviceName, boolean waitForAddress, final boolean isStrictLocationMatch) {
		whoAmI += PIDGetter.getPID();
        String overrideZone = "overrideZone." + serviceName;
        final String zone = System.getProperty(overrideZone, VSOProperties.getZone());
		String[] addresses = lookupSpace.getServiceAddresses(serviceName, zone, isStrictLocationMatch);
		if (addresses == null || addresses.length == 0) {
			if (waitForAddress) {
				addresses  = waitForAddress(whoAmI, lookupSpace, serviceName, zone, isStrictLocationMatch);
			}
			if (!waitForAddress) {
				List<ServiceInfo> infos = lookupSpace.findService("");
				SS_LOGGER.warn(whoAmI + " DID NOT Get Any Address for:" + serviceName);
				SS_LOGGER.warn(whoAmI + " All Available Addresses:" + infos);
			}
		}

		AddressUpdater listener = new AddressEventListener(serviceName, whoAmI);
		
		final RefreshAddrs refreshTask = new AddressHandler.RefreshAddrs() {
			public String[] getAddresses() {
                // If we get a null lookup space the bounce the agent and wait for the Manager to start
                if (lookupSpace == null) {
                    auditLogger.emit("LookupSpace is NULL, Forcing Bounce","");
                    SS_LOGGER.info("LookupSpace is NULL, Forcing Bounce");
                    System.out.println(new Date() + " LookupSpace is NULL, Forcing Bounce EXIT:10");
                    System.exit(10);
                }
				auditLogger.emit("RefreshingEndpoints", serviceName);
				SS_LOGGER.warn(VSOProperties.HA_TAG + ">>>>>>>>>  Trying to Refresh Endpoints for:" + serviceName + " from:" + lookupSpace);
				String[] serviceAddresses = lookupSpace.getServiceAddresses(serviceName, zone, isStrictLocationMatch);
				if (serviceAddresses != null && serviceAddresses.length == 0) {
					if (serviceAddresses != null) {
						auditLogger.emit("RefreshingEndpoints", serviceName).emit("Length", serviceAddresses.length + "");
					}
					SS_LOGGER.warn("Failed to get addressList for:" + serviceName + " from:" + lookupSpace + " strict:" + isStrictLocationMatch);
				}
				SS_LOGGER.warn(VSOProperties.HA_TAG + " service:" + serviceName + " <<<<<<<<< Given EPS:" + Arrays.toString(serviceAddresses));
				return serviceAddresses;
			}
		};
		
		T remoteService = proxyFactory.getRemoteService(serviceName, type, addresses, listener, refreshTask);
		//TODO: this is a bit of a hack - but - the Update Listener (also a ProxyClient) - will stamp out the main ProxyClient
		// so when we list them via JMX the original is missing
		proxyFactory.registerProxyClient(serviceName, remoteService);
		
		URI address = proxyFactory.getAddress();
		SS_LOGGER.info(String.format(VSOProperties.HA_TAG +"Using Space:%s ProxyAddress [%s] for Service[%s] Strict:" + isStrictLocationMatch, Arrays.asList(addresses != null ? addresses : new String[0]).toString(), address, serviceName));
		try {
			if (addresses.length == 0) SS_LOGGER.info(whoAmI + " ProxyIsWaiting:" + remoteService);
					
			
			lookupSpace.registerUpdateListener(listener.getId(), listener, serviceName, serviceName + "-" + address.getHost() + "-" + address.getPort(), whoAmI, zone, isStrictLocationMatch);
		} catch (Throwable e) {
			SS_LOGGER.error(e.getMessage(), e);
		}
		return remoteService;
	}
	public ScheduledExecutorService getScheduler() {
		return scheduler;
	}
	public SpaceServiceAdmin getSpaceServiceAdmin() {
		return spaceServiceAdmin;
	}
	public ServiceInfo getServiceInfo() {
		return serviceInfo;
	}
	public boolean isStarted() {
		return state == LifeCycle.State.STARTED;
	}
	public Space getSpace() {
		return orMapper.getSpace();
	}

	public void registrationFailed(int failedCount) {
		LOGGER.fatal(name + " CANNOT Maintain lease!!");
	}

    @Override
    public String exportObjectAsXML(String filter, String CONFIG_START, String CONFIG_END) {

        Map<String, Object> map = exportObjects(filter);
        List<String> keys = new ArrayList<String>(map.keySet());
        Map<String, Object> result = new TreeMap<String, Object>();
        for (String key : keys) {
            if (filter.contains(filter)) {
                Object value = map.get(key);
                result.put(key, value);
            }
        }
        return CONFIG_START + "\n" + new XStream().toXML(result) + "\n" + CONFIG_END;
    }
    public void importFromXML(String xmlConfig, boolean merge, boolean overwrite, String CONFIG_START, String CONFIG_END) {
        String config = xmlConfig.substring(xmlConfig.indexOf(CONFIG_START), xmlConfig.indexOf(CONFIG_END));
        Map<String, Object> fromXML = (Map<String, Object>) new XStream().fromXML(config);
        importData2(fromXML, merge, overwrite);
    }

	public ORMapperFactory getMapperFactory() {
		return mapperFactory;
	}
}
