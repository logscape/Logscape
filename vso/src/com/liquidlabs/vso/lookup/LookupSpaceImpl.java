package com.liquidlabs.vso.lookup;

import java.io.FileOutputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.liquidlabs.common.*;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.format.DateTimeFormat;

import com.liquidlabs.common.concurrent.NamingThreadFactory;
import com.liquidlabs.common.jmx.JmxHtmlServerImpl;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.orm.ORMEventListener;
import com.liquidlabs.orm.ORMapperClient;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.proxy.AddressUpdater;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.transport.proxy.events.Event.Type;
import com.liquidlabs.transport.serialization.ObjectTranslator;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.VSOProperties.ports;
import com.liquidlabs.vso.agent.EmbeddedServiceManager;
import com.liquidlabs.vso.agent.ResourceProfile;

/**
 * lookupSpaceA.registerServiceLocation("myService-1", "tcp://xxx", -1);
 * lookupSpaceA.registerServiceLocation("myService-2", "tcp://xxx", -1);
 * lookupSpaceA.registerServiceLocation("myService-3", "tcp://xxx", -1);
 * String[] serviceAddresses = lookupSpaceA.getServiceAddresses("myService");
 */
public class LookupSpaceImpl implements LookupSpace {
    static Logging auditLogger = Logging.getAuditLogger("VAuditLogger", "LookupSpace");
	
	// will be used by other services run in the same process space - so it depend on the boot(-forked).bundle - if in the 
	// same process they will know it is not clustered
	public static ORMapperFactory sharedMapperFactory;

    private final static Logger LOGGER = Logger.getLogger(LookupSpace.class);

    LifeCycle.State state = LifeCycle.State.STOPPED;

    ORMapperClient ormClient;
    ORMapperFactory mapperFactory;
    ObjectTranslator query = new ObjectTranslator();
    final Integer syncIntervalSeconds = VSOProperties.getLUAddressSyncInterval();
    final boolean uniqueAddrSyncUpdates = VSOProperties.getAddrSyncSuppressRepeats();
    private ServiceInfo serviceInfo;
    private String startTime = Calendar.getInstance().getTime().toString();

    private Map<String, AddressSyncher> addressListeners = new ConcurrentHashMap<String, AddressSyncher>();

    private TransportFactory transportFactory;

	private ScheduledExecutorService scheduler;
	private ScheduledExecutorService leaseScheduler = Executors.newScheduledThreadPool(1, new NamingThreadFactory("LU-LEASE-SCHED"));

    public LookupSpaceImpl(ORMapperClient orMapper) {
        auditLogger.emit("Created","orm");
        this.ormClient = orMapper;
    }

    public LookupSpaceImpl(int port, int replicationPort) {
        auditLogger.emit("Created","orm port:" + port);
    	LOGGER.info("Constructing:" + port + "/" + replicationPort);
        mapperFactory = new ORMapperFactory(port, NAME, 10 * 1024, replicationPort);
        transportFactory = mapperFactory.getTransportFactory();

        ormClient = mapperFactory.getORMapperClient(NAME, this, true, false);
        mapperFactory.publishServiceAvailability(NAME);
        scheduler = mapperFactory.getScheduler();
    }
    public static synchronized ORMapperFactory getSharedMapperFactory() {
    	if (sharedMapperFactory == null) {
    		sharedMapperFactory = new ORMapperFactory(VSOProperties.getPort(ports.SHARED), "SHARED", -1, VSOProperties.getREPLICATIONPort(ports.SHARED));
    	}
    	return sharedMapperFactory;
    }

    public URI getEndPoint() {
        return mapperFactory.getClientAddress();
    }

    public String ping(String agentRole, long agentTime) {
        if (VSOProperties.isFailoverNode()) {
            long diff = Math.abs(System.currentTimeMillis() - agentTime);
            if (diff > 30 * 1000) {
                System.err.println("ERROR - Detected CLOCK-DRIFT beween Manager and Failover of :" + diff + "ms");
                System.err.println("Correct the SYNC and restart the system ASAP");
                LOGGER.error("Detected CLOCK-DRIFT beween Manager and Failover of :" + diff + "ms");
                LOGGER.error("Correct the SYNC and restart the system ASAP");
            }
        }
        return startTime;
    }

    /**
     * Return UTC time MS
     */
    public long time() {
        return DateTimeUtils.currentTimeMillis();
    }

    public void start() {
        auditLogger.emit("Start","");
        if (this.state == LifeCycle.State.STARTED) return;
        LOGGER.info("Starting:LookupSpace" + this.getEndPoint().toString());
        this.state = LifeCycle.State.STARTED;

        final ServiceInfo lookupServiceInfo = new ServiceInfo(LookupSpace.NAME, this.getEndPoint().toString(), LookupSpace.class.getName(), JmxHtmlServerImpl.locateHttpUrL(), "boot-1.0", "", VSOProperties.getZone(), VSOProperties.getResourceType());

        final String lease = this.registerService(lookupServiceInfo, VSOProperties.getLUSpaceServiceLeaseInterval());
        this.setServiceInfo(lookupServiceInfo);

        leaseScheduler.scheduleAtFixedRate(new Runnable() {
            String myLease = lease;

            public void run() {
                try {
                    renewLease(myLease, VSOProperties.getLUSpaceServiceLeaseInterval());
                } catch (Exception e) {
                    LOGGER.warn("Failed to renew myLease:" + lease + " ex:" + e.getMessage(), e);
                    myLease = registerService(lookupServiceInfo, VSOProperties.getLUSpaceServiceLeaseInterval());
                }
            }

            ;
        }, VSOProperties.getLUSpaceServiceRenewInterval(), VSOProperties.getLUSpaceServiceRenewInterval(), TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                LOGGER.warn("Stopping:" + LookupSpaceImpl.this.toString());
                try {
                    Thread.sleep(1000);
                } catch (Throwable t) {
                }
                LookupSpaceImpl.this.stop();
            }
        });
        
        scheduler.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                syncAddressListeners();
            }
        }, 1, syncIntervalSeconds, TimeUnit.SECONDS);
        
        scheduler.scheduleAtFixedRate(new Runnable() {
            public void run() {
                if (resourceProfile == null) {
                    resourceProfile = new ResourceProfile();
                    resourceProfile.oneOffUpdate();
                }
                resourceProfile.logStats(LookupSpace.class, LOGGER);
            }

            ;
        }, 60 - new DateTime().getSecondOfMinute(), 5 * 60, TimeUnit.SECONDS);
        
        
        addClusterPeer();
    }

	private void addClusterPeer() {
		if (!VSOProperties.isManagerOnly()) {
			final String startTime = new DateTime().toString();
			try {
	        	final String lookupAddress = VSOProperties.getLookupAddress();
	        	
	        	// Assume the Manager is replicating on 15000
	        	final String lookupReplicator = lookupAddress.replaceAll("11000", VSOProperties.getManagerReplicationPort());
	        	
	        	final URI myuri = new URI("stcp://" + NetworkUtils.getIPAddress() + ":"+ VSOProperties.getReplicationPort() + "?startedAt" + startTime);
	        	LOGGER.info("Adding Cluster PEER, Manager:" + lookupAddress + " Self:" + myuri);
	        	final LookupSpace managerLookup = LookupSpaceImpl.getLookRemoteSimple(lookupAddress, mapperFactory.getProxyFactory(), "FailoverBOOT");
	        	managerLookup.addLookupPeer(myuri);
	        	try {
					this.addLookupPeer(new URI(lookupReplicator));
				} catch (URISyntaxException e) {
					e.printStackTrace();
				}
				// now keep track of the manager, if it goes down and then we need to reregister when it comes back.
				Thread thread = new Thread("ManagerIsAlive"){
					boolean managerAlive = true;
					public void run() {
						while (true) {
							try {
								Thread.sleep(10 * 1000);
								managerLookup.ping(VSOProperties.getResourceType(), System.currentTimeMillis());
								// it was dead - and now its now.... addPeersAgain
								if (!managerAlive) {
									LOGGER.error("********************** Manager is now ALIVE: Going to addPeer");
									managerAlive = true;
									managerLookup.addLookupPeer(myuri);
									LookupSpaceImpl.this.addLookupPeer(new URI(lookupReplicator + "?" + new DateTime()));
								}
							} catch (InterruptedException ie) {
								
							} catch (Throwable t) {
								if (managerAlive) LOGGER.error("*************** Manager DIED:" + t.getMessage());
								managerAlive = false;
							}
						}
					}
					
				};
				thread.setDaemon(true);
				thread.start();
			} catch (Throwable t) {
				LOGGER.error("Failed to AddPeer to Management:" + VSOProperties.getLookupAddress(), t);
			}
        }
	}

    public void stop() {
        auditLogger.emit("Stop","");
    	LOGGER.info("Stopping:LookupSpace");
        if (this.state == LifeCycle.State.STOPPED) {
        	LOGGER.info("Already Stopped");
        	return;
        }
        LOGGER.info("Stopped");
        this.state = LifeCycle.State.STOPPED;
        addressListeners.clear();
        if (this.serviceInfo != null) this.unregisterService(serviceInfo);
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {}
        if (mapperFactory != null) {
            try {
                mapperFactory.stop();
            } catch (Throwable t) {
            	LOGGER.warn(t.toString());
            }
        }
    }

    public String registerService(ServiceInfo serviceInfo, long timeout) {
        auditLogger.emit("Register",serviceInfo.toString());
        serviceInfo.setInsertTime(DateTimeFormat.shortDateTime().print(DateTimeUtils.currentTimeMillis()));
        LOGGER.info(String.format("\n\n\t\t%s RegisterService:%s Lease:%s Loc:%s",LookupSpace.NAME, serviceInfo, "xxx", serviceInfo.zone));
        String store = ormClient.store(serviceInfo, (int) timeout);
        return store;
    }

    public void renewLease(String serviceLocationLease, int expires) throws Exception {
    	if (VSOProperties.isVERBOSELookups()) LOGGER.info(String.format("Renewing lease for Key:%s Expires %d", serviceLocationLease, expires));
    	try {
    		ormClient.renewLease(serviceLocationLease, expires);
    	} catch (Throwable t) {
    		LOGGER.error("Failed to find Lease for:" + serviceLocationLease, t);
    	}

    }

    public boolean unregisterService(ServiceInfo serviceToRemove) {
        auditLogger.emit("UnRegister", serviceInfo.toString());
        LOGGER.info(LookupSpace.NAME + " UnregisterService:" + serviceToRemove);
        List<ServiceInfo> removeObjects = ormClient.removeObjects(ServiceInfo.class, String.format("locationURI equals %s", serviceToRemove.locationURI), false, -1l, -1l, 10);
        LOGGER.info("Removed Service:" + removeObjects);
        return true;
    }


	public void registerUpdateListener(final String addressListenerId, final AddressUpdater addressListener, final String template, final String resourceId, final String contextInfo, final String listenerZone, final boolean isStrictLocationMatch) {

        final String listenerId = "UPDList_" + resourceId + "_" + addressListenerId + "_" + contextInfo + "_" + listenerZone;
        auditLogger.emit("RegisterServiceListener","Zone:" + listenerZone + " id:" + listenerId);

        LOGGER.info(String.format("\n\n\t\t%s * RegisterUpdateListener[%s][%s] temp[%s] zone[%s]",LookupSpace.NAME, listenerId, addressListener.toString(), template, listenerZone));

        ORMEventListener eventListener = new ORMEventListener() {
            boolean isThisProxyWorking = true;

            public String getId() {
                return listenerId;
            }

            public void notify(String key, String payload, Type event, String source) {
            	
                try {
                	ServiceInfo info = query.getObjectFromFormat(ServiceInfo.class, payload);
                	boolean isLocationMatch = isMatch(listenerZone, info.zone, isStrictLocationMatch);
                	
                	if (!isLocationMatch) {
                		LOGGER.info("Ignoring Notify Service" + info + " ZONE:" + info.zone + " <> " + listenerZone);
                		return;
                	}
                	
                	if (VSOProperties.isVERBOSELookups()) LOGGER.info(String.format("%s/%d Notifying %s of %s [%s]", LookupSpace.NAME, LookupSpaceImpl.this.getEndPoint().getPort(), listenerId, info, event.toString()));
                    if (LOGGER.isDebugEnabled()) LOGGER.debug(String.format("\n\n\t\t%s/%s/%s Callingback (*)\n\t\tClient:%s\n\t\t%s\n\t\tEvent[%s]\n", 
                    			LookupSpace.NAME, LookupSpaceImpl.this.serviceInfo.getLocationURI(), source,
                    			info.toString(), listenerId, event));
                    
                    if (!isThisProxyWorking) {
                        LOGGER.info("ProxyIsNotWorking:" + listenerId + "Service:" + info + " template:" + template);
                        return;
                    }
                    if (event.equals(Type.WRITE) || event.equals(Type.UPDATE)) {
//                    	                        addressListener.updateEndpoint(info.locationURI, info.getReplicationAddress());
                    	if (addressListeners.containsKey(addressListenerId)) syncAddressListener(addressListener, info.name, listenerZone);
                        
                    } else if (event.equals(Type.TAKE)) {

                        String msg = String.format(" ServiceInfoExpired id[%s] name[%s]:", info.id, info.name, info.locationURI);
                        System.err.println(new DateTime() + " FATAL " + msg);
                        LOGGER.fatal(msg);
                        LOGGER.error(msg);
                        
                        dumpThreads();
                        
                        // block messages to this endpoint so they dont all try and remove the endpoint
                        synchronized (addressListener) {
	                        if (addressListeners.containsKey(addressListenerId)) {
	                        	try {
	                        		LOGGER.error("RemoveEndPoint - " + addressListenerId + ":" + info.getReplicationAddress());
	                        		addressListener.removeEndPoint(info.locationURI, info.getReplicationAddress());
	                        	} catch (Throwable t) {
	                        		LOGGER.error("Failed to removeEndPoint - throwing it away:", t);
	                        		addressListeners.remove(addressListenerId);
	                        	}
	                        }
                        }
                    }
                } catch (Exception ex) {
                    LOGGER.warn(String.format("Resource[%s] template[%s] ex[%s]", resourceId, template, ex.toString()), ex);
                    isThisProxyWorking = false;
                }
            }


        };
        addressListeners.put(addressListenerId, new AddressSyncher(template, addressListenerId, addressListener, listenerZone, isStrictLocationMatch));
        String generalTemplate =  "name equals " + template;
        
        // if a zone-id was provided then match it exactly
        if (listenerZone != null && listenerZone.trim().length() > 0) {
        	generalTemplate +=  " AND zone equals " + listenerZone;
        }
        
        ormClient.registerEventListener(ServiceInfo.class, generalTemplate, eventListener, new Type[]{Type.WRITE, Type.UPDATE, Type.TAKE}, -1);
    }


    long dumpedThreads;
	protected void dumpThreads() {
		try {
            if (dumpedThreads > System.currentTimeMillis() - 1000) return;
            dumpedThreads = System.currentTimeMillis();
			System.err.println(new DateTime() + "ServiceInfoExpired - Lookup Dumping threads");
			FileOutputStream fos = new FileOutputStream("lu-panic-threads.txt");
			String threadDump = ThreadUtil.threadDump("", "");
			fos.write(threadDump.getBytes());
			fos.close();
		} catch (Throwable t) {
			t.printStackTrace();
		}
		
	}

	protected void syncAddressListener(AddressUpdater addressListener, String name, String zone) {
		List<ServiceInfo> services = getSortedServiceAddresses(name, zone, false);
		String[] addresses = getLocations(services);
        // maybe we should keep pushing anyways
        String[] replicationLocations = getReplicationLocations(services);
		try {
			addressListener.syncEndPoints(addresses, replicationLocations);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @param interestLocation represents the endPoint to match to
	 * @param candidateLocation represents the service which became available
	 * @param isStrictLocationMatch 
	 * @return
	 */
	boolean isMatch(String interestLocation, String candidateLocation, boolean isStrictLocationMatch) {
		if (isStrictLocationMatch) {
            return interestLocation.equals(candidateLocation);
        }
		if (interestLocation == null || candidateLocation == null) return true;
		if (interestLocation.equals(candidateLocation)) return true;
		String[] interestSplit = interestLocation.split("\\.");
		String[] candidateSplit = candidateLocation.split("\\.");
		if (interestSplit.length < candidateSplit.length) return false;
		for (int i = 0; i < candidateSplit.length; i++) {
			if (!interestSplit[i].equals(candidateSplit[i])) return false;
		}
		
		return true;
	}


	public String[] getServiceAddresses(String serviceName, String zone, boolean strictMatch) {
        if (zone == null || zone.length() == 0 && serviceName.contains(".")) {
            zone = serviceName.substring(0, serviceName.lastIndexOf("."));
            serviceName = serviceName.substring(serviceName.lastIndexOf(".")+1);
        }

        List<ServiceInfo> locationOrdered = getSortedServiceAddresses(serviceName, zone, strictMatch);
        
        String[] results = new String[locationOrdered.size()];

        int pos = 0;
        for (ServiceInfo serviceInfo : locationOrdered) {
            String[] splitAddresses = com.liquidlabs.common.collection.Arrays.split(",", serviceInfo.locationURI);
            for (String address : splitAddresses) {
                results[pos++] = address;
            }
        }
// this happens alot - should use log config to filter it out		
		if (VSOProperties.isVERBOSELookups()) LOGGER.info("VERBOSE_LOOKUP <<<<<<<<<< Getting serviceAddress [" + serviceName + "] Zone:" + zone + " Result:" + Arrays.toString(results) + " SVCS:" + locationOrdered);
        return results;
    }

	private List<ServiceInfo> getSortedServiceAddresses(final String serviceName, final String zone, final boolean strictMatch) {

		List<ServiceInfo> serviceInfos = ormClient.findObjects(ServiceInfo.class, "name equals " + serviceName, false, Integer.MAX_VALUE);

        List<ServiceInfo> strictMatches = new ArrayList<ServiceInfo>();
        List<ServiceInfo> locationOrdered = new ArrayList<ServiceInfo>();
        List<ServiceInfo> failovers = new ArrayList<ServiceInfo>();
        // will provide a.b.c, a.b, a so we can match on most specific first
        List<String> zoneMatches = getOrderedZones(zone);
        int foundZoneLength = -1;

        for (String zonePreference : zoneMatches) {
        	for (ServiceInfo info : serviceInfos) {
                if (info.getName().equals(serviceName)) {
                    if (strictMatch && info.zone.equals(zone)) {
                        if (!strictMatches.contains(info)) strictMatches.add(info);
                    }
                    if (info.zone.equals(zonePreference)) {
                        if (foundZoneLength == -1) foundZoneLength = info.zone.length();
                        if (info.zone.length() < foundZoneLength) continue;
                        if (failovers.contains(info) || locationOrdered.contains(info)) continue;
                        if (info.getAgentType().equals("Failover")) failovers.add(info);
                        else locationOrdered.add(info);
                    }
                }
			}
		}
        
        if (VSOProperties.isVERBOSELookups()) LOGGER.info("getSortedServiceAddresses:" + serviceName + " HEAD:" + locationOrdered + " TAIL:" + failovers);
        
        locationOrdered.addAll(failovers);

        // try and use the strict matches when they exist
        if (strictMatch && strictMatches.size() > 0) return strictMatches;

		return locationOrdered;
	}

    private List<String> getOrderedZones(String location) {
    	if (location == null) return new ArrayList<String>();
    	String[] locationParts = location.split("\\.");
    	String currentLocation = "";
    	List<String> results = new ArrayList<String>();
    	for (String part : locationParts) {
			if (currentLocation.contains(".")) {
				currentLocation += part;
			}
			else {
				currentLocation = part;
			}
			results.add(currentLocation);
			currentLocation += ".";
		}
    	Collections.reverse(results);
		return results;
	}

	public List<ServiceInfo> findService(String query) {
        return ormClient.findObjects(ServiceInfo.class, query, false, Integer.MAX_VALUE);
    }

    private void setServiceInfo(ServiceInfo serviceInfo) {
        this.serviceInfo = serviceInfo;
    }

    public static LookupSpace getRemoteService(String lookupSpaceURI, ProxyFactory proxyFactory, String context) {
        return getRemoteService(lookupSpaceURI, proxyFactory, false, context);
    }

    public static LookupSpace getRemoteService(String lookupSpaceURI, ProxyFactory proxyFactory, boolean fastBreak, String context) {
    	context += PIDGetter.getPID();
        AddressEventListener listener = new AddressEventListener(LookupSpace.NAME, context);
        LookupSpace lookupSpace = proxyFactory.getRemoteService(LookupSpace.NAME, LookupSpace.class, lookupSpaceURI.split(","), listener);

        int count = 0;
        boolean breakout = false;
        while (!breakout) {
            try {
                lookupSpace.registerUpdateListener(listener.getId(), listener, LookupSpace.NAME, LookupSpace.NAME + "-" + proxyFactory.getAddress().getHost() + "-" + proxyFactory.getAddress().getPort(), "LUSpaceImplGetLUSpace", VSOProperties.getZone(), false);
                breakout = true;
            } catch (Throwable ex) {
                if (fastBreak) {
                    LOGGER.warn(String.format("GetRemoteService[LookupSpace] uri[%s] Failed  - still returning proxy object", lookupSpaceURI));
                    return lookupSpace;
                }
                try {
                    LOGGER.warn(String.format("GetRemoteService -Failed to contact LUSpace[%s], waiting, count:%d ex:%s", lookupSpaceURI, count++, ex.toString()));
                    if (count < 10) LOGGER.error("ConnectExeption", ex);
                    Thread.sleep(30 * 1000);

                } catch (InterruptedException e) {
                }
            }
        }

        // Try to get the fail-over version now
        String[] addresses = lookupSpace.getServiceAddresses(LookupSpace.NAME, VSOProperties.getZone(), false);
        if (addresses.length == 0) {
            LOGGER.warn(String.format("Didnt get Address for LUSpace[%s]", lookupSpaceURI));
            return lookupSpace;
        }

        LookupSpace remoteService = proxyFactory.getRemoteService(LookupSpace.NAME, LookupSpace.class, addresses, listener);
        LOGGER.info("GetRemoteService:" + remoteService );
        return remoteService;
    }

    public static LookupSpace getLookRemoteSimple(String lookupSpaceURI, ProxyFactory proxyFactory, String context) {
        AddressEventListener listener = new AddressEventListener(LookupSpace.NAME, context);
        return proxyFactory.getRemoteService(LookupSpace.NAME, LookupSpace.class, lookupSpaceURI.split(","), listener);
    }

    public static void main(String[] args) {
        try {
        	System.out.println("Starting LookupSpace");
        	
            JmxHtmlServerImpl jmxServer = new JmxHtmlServerImpl(VSOProperties.getJMXPort(ports.LOOKUP) , true);
            jmxServer.start();

            LookupSpaceImpl.boot(VSOProperties.getLookupPort(), VSOProperties.getReplicationPort(), null);

        } catch (Throwable t) {
            LOGGER.error(t.getMessage(), t);
            LOGGER.fatal(t.getMessage(), t);
        }
    }


    public static void boot(int port, int replicationPort, EmbeddedServiceManager esm) throws URISyntaxException {

        auditLogger.emit("Boot","");

        LOGGER.info(String.format("Starting LookupSpace port:%d rep:%d base:%d", port, replicationPort, VSOProperties.getBasePort()));
        final LookupSpaceImpl lookupSpaceImpl = new LookupSpaceImpl(port, replicationPort);
        if (esm != null) esm.registerLifeCycleObject(lookupSpaceImpl);
        lookupSpaceImpl.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    LOGGER.info("Running shutdown hook to unregister services");
                    lookupSpaceImpl.stop();
                    Thread.sleep(2000);
                } catch (Throwable t) {
                    LOGGER.info(t);
                }
            }
        });
        LOGGER.info("Started");
    }

    public TransportFactory getTransportFactory() {
        return transportFactory;
    }

    public void cancelLease(String leaseKey) {
    }

    private ResourceProfile resourceProfile;

    public void addLookupPeer(URI replicationURI) {
    	LOGGER.info("Adding LookupPeer: " + replicationURI);
        this.ormClient.addPeer(replicationURI);
    }

    public void removePeer(URI uri) {
        LOGGER.info("Lookup - Removing Peer: " + uri);
        this.ormClient.removePeer(uri);
    }

    void syncAddressListeners() {
		if (state == LifeCycle.State.STARTED) {
				List<String> addressListenerKeys = new ArrayList<String>(addressListeners.keySet());
				Collections.sort(addressListenerKeys);
//				LOGGER.info(VSOProperties.HA_TAG + " GOING TO SYNC EPS:" + addressListenerKeys.size());
				for (String addressListenerId : addressListenerKeys) {
					try {
//						LOGGER.info(VSOProperties.HA_TAG + " SYNC:" + addressListenerId);
						AddressSyncher syncher = addressListeners.get(addressListenerId);
						if (syncher == null) {
							LOGGER.warn("Failed to find Listener:" + addressListenerId);
							continue;
						}
						boolean endPointOk = syncher.syncEndPoints();
				        if (!endPointOk) {
				            addressListeners.remove(addressListenerId);
				            LOGGER.warn("EP non-responsive, Removing AddressSyncher:" + addressListenerId);
				            ormClient.unregisterEventListener(addressListenerId);
				        }
					} catch (Throwable t) {
						LOGGER.warn("SyncFailed:" + t.toString(), t);
					}
					
				}
		}
	}
    class AddressSyncher {
        String serviceName;
        AddressUpdater listener;
        int syncFailures = 0;
        String lastSync = "";
        long lastUpdated = 0;
		private String addressUpdaterId;
		private final String zone;
        private boolean strict;

        AddressSyncher(String serviceName, String addressUpdaterId, AddressUpdater addressUpdater, String zone, boolean strict) {
            this.serviceName = serviceName;
			this.addressUpdaterId = addressUpdaterId;
            this.listener = addressUpdater;
			this.zone = zone;
            this.strict = strict;
        }
        @Override
        public String toString() {
        	return super.getClass().getSimpleName() + " SVC:" + serviceName + " UpdaterId:" + addressUpdaterId;
        }

        public boolean syncEndPoints() {
        	
            List<ServiceInfo> services = getSortedServiceAddresses(serviceName, zone, strict);

            try {
            	if (services.size()  == 0) return true;
            	if (isRecentlyUpdated(services, this.lastUpdated)) {
            		this.lastUpdated = System.currentTimeMillis();
	            	String[] addresses = getLocations(services);
	                lastUpdated = System.currentTimeMillis();
	                lastSync = services.toString();
	                // maybe we should keep pushing anyways
	                String[] replicationLocations = getReplicationLocations(services);
	                if (VSOProperties.isVERBOSELookups()) {
	                	LOGGER.info(serviceInfo +"\n\t_HA_ SYNC:" + this.toString() + " ADDR:" + services.toString() + " OLD:" + lastSync  + " REP:" + Arrays.toString(replicationLocations) );
	                }
					listener.syncEndPoints(addresses, replicationLocations);
	                syncFailures = 0;
            	}
            } catch (Throwable t) {
                syncFailures++;
                LOGGER.warn("AddressSyncher:" + serviceName + " incrementing syncCount:" + syncFailures + " :" + listener);
            }
            return syncFailures != VSOProperties.getAddressSyncherRetryCount();
        }

        private boolean isRecentlyUpdated(List<ServiceInfo> services, long lastFired) {
			for (ServiceInfo serviceInfo : services) {
				if (serviceInfo.insertTimeMs > lastFired) return true;
			}
			if (lastFired < new DateTime().minusMinutes(1).getMillis()) return true;
			if (!this.lastSync.equals(services.toString())) return true;
			
			return false;
		}
    }
    private String[] getLocations(List<ServiceInfo> serviceInfos) {
    	String[] locations = new String[serviceInfos.size()];
    	int pos = 0;
    	for (ServiceInfo info : serviceInfos) {
    		locations[pos++] = info.getLocationURI();
    	}
    	return locations;
    }
    
    private String[] getReplicationLocations(List<ServiceInfo> serviceInfos) {
    	List<String> replicatonLocations = new ArrayList<String>();
    	for (ServiceInfo info : serviceInfos) {
    		String replicationUri = info.getReplicationAddress();
    		if (replicationUri != null){
    			replicatonLocations.add(replicationUri);
    		}
    	}
    	return replicatonLocations.toArray(new String[0]);
    }
}
