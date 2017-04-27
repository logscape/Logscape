package com.liquidlabs.vso.deployment.bundle;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.Logging;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.vso.SpaceService;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.agent.ResourceAgent;
import com.liquidlabs.vso.agent.ResourceProfile;
import com.liquidlabs.vso.deployment.SystemBouncer;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.LookupSpaceImpl;
import com.liquidlabs.vso.resource.AllocListener;
import com.liquidlabs.vso.resource.ResourceRegisterListener;
import com.liquidlabs.vso.resource.ResourceSpace;
import com.liquidlabs.vso.resource.ResourceSpaceImpl;
import com.liquidlabs.vso.work.WorkAllocator;
import com.liquidlabs.vso.work.WorkAllocatorImpl;
import com.liquidlabs.vso.work.WorkAssignment;
import org.apache.log4j.Logger;

import java.io.File;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.*;

public class BundleSpaceImpl implements BundleSpace, ServiceFinder {
	

	private final static Logger LOGGER = Logger.getLogger(BundleSpace.class);
    static Logging auditLogger = Logging.getAuditLogger("VAuditLogger", "BundleSpace");

	LifeCycle.State state = LifeCycle.State.STOPPED;

	private ResourceSpace resourceSpace;
//	Map<String, WorkAssignment> workToAllocate = new ConcurrentHashMap<String, WorkAssignment>();
	Map<String, String> variables = new ConcurrentHashMap<String, String>();
	int requestId = 0;

	private BundleServiceAllocator serviceAllocListener;

	private BundleRRegListenerImpl resourceRegisterListener;

	private WorkAllocator workAllocator;

	private BackgroundServiceAllocator backgroundServiceAllocator;

	private ScheduledExecutorService scheduler;

	private SpaceService spaceService;

	private ProxyFactory proxyFactory;

	private ResourceAgent agent;

	private SpaceService workToAllocate;

	public BundleSpaceImpl(){
	}
	public BundleSpaceImpl(ResourceSpace resourceSpace, WorkAllocator workAllocator, SpaceService spaceService, ScheduledExecutorService scheduler, ProxyFactory proxyFactory, ResourceAgent agent, SpaceService pendingWork) {
		this.resourceSpace = resourceSpace;
		this.workAllocator = workAllocator;
		this.spaceService = spaceService;

		this.scheduler =  scheduler;
		this.proxyFactory = proxyFactory;
		this.agent = agent;
		this.workToAllocate = pendingWork;
			
		resourceRegisterListener = new BundleRRegListenerImpl(resourceSpace, workToAllocate, variables, spaceService.getClientAddress());
		registerResourceRegListener(resourceRegisterListener, resourceRegisterListener.getId(), "");

		serviceAllocListener = new BundleServiceAllocator(workToAllocate, variables, resourceRegisterListener, workAllocator, spaceService.getClientAddress(), resourceSpace, scheduler);
		registerResourceAllocListener(serviceAllocListener, serviceAllocListener.getId());
		
		resourceRegisterListener.setServiceListenerId(serviceAllocListener.getId());
		
		backgroundServiceAllocator = new BackgroundServiceAllocator(workToAllocate, variables, workAllocator, spaceService.getClientAddress(), this, scheduler);
		registerResourceRegListener(backgroundServiceAllocator, backgroundServiceAllocator.getId(), "id == 0");
	}
	
	public void bounceSystem() {
		SystemBouncer bouncer = new SystemBouncer(1000, resourceSpace, proxyFactory);
		bouncer.bounce(agent);
	}

	private void registerResourceRegListener(final ResourceRegisterListener listener, final String id, final String resourceSelection) {
		
		scheduler.scheduleAtFixedRate(new Runnable(){
			public void run() {
				try {
					resourceSpace.registerResourceRegisterListener(listener, id, resourceSelection, VSOProperties.getResourceListenerRegInterval());
				} catch (Exception e) {
					LOGGER.error("Failed to registerListener on:" + resourceSpace.toString(), e);
				}
			}
		}, 1, VSOProperties.getResourceListenerRegInterval() - 60, TimeUnit.SECONDS);
		
		Runtime.getRuntime().addShutdownHook(new Thread(){
			@Override
			public void run() {
				try {
					resourceSpace.unregisterResourceRegisterListener(id);
					Thread.sleep(20);
				} catch (Exception e) {
				}
			}
		});
	}
	private void registerResourceAllocListener(final AllocListener allocListener, final String id) {
		
		scheduler.scheduleAtFixedRate(new Runnable(){
			public void run() {
				resourceSpace.registerAllocListener(allocListener, id, id);
			}
		}, 1, VSOProperties.getResourceListenerRegInterval(), TimeUnit.SECONDS);
		Runtime.getRuntime().addShutdownHook(new Thread(){
			@Override
			public void run() {
                try {
				    resourceSpace.unregisterAllocListener(id);
                } catch(Exception e) {}
				try {
					Thread.sleep(20);
				} catch (InterruptedException e) {
				}
			}
		});
	}
	
	public void start() {
		if (this.state == LifeCycle.State.STARTED) return;
		this.state = LifeCycle.State.STARTED;
		LOGGER.info("Starting");
		
		final URI clientAddress = spaceService.getClientAddress();
		LOGGER.info("Registering BundleSpaceAddress:" + clientAddress);
	
		workToAllocate.start(this, "boot-1.0");
		spaceService.start(this, "boot-1.0");
	
		final BundleSpace myself = this;
		Runtime.getRuntime().addShutdownHook(new Thread(){
			public void run() {
				LOGGER.warn("Stopping:" + myself.toString());
				myself.stop();
			}
		});

	}
	public void stop() {
		if (this.state == LifeCycle.State.STOPPED) return;
		this.state = LifeCycle.State.STOPPED;
		
		try {
			this.resourceSpace.unregisterAllocListener(serviceAllocListener.getId());
			this.resourceSpace.unregisterResourceRegisterListener(resourceRegisterListener.getId());
			this.resourceSpace.unregisterResourceRegisterListener(backgroundServiceAllocator.getId());
		} catch (Throwable t) {
		}
		spaceService.stop();
		workToAllocate.stop();
	}

	public URI getEndPoint() {
		return spaceService.getClientAddress();
	}
	
	public List<Bundle> getBundles() {
		return spaceService.findObjects(Bundle.class, "", false, Integer.MAX_VALUE);
	}
	public List<String> getBundleNames(String query) {
		List<Bundle> findObjects = spaceService.findObjects(Bundle.class, query, false, Integer.MAX_VALUE);
		ArrayList<String> results = new ArrayList<String>();
		for (Bundle bundle : findObjects) {
			results.add(bundle.getId());
		}
		return results;
	}
	
	public Bundle getBundle(String fullBundleName) {
		return spaceService.findById(Bundle.class, fullBundleName);
	}
	public String getBundleXML(String bundleId) {
		Bundle bundle = getBundle(bundleId);
        if (bundle == null) return "";
		return new BundleSerializer(new File(VSOProperties.getDownloadsDir())).getXML(bundle);
	}
	public String getBundleStatus(String bundleId) {
		Bundle bundle = getBundle(bundleId);
		if (bundle == null) return Bundle.Status.UNINSTALLED.name();
		return bundle.getStatus().name();
	}

	public List<Service> getBundleServices(String bundleId) {
		return spaceService.findObjects(Service.class, "bundleId equals " + bundleId, false, Integer.MAX_VALUE);
	}
	public String[] getBundleServiceNames(String bundleId) {
		return spaceService.findIds(Service.class, "bundleId equals " + bundleId);
	}
	public List<Service> getBundleServicesForQuery(String query) {
		return spaceService.findObjects(Service.class, query, false, Integer.MAX_VALUE);
	}
	public Service getBundleService(String serviceId) {
		Service retrieve = spaceService.findById(Service.class, serviceId);
		if (retrieve == null) {
			String[] serviceIds = spaceService.findIds(Service.class, serviceId);
			String warnMsg = String.format("Failed to find Service[%s] - check the name is valid against:%s", serviceId, Arrays.toString(serviceIds));
			LOGGER.info(warnMsg);
			throw new RuntimeException(warnMsg);
		}
//		LOGGER.info("ReturningService:" + retrieve + " for:" + serviceId);
		return retrieve;
	}

	public void registerBundle(Bundle bundle) throws Exception {
		LOGGER.info("Registering bundle " + bundle.getId());
		if (getBundle(bundle.getId()) != null) {
			LOGGER.info(String.format("Bundle[%s] is already deployed", bundle.getId()));
			return;
		}
		spaceService.store(bundle, -1);
	}
	public void updateBundle(Bundle bundle) {
		spaceService.store(bundle, -1);
	}

	public void registerBundleService(Service service) {
        auditLogger.emit("RegisterService", service.getId() + " Selection:" + service.getResourceSelection());
		spaceService.store(service, -1);
	}

	public void registerBundleServices(List<Service> services) {
		spaceService.store(services, -1);
	}

	public boolean requestServiceStart(String requestId, final WorkAssignment workInfo) {
		
		workInfo.setResourceId(requestId);

		LOGGER.info(String.format("RequestId[%s] requestServiceStart %s resourceType[%s]", requestId, workInfo, workInfo.getResourceSelection()));
		
		// will be picked up by BG and FG Allocator
		
		if (workInfo.isBackground()) {

            Runnable task = new Runnable() {
                @Override
                public void run() {
                    try {
                        auditLogger.emit("CommencingBGServiceAllocations", workInfo.toString());
                        workToAllocate.store(workInfo, -1);
                        // get all '0' instances per agent
                        List<String> bgResources = resourceSpace.findResourceIdsBy(workInfo.getResourceSelection() + " AND id == 0");
                        for (String resourceId : bgResources) {
                            ResourceProfile resourceDetails = resourceSpace.getResourceDetails(resourceId);
                            LOGGER.info(String.format(" Assigning BGTask %s to %s", workInfo.getId(), resourceDetails.getResourceId()));
                            backgroundServiceAllocator.register(resourceDetails.getResourceId(), resourceDetails);
                        }
                        if (bgResources.size() == 0) {
                            LOGGER.info(String.format(" Pending BGTask %s Selection[%s] ", workInfo.getId(), workInfo.getResourceSelection()));
                        }
                    } catch (Throwable t) {
                        LOGGER.error("Problem Running Service:" + workInfo,t );
                    }

                }
            };
            int delay = workInfo.getPauseSeconds();

            if (delay <= 0) delay = 1;
            // anything wih a large delay is probably a scheduled task
            if (delay >= 120) delay = 10;
            auditLogger.emit("PendingBGServiceAllocations", "Delay:" + delay + " " + workInfo.toString());
            ScheduledFuture<?> bgTasksSchedule = scheduler.schedule(task, delay, TimeUnit.SECONDS);
            addToPendingBundleFutures(workInfo.getBundleId(), bgTasksSchedule);

            return true;
		} else {
            workToAllocate.store(workInfo, -1);
        }
		
		int allocationsOutstanding = workInfo.getAllocationsOutstanding();
		LOGGER.info(requestId + " RequestingResources for Service:"  + workInfo.getServiceName() + " count:" + allocationsOutstanding);
		int assignmentCount = 0;
		try {
			assignmentCount = resourceSpace.requestResources(requestId, allocationsOutstanding, workInfo.getPriority(), workInfo.getResourceSelection(), workInfo.getId(), VSOProperties.getLUSpaceServiceLeaseInterval(), serviceAllocListener.getId(), "");
			workInfo.setAllocationsOutstanding(allocationsOutstanding - assignmentCount);
			LOGGER.info(requestId + " Given ResourceCount:" + assignmentCount);
		} catch (Exception e) {
			LOGGER.info(requestId + " Failed to requestResources", e);
		}
		workToAllocate.store(workInfo, -1);
		return assignmentCount == allocationsOutstanding;
	}

    Map<String,List<ScheduledFuture>> pendingBundleWork = new ConcurrentHashMap<String, List<ScheduledFuture>>();
    private void addToPendingBundleFutures(String bundleId, ScheduledFuture<?> bgTasksSchedule) {
        if (!pendingBundleWork.containsKey(bundleId)) pendingBundleWork.put(bundleId, new ArrayList<ScheduledFuture>());
        pendingBundleWork.get(bundleId).add(bgTasksSchedule);

    }

    public boolean isWorkAssignmentPending(WorkAssignment workAssignmenet){
		return serviceAllocListener.isWorkAssignmentPending(workAssignmenet);
	}
	
	/* (non-Javadoc)
	 * @see com.liquidlabs.vso.deployment.bundle.ServiceFinder#isServiceRunning(java.lang.String)
	 */
	public boolean isServiceRunning(String serviceId) {
		try {
			List<ResourceProfile> profiles = resourceSpace.findResourceProfilesBy("workId contains " + serviceId);
			boolean b = profiles.size() > 0;
			return b;
		} catch (Throwable t) {
			if (t.getMessage()!= null && t.getMessage().startsWith("Failed to find Service")) return false;
			throw new RuntimeException(t);
		}
	}
	
	/* (non-Javadoc)
	 * @see com.liquidlabs.vso.deployment.bundle.ServiceFinder#findService(java.lang.String, java.lang.String)
	 */
	public Service findService(String serviceName, String bundleId) {
		List<Service> services = spaceService.findObjects(Service.class, String.format("name equals %s AND bundleId equals %s", serviceName, bundleId), false, 1);
		if (services.isEmpty()) {
			return null;
		}
		return services.get(0);
	}
	public void stopWork(String bundleId) {
		LOGGER.info("StopWork Bundle:" + bundleId);
		workAllocator.unassignWorkFromBundle(bundleId);
		removefromWorkPEND_SPACE(bundleId);
	}

	public void remove(String bundleId) {
		LOGGER.info("Uninstall Bundle:" + bundleId);
		workAllocator.unassignWorkFromBundle(bundleId);
		spaceService.purge(Service.class,"bundleId equals " + bundleId);
		spaceService.purge(Bundle.class, "id equals " + bundleId);

        List<ScheduledFuture> futures = pendingBundleWork.remove(bundleId);
        if (futures != null) {
            for (ScheduledFuture future : futures) {
                try {
                    future.cancel(true);
                } catch (Throwable t){
                    t.printStackTrace();
                }
            }
        }

        removefromWorkPEND_SPACE(bundleId);
		LOGGER.info("Deployed Bundles List:" + Arrays.toString(spaceService.findIds(Bundle.class, "")));
	}
	private void removefromWorkPEND_SPACE(String bundleId) {
		workToAllocate.purge(WorkAssignment.class, "bundleId equals " + bundleId);
	}
	
	public static void main(String[] args) {
		try {
			BundleSpaceImpl.boot(args[0], null);
		} catch (Throwable t){
			LOGGER.error(t.toString(), t);
		}
	}
	public static BundleSpace boot(String lookupAddress, ResourceAgent agent) throws URISyntaxException{
		try {
			ORMapperFactory mapperFactory = LookupSpaceImpl.getSharedMapperFactory();
			
			LookupSpace lookupSpace = LookupSpaceImpl.getRemoteService(lookupAddress, mapperFactory.getProxyFactory(),"BundleSpaceBootLU");
			ResourceSpace resourceSpace = ResourceSpaceImpl.getRemoteService("BundleSpaceBootRS", lookupSpace, mapperFactory.getProxyFactory());
			while(!resourceSpace.isStarted()) {
				LOGGER.info("Waiting for:" + resourceSpace);
				Thread.sleep(1000);
			}
			WorkAllocator workAllocator = WorkAllocatorImpl.getRemoteService("BundleSpaceBootWA", lookupSpace, mapperFactory.getProxyFactory());
			
			SpaceServiceImpl spaceService = new SpaceServiceImpl(lookupSpace, mapperFactory, BundleSpace.NAME, mapperFactory.getScheduler(), true, false, false);
			SpaceServiceImpl pendingWork = new SpaceServiceImpl(lookupSpace, mapperFactory, BundleSpace.NAME+"_PEND", mapperFactory.getScheduler(), true, false, true);
			
			BundleSpace bundleSpace = new BundleSpaceImpl(resourceSpace, workAllocator, spaceService,  mapperFactory.getScheduler(), mapperFactory.getProxyFactory(), agent, pendingWork);
			mapperFactory.getProxyFactory().registerMethodReceiver(BundleSpace.NAME, bundleSpace);
			bundleSpace.start();
			return bundleSpace;
		} catch (Throwable t){
			LOGGER.error("Failed to boot BundleSpace:" + t.toString(), t);
			throw new RuntimeException("Failed to boot BundleSpace", t);
		}
	}
	public boolean isWaitingForDependencies(String serviceName, String bundleId) {
		Service service = findService(serviceName, bundleId);
		if (service == null) {
			LOGGER.warn("Failed to find Service:" + bundleId + " " + serviceName);
			return false;
		}
		String[] dependencies = service.getDependencies();
		List<String> deps = new ArrayList<String>(Arrays.asList(dependencies));
		for (String dependency : deps) {
			if (!isServiceRunning(dependency)) {
				return true;
			}
		}
		return false;
	}
	public static BundleSpace getRemoteService(String whoAmI, LookupSpace lookupSpace, ProxyFactory proxyFactory) {
		return SpaceServiceImpl.getRemoteService(whoAmI, BundleSpace.class, lookupSpace, proxyFactory, BundleSpace.NAME, true, false);
	}
	

}
