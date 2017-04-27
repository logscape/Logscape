
package com.liquidlabs.vso.container;

import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.joda.time.DateTimeUtils;

import com.liquidlabs.common.ExceptionUtil;
import com.liquidlabs.common.FileHelper;
import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.collection.PropertyMap;
import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.common.concurrent.NamingThreadFactory;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.jmx.JmxHtmlServerImpl;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.space.lease.LeaseRenewalService;
import com.liquidlabs.space.lease.Registrator;
import com.liquidlabs.space.lease.Renewer;
import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.TransportFactoryImpl;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.transport.proxy.ProxyFactoryImpl;
import com.liquidlabs.vso.ServiceStatus;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.agent.Resource;
import com.liquidlabs.vso.agent.ResourceAgent;
import com.liquidlabs.vso.agent.ResourceAgentImpl;
import com.liquidlabs.vso.agent.ResourceProfile;
import com.liquidlabs.vso.container.sla.Rule;
import com.liquidlabs.vso.container.sla.SLA;
import com.liquidlabs.vso.container.sla.SLASerializer;
import com.liquidlabs.vso.container.sla.SLAValidator;
import com.liquidlabs.vso.container.sla.Variable;
import com.liquidlabs.vso.deployment.bundle.BundleHandler;
import com.liquidlabs.vso.deployment.bundle.BundleHandlerImpl;
import com.liquidlabs.vso.deployment.bundle.BundleSpace;
import com.liquidlabs.vso.deployment.bundle.BundleSpaceImpl;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.LookupSpaceImpl;
import com.liquidlabs.vso.lookup.ServiceInfo;
import com.liquidlabs.vso.monitor.Metrics;
import com.liquidlabs.vso.monitor.MonitorSpace;
import com.liquidlabs.vso.resource.AllocListener;
import com.liquidlabs.vso.resource.ResourceSpace;
import com.liquidlabs.vso.resource.ResourceSpaceImpl;
import com.liquidlabs.vso.work.InvokableImpl;
import com.liquidlabs.vso.work.WorkAllocator;
import com.liquidlabs.vso.work.WorkAllocatorImpl;
import com.liquidlabs.vso.work.WorkAssignment;


/**
 * Load the SLA and request to add/remove/resources accordingly
 *
 */
public class SLAContainer implements Runnable, AllocListener, AddListener {
	
	private static final String RFS_ROOT = "rfs/";
	private static final String rfsPath = System.getProperty("rfs.root", RFS_ROOT);
	
	private static Logger LOGGER = Logger.getLogger(SLAContainer.class);
	public static String TAG = "SLACONTAINER";
	
	private ResourceSpace resourceSpace;
	List<URI> serverAddresses = new ArrayList<URI>();
	
	private SLA  sla;
	int resourceRequestCount;
	Map<String, String> allocatedResources = new ConcurrentHashMap<String, String>();
	Set<String> lastUnknownResourceIds = new HashSet<String>();

	private final URI uri;
	transient private final Consumer consumer;
	transient private final MonitorSpace monitorSpace;
	transient private final WorkAllocator workAllocator;
	transient private final ProxyFactory proxyFactory;
	private final SLAValidator slaValidator;
	
	private final String workingDirectory;
	private final String serviceToRun;
	private String slaVariables;
	private final String fullBundleName;
	private long requestId;
	private final String consumerName;
	private int consumerAllocDelta = 5;
	
	/**
	 * Keep executing when WARNING, stop executing on ERROR
	 */
	private ServiceStatus status  = new ServiceStatus();
	transient private ResourceAgent resourceAgent;
	private final String slaContainerWorkId;
	
	public long counter = 0;
	public String owner;
	
	WorkAssignment workAssignment;
	private final String slaFilename;
	private String serviceCriteria = "mflops > 0";
	
	public SLAContainer(String slaContainerWorkId, String serviceToRun, WorkAssignment workAssignment, String consumerName, Consumer consumer, SLA sla, String slaFilename, final ResourceSpace resourceSpace, WorkAllocator workAllocator, 
								MonitorSpace monitorSpace, URI uri, String workingDirectory, String fullBundleName, String notifyFilter, ProxyFactory proxyFactory, ResourceAgent resourceAgent, SLAValidator slaValidator) {
		this.serviceToRun = serviceToRun;
		this.workAssignment = workAssignment;
		this.slaFilename = slaFilename;
		this.fullBundleName = fullBundleName;
		this.slaContainerWorkId = slaContainerWorkId;
		this.workingDirectory = workingDirectory;

		this.consumerName = consumerName;
		this.resourceSpace = resourceSpace;
		this.monitorSpace = monitorSpace;
		this.workAllocator = workAllocator;
		this.consumer = consumer;
		
		this.uri = uri;
		this.resourceAgent = resourceAgent;
		this.proxyFactory = proxyFactory;
		this.slaValidator = slaValidator;
		
		this.status = new ServiceStatus(System.currentTimeMillis());
		
		owner = getOwnerId();
		registerResourceAllocListener(this, getOwnerId());
		consumer.setInfo(getOwnerId(), serviceToRun, fullBundleName);
		
		setSLA(sla, fullBundleName);
		makeConsumerInvokable(workAllocator, consumer, proxyFactory, slaContainerWorkId);
	}
	public String getId() {
		return owner;
	}


	private void makeConsumerInvokable(WorkAllocator workAllocator, Consumer consumer, ProxyFactory proxyFactory, String slaContainerWorkId) {
		boolean isConsumerInvokable = consumer.getUI() != null;
		log("Consumer isInvokable:" + isConsumerInvokable);
		if (isConsumerInvokable) {
			try {
				InvokableImpl invokable = new InvokableImpl(consumer.getUI());
				proxyFactory.registerMethodReceiver("INVOKABLE", invokable);
				workAllocator.update(slaContainerWorkId, "invokableEP replace '" + proxyFactory.getEndPoint() + "'");
			} catch (Exception e) {
				LOGGER.error(e.toString(), e);
			}
		}
	}
	
	
	private void registerResourceAllocListener(final AllocListener allocListener, final String id) {
		
		log("Registering ResourceSpace AllocListener:" + allocListener.toString() + " id:" + id);
		proxyFactory.getScheduler().scheduleAtFixedRate(new Runnable(){
			public void run() {
				try {
					resourceSpace.registerAllocListener(allocListener, id, id);
				} catch (Throwable t) {
					LOGGER.warn("resourceSpace.registerAllocListener(allocListener, id, id) Failed:" + resourceSpace, t);
				}
			}
		}, 1, VSOProperties.getResourceListenerRegInterval(), TimeUnit.SECONDS);
		
		proxyFactory.getScheduler().scheduleAtFixedRate(new Runnable(){
			public void run() {
				try {
					resourceSpace.renewAllocLeasesForOwner(id, 3 * 60);
				} catch (Throwable t) {
					LOGGER.warn("resourceSpace.renewAllocLeasesForOwner(getOwnerId(), 180); Failed:" + resourceSpace, t);
				}

			}
		}, 1, 2 * 60, TimeUnit.SECONDS);
		Runtime.getRuntime().addShutdownHook(new Thread(){
			@Override
			public void run() {
				resourceSpace.unregisterAllocListener(id);
				pause();
			}
		});
	}
	
	/**
	 * 
	 * Called when an SLA is updated/saved
	 * @param newSLA
	 * @param bundleName
	 */
	public String setSLA(SLA newSLA, String bundleName) {
		// consumer needs SLA variables before it can generate metrics
		copySLAVariablesIntoConsumer(consumer, newSLA, bundleName);
		String slaString = new SLASerializer().serialize(newSLA);
		String isValidSLA = validateSLA(slaString);
		if (!Boolean.getBoolean("validation.disabled")) {
			if (isValidSLA.toUpperCase().contains("ERROR")) {
				LOGGER.warn("********** SLA is not valid:" + isValidSLA);
				
				resourceAgent.updateStatus(slaContainerWorkId, LifeCycle.State.ERROR, isValidSLA);
				status.setError(String.format("SLA is not valid:" + isValidSLA));
				return isValidSLA;
			}
		}
		writeSLAFileToRFS(slaFilename, slaString);
		this.sla = newSLA;
		this.sla.setScriptLogger(Logger.getLogger(consumer.getClass()));
		return "SLA is now active";
	}
	private void writeSLAFileToRFS(String slaFilename, String slaString) {
		try {
			// need to move to the Agents working dir and not the service working dir
			
			FileHelper.mkAndList(rfsPath + "/" + fullBundleName);
			String path = rfsPath + "/" + fullBundleName;
			File slaFile = new File(path, "." + slaFilename);
			log(String.format("Writing SLA %s to %s", slaFilename, slaFile.getPath()));
			FileOutputStream fos = new FileOutputStream(slaFile);
			fos.write(slaString.getBytes());
			fos.close();
			
			// kind-of atomic write to RFS
			FileUtil.renameTo(slaFile,new File(path, slaFilename));
		} catch (Exception e) {
			LOGGER.warn(e.toString(), e);
		}
		
	}


	public String validateSLA(String newSLA) {
		log(String.format("%s %s >> Validating SLA", TAG, getOwnerId()));
		try {
			System.setProperty("is.validating", "true");
			Metric [] metrics = consumer.collectMetrics();
			GenericMetric totalAgentCount = new GenericMetric("totalAgents", getTotalAgentCount(resourceSpace.getResourceCount(serviceCriteria)));
			GenericMetric consumerCount = new GenericMetric("consumerUsed", consumer.getUsedResourceCount());
			GenericMetric allocCount = new GenericMetric("consumerAlloc", this.allocatedResources.size());
			GenericMetric cpu = new GenericMetric("cpu", resourceProfile == null ? 0 : resourceProfile.getCpuUtilisation());
			metrics = addMetrics(metrics, consumerCount, allocCount, totalAgentCount, cpu);

			return slaValidator.validate(newSLA, metrics, consumer);
		} catch (Throwable t){
			LOGGER.error(t.toString(), t);
			return "Error:" + t;
		} finally {
			log(String.format("%s %s << Validating SLA", TAG, getOwnerId()));
			System.setProperty("is.validating", "false");
		}
	}
	


	/**
	 * from the gui when wanting to reset an error state
	 */
	public void resetStatusToRunning() {
		this.status.set(LifeCycle.State.RUNNING, "");
		this.resourceAgent.updateStatus(slaContainerWorkId, LifeCycle.State.RUNNING, "");
	}

	private void copySLAVariablesIntoConsumer(Consumer consumer, SLA sla, String bundleName) {
		List<Variable> slaVariablesList = sla.getVariables();
		PropertyMap consumerVariables = new PropertyMap("");
		for (Variable variable : slaVariablesList) {
			consumerVariables.put(variable.name, variable.value);
		}
		consumerVariables.put("ownerId", getOwnerId());
		consumerVariables.put("bundleName", bundleName);
		consumerVariables.put("serviceToRun", serviceToRun);
		consumer.setVariables(consumerVariables);
		slaVariables = consumerVariables.toString();
	}
	public Consumer getConsumer() {
		return consumer;
	}
	
	
	public void run() {
		try {
			log("Run >> " + owner);
			
			try {
				/**
				 * When an ERROR state was entered stop executing
				 */
				if (status.isError()) {
	// dont spam the log file				
	//				LOGGER.info(String.format("%s %s Not executing due to ERROR State, msg:%s",TAG, getOwnerId(), status.msg()));
					return;
				}
				if (pendingAddCounter.get() < 0) pendingAddCounter.set(0);
				Metric [] metrics = consumer.collectMetrics();
				GenericMetric totalAgentCount = new GenericMetric("totalAgents", getTotalAgentCount(resourceSpace.getResourceCount(serviceCriteria)));
				GenericMetric consumerCount = new GenericMetric("consumerUsed", consumer.getUsedResourceCount());
				GenericMetric allocCount = new GenericMetric("consumerAlloc", this.allocatedResources.size());
				GenericMetric cpu = new GenericMetric("cpu", resourceProfile != null ? resourceProfile.getCpuUtilisation() : 0);
				metrics = addMetrics(metrics, consumerCount, allocCount, totalAgentCount, cpu);
				
				log(String.format("%s %s Collecting metrics, ALLOCATED:%d CONS_ALLOC:%d pendingAdd:%d Add:%d", TAG, getOwnerId(), this.allocatedResources.size(), consumerCount.value().intValue(), pendingAddCounter.get(), currentlyAdding.size()));
				
				if (consumerCount.value() > allocCount.value() + this.consumerAllocDelta) {
						// only enter warning status once
						if (!status.isWarning()) {
							LOGGER.warn(String.format("%s %s Entering WARN state [%s] AND suspending Rule Processing", TAG, getOwnerId(), status));
							status.setWarning("Consumer not using Allocs:" + this.allocatedResources.size() + " used:" + consumerCount.value());
							resourceAgent.updateStatus(slaContainerWorkId, status.status(), status.msg());
						}
						
						// stop processing
						return;
				} else if (status.isWarning()) {
					// recovered from warning state
					resourceAgent.updateStatus(slaContainerWorkId, status.status(), status.msg());				
				}
				
				log(String.format("%s %s METRICS %s, %s, %s, STATUS:%s", TAG, getOwnerId(), java.util.Arrays.toString(metrics), consumerCount, allocCount, status.status()));
				monitorSpace.write(new Metrics(getOwnerId(), fullBundleName, metrics, consumerName, sla.currentPriority(this.allocatedResources.size())));
				
				if (currentlyAdding.isEmpty() && pendingAddCounter.get() == 0 || status.isWarning()) {
					
					
					try {
						com.liquidlabs.vso.container.Action action = sla.evaluate(this.allocatedResources.size(), consumer, metrics);
						if (action != null && !action.toString().contains(NullAction.class.getSimpleName())) {
							log(String.format("%s %s ACTION:%s", TAG, getOwnerId(), action));
							action.perform(resourceSpace, getOwnerId(), consumer, this.serviceToRun, fullBundleName, this);
							
							// successful SLA execution, switch back to running
							if (status.isWarning()) {
								status.setRunning("");
								resourceAgent.updateStatus(slaContainerWorkId, status.status(), status.msg());							
							}
						}
					} catch (Throwable t) {
						LOGGER.warn("Failed to execute SLAAction, entering WARN state:" + t, t);
						
						Rule rule = sla.getRule(this.allocatedResources.size());
						String script = rule != null ? rule.getScript() : ""; 
						String stringFromStack = ExceptionUtil.stringFromStack(t, 1024);
						status.setWarning(String.format("Failed to execute SLAAction[%s]\n Script[%s]\n Ex[%s]", t.getMessage(), script, stringFromStack));
						resourceAgent.updateStatus(slaContainerWorkId, status.status(), status.msg());
						
					}
				} else {
					log(String.format(" --------- Not Evaluating Rules as currently Adding resources OR in warning state Warn==" + status.isWarning()));
				}
				
				getConsumersReleasedResources();
			} catch (Throwable t){
				LOGGER.warn(getConsumer() + " SLARunError:" + t.getMessage(), t);
			} finally {
				log("Run << " + owner);
			}
				
				
			
		} catch (Throwable t){
			LOGGER.error(String.format("%s %s SLAContainer entering ERROR state msg[%s]", TAG, getOwnerId(), t.getMessage()), t);
			String stringFromStack = ExceptionUtil.stringFromStack(t, 1024);
			try {
				status.setError(t.getMessage().replaceAll("'", "`") + " " + stringFromStack);
				resourceAgent.updateStatus(slaContainerWorkId, status.status(), status.msg());
			} catch (Throwable t2){}
		}
	}


	/**
	 * only update the total count every 60 seconds
	 * @param resourceCount
	 * @return
	 */
	long lastResourceCountTime = 0;
	int lastResourceCountValue = 0;
	private Double getTotalAgentCount(int resourceCount) {
		long now = DateTimeUtils.currentTimeMillis();
		if (lastResourceCountValue == 0 || now - lastResourceCountTime > 60 * 1000) {
			lastResourceCountValue = resourceSpace.getResourceCount(serviceCriteria);
			lastResourceCountTime = now;
		}
		return (double) lastResourceCountValue;
	}


	private Metric[] addMetrics(Metric[] metrics, GenericMetric... metrics2) {
		Metric[] results = new Metric[metrics.length + metrics2.length];
		System.arraycopy(metrics, 0, results, 0, metrics.length);
		System.arraycopy(metrics2, 0, results, metrics.length, metrics2.length);
		return results;
	}


	private void getConsumersReleasedResources() {
		List<String> releasedResourceIds = new ArrayList<String>(consumer.getReleasedResources());
		if (releasedResourceIds.size() > 0) 
			LOGGER.debug(String.format("%s CONSUMER finished with %s", TAG, releasedResourceIds));
		for (String resourceId : releasedResourceIds) {
			try {
				String requestId = "SLA_CONS_REL" + counter++;
				this.allocatedResources.remove(resourceId);
				log(String.format("%s %s %s Consumer ReleaseWorkAssignment [%s]", TAG, getOwnerId(), requestId, resourceId));
				workAllocator.unassignWorkFromResource(requestId, resourceId, workAssignment.getBundleId(), workAssignment.getServiceName());
			} catch (Throwable t){
				LOGGER.warn("Failed to Release:" + resourceId + " t:" + t.getMessage(), t);
			}
		}

		if (releasedResourceIds != null && releasedResourceIds.size() > 0) resourceSpace.releasedResources(releasedResourceIds);
	}
	
	/**
	 * Periodically asks the consumer for what resources it thinks it has and anything
	 * that is unknown is freed back to VSO. 
	 * TODO: also need to do the opposite - when the Consumer has items we dont know about we
	 * probably need to grab the allocation.
	 */
	public void syncResources() {
		try {
			
			// if there is activity underway then dont sync
			if (!currentlyAdding.isEmpty() || pendingAddCounter.get() > 0) {
				log(String.format("Ignoring Sync, currAdd[%s] pendingAdd[%d]", currentlyAdding.size(), pendingAddCounter.get()));
				return;
			}
				
			// used when consumer is the boss of allocations (i.e. we are copying their environment/bolt-on)
			//
			boolean slaveToConsumer = Boolean.getBoolean("vscape.slaveToConsumer");
			
			consumer.synchronizeResources(this.allocatedResources.keySet());
			
			log(String.format("%s %s >>> SYNCChecking to syncResources", TAG, getOwnerId()));
			
			Set<String> consumerResourceIds = consumer.collectResourceIdsForSync();
			
			HashSet<String> unknownResourceIds = new HashSet<String>(this.allocatedResources.keySet());
			unknownResourceIds.removeAll(consumerResourceIds);
			HashSet<String> stillUnknownSetOfResources = new HashSet<String>(this.lastUnknownResourceIds);
			
			stillUnknownSetOfResources.removeAll(this.currentlyAdding.keySet());
			
			stillUnknownSetOfResources.retainAll(unknownResourceIds);
			if (stillUnknownSetOfResources.size() > 0) {
				
				if (slaveToConsumer) {
					
					// Free unknown allocations
					log(String.format("%s %s SYNCFree ResourceIds:%s", TAG, getOwnerId(), stillUnknownSetOfResources));
					for (String deltaResourceId : stillUnknownSetOfResources) {
						try {
							resourceSpace.forceFreeResourceAllocation(owner, "SLASync" + getOwnerId()+requestId++, deltaResourceId);
						} catch (Throwable t){
							LOGGER.warn(t);
						}
					}
					// Grab allocations for those we are supposed to have already
				} else {
					log(String.format("%s %s SYNCGrab ResourceIds:%s", TAG, getOwnerId(), stillUnknownSetOfResources));
					String resourcesToGrab = stillUnknownSetOfResources.toString().replaceAll("\\[", "'").replaceAll("\\]","'");
					resourceSpace.requestResources(getOwnerId() +"-SyncGRAB" + counter++, stillUnknownSetOfResources.size(), 9, "resourceId containsAny " + resourcesToGrab, serviceToRun, 180, getOwnerId(), "");
				}
			}
			// Grab the current unknown set, and remove those items we freed above
			this.lastUnknownResourceIds = unknownResourceIds;
			this.lastUnknownResourceIds.removeAll(stillUnknownSetOfResources);
			if (lastUnknownResourceIds.size() > 0) LOGGER.debug("LastUnknownResources:" + lastUnknownResourceIds);
			
			log(String.format("%s %s <<< SYNCChecking to syncResources", TAG, getOwnerId()));
		} catch (Throwable t){
			LOGGER.warn(t.getMessage(), t);
		}
	}



	/**
	 * Streaming updates via cont query
	 * Called from the resourceContainer when an SLA request is being full-filled.
	 * @param allocation
	 */
	Map<String, Integer> currentlyAdding = new ConcurrentHashMap<String, Integer>();
	ResourceProfile resourceProfile;
	
	public AtomicInteger pendingAddCounter = new AtomicInteger();
	
	public void add(String requestId, List<String> resourceIds, String owner, int priority) {
		if (this.allocatedResources.keySet().contains(resourceIds.get(0))) {
			log(" xxxxxxxxxxxxxxx DUPLICATE:" + resourceIds.get(0));			
		}
		pendingAddCounter.set(0);
		log(String.format("%s App:%s AddResource Resources:%s  ServiceToRun:%s allocCount:%d pendingAdd:%d", TAG, getOwnerId(), resourceIds, serviceToRun, allocatedResources.size()+1, pendingAddCounter.get()));
		addToCurrent(resourceIds, priority);
		
		workAllocator.assignWork(requestId, workingDirectory, resourceIds, priority, workAssignment, LifeCycle.State.PENDING);
		
		consumer.add(requestId, resourceIds, (AddListener) this);
		for (String resourceId : resourceIds) {
			this.allocatedResources.put(resourceId, resourceId);
		}
	}


	private void addToCurrent(List<String> resourceIds, int priority) {
		for (String string : resourceIds) {
			currentlyAdding.put(string, priority);
		}
	}


	/**
	 * Called by the SLA or client application when a resource is no longer needed.
	 * @param allocation
	 *  TODO: Finish off remove resource handing
	 */
	public void take(String requestId, String owner, java.util.List<String> resourceIds) {
		log(String.format("%s %s App:%s TakeResource Resource:%s", TAG, requestId, getOwnerId(), resourceIds));
		pendingAddCounter.set(0);
		// suppress TAKE actions without allocations - they WILL have already been called by release
		resourceIds.retainAll(this.allocatedResources.keySet());
		for (String resourceId : resourceIds) {
			this.consumer.take(requestId, resourceIds);
			this.allocatedResources.remove(resourceId);
			workAllocator.unassignWorkFromResource(requestId, resourceId, workAssignment.getBundleId(), workAssignment.getServiceName());
		}
		
	}
	
	/**
	 * Called by ResourceSpace where X number of resources must be removed. Its a chance for the consumer to free 
	 * the appropriate resources from the set (gracefully)
	 */
	public java.util.List<String> release(String requestId, java.util.List<String> resourceIds, int requiredCount) {
		pendingAddCounter.set(0);	
		log(String.format("%s %s Owner[%s] NegotiatedRelease Allocs[%d] Required[%d]", TAG, requestId, getOwnerId(), resourceIds.size(), requiredCount));
		
		// let consumer tell us what its expecting to release
		List<String> releasedResourceIds = new ArrayList<String>(consumer.release(requestId, resourceIds, requiredCount));
		
		workAllocator.assignWork(requestId, workingDirectory, releasedResourceIds, 9, workAssignment, LifeCycle.State.STOPPING);
		
		log(String.format("%s %s Owner[%s] NegotiatedRelease %s <<", TAG, requestId, getOwnerId(), releasedResourceIds));
		
		return releasedResourceIds;
	}
	
	
	/**
	 * Expects -lookup:tcp://localhost:15000/LookupSpace -sla:someSlaFile.xml -consumer:com.someConsumerClass 
	 * @param args
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		 
		LOGGER.info(TAG + " Starting SLAContainer... args:" + Arrays.toString(args));
		
		try {
			String lookupUri = getArg("-lookup:", args, "tcp://localhost:11000/LookupSpace", false);
			String slaFilename = getArg("-sla:", args, null, false);
			String serviceToRun = getArg("-serviceToRun:", args, null, false);
			String workingDirectory = getArg("-workingDirectory:", args, null, false);
			String fullBundleName = getArg("-bundleName:", args, null, false);
			String consumerName = getArg("-consumerName:", args, null, false);
			String consumerClassName = getArg("-consumerClass:", args, "notSpecified-ShouldBeInSLAFile", false);
			String consumerPercent = getArg("-consumerPercent:", args, "10.1%", false);
			String notifyFilter = getArg("-notifyFilter:", args, null, true);
			
			String agentAddress = getArg("-agentAddress:", args, null, true);
			String resourceId = getArg("-resourceId:", args, null, true);
			String slaContainerWorkId = getArg("-workId:", args, null, true);
			String serviceCriteria = getArg("-serviceCriteria:", args, "mflops > 1", true);
			int runInterval = Integer.parseInt(getArg("-runInterval:", args, "-1", true));
			
			LOGGER.debug("Using LookupSpace:" + lookupUri);
			ProxyFactoryImpl proxyFactory = null;
			TransportFactoryImpl transport = null; 
//			if (LookupSpaceImpl.sharedMapperFactory != null) {
//				proxyFactory = LookupSpaceImpl.sharedMapperFactory.getProxyFactory();
//				
//			} else {
				java.util.concurrent.ExecutorService executor = ExecutorService.newDynamicThreadPool("manager", "SLAContainer");
				transport = new TransportFactoryImpl(executor, "sla");
				transport.start();
				
				proxyFactory = new ProxyFactoryImpl(transport, VSOProperties.getBasePort() + 30, executor, "SLA_TestService");
				proxyFactory.start();
//			}
			final ProxyFactoryImpl finalProxyFactory = proxyFactory;  
			final TransportFactory finalTransport = transport;  
			final LookupSpace lookupSpace = LookupSpaceImpl.getRemoteService(lookupUri, proxyFactory,"SLACont");

			
			/**
			 * Lookup vscape objects
			 */
			ResourceSpace resourceSpace = ResourceSpaceImpl.getRemoteService("SLAContainer", lookupSpace, proxyFactory);
			BundleSpace bundleSpace = BundleSpaceImpl.getRemoteService("SLAContainer", lookupSpace, proxyFactory);
			BundleHandler bundleHandler = new BundleHandlerImpl(bundleSpace);
			MonitorSpace monitorSpace = SpaceServiceImpl.getRemoteService("SLAContainer", MonitorSpace.class, lookupSpace, proxyFactory, MonitorSpace.NAME, true, false);
			WorkAllocator workAllocator = WorkAllocatorImpl.getRemoteService("SLAContainer", lookupSpace, proxyFactory);
			ResourceAgent resourceAgent = proxyFactory.getRemoteService(ResourceAgent.NAME, ResourceAgent.class, agentAddress);
			
			LOGGER.info(String.format("%s Loading SLA:%s", TAG, slaFilename));
			LOGGER.info(String.format("%s SLAContainer address:%s", TAG, proxyFactory.getAddress()));
			
			/**
			 * Consumer setup
			 */
			SLA sla = null;
			String consumerClass = null;
			Consumer consumer = null;
			WorkAssignment workAssignment = bundleHandler.getWorkAssignmentForService(serviceToRun);
			
			// drive it by consumerClassname
			if (consumerClassName.equals(PercentConsumer.class.getName())) {
				LOGGER.info(String.format("%s Loading PercentConsumer:%s", TAG, consumerClassName));
				PercentConsumer percentConsumer = new PercentConsumer(consumerPercent, serviceCriteria, workAssignment.isBackground());
				sla = percentConsumer.getSLA();
				consumer = percentConsumer;
			}
			// drive it by the SLA
			else {
				sla = new SLASerializer().deSerializeFile(rfsPath + fullBundleName, slaFilename);
				consumerClass = sla.getConsumerClass();
						
				LOGGER.info(String.format("%s Loading Consumer:%s", TAG, consumerClass));
				Class<Consumer> aconsumerClass = (Class<Consumer>) Class.forName(consumerClass);
				consumer = aconsumerClass.newInstance();
				setLookupOnConsumer(aconsumerClass, consumer, lookupSpace, proxyFactory, resourceSpace, bundleHandler, workAssignment, lookupUri);
			}
			
			/**
			 * Create it
			 * 
			 */
			final SLAContainer slaContainer = new SLAContainer(slaContainerWorkId, serviceToRun, workAssignment, consumerName, consumer, sla, slaFilename, resourceSpace, workAllocator, 
					monitorSpace, proxyFactory.getAddress(), workingDirectory, fullBundleName, notifyFilter, proxyFactory, resourceAgent, new SLAValidator());
			slaContainer.setServiceCriteria(serviceCriteria);
			
			final ScheduledExecutorService scheduler = registerAdminOnLookupSpace(slaContainer, serviceToRun, fullBundleName, notifyFilter,
					serviceCriteria, proxyFactory, lookupSpace, resourceSpace, consumer, LOGGER);
			
			LOGGER.debug("Starting MainLoop");
			setupStatLogging(scheduler, slaContainer, LOGGER);

			final Logger loggerFinal = LOGGER;
			
			Runtime.getRuntime().addShutdownHook(new Thread(){
				public void run() {
					loggerFinal.warn("Shutdown SLAContainer:");
					scheduler.shutdownNow();
					if (finalProxyFactory != null) finalProxyFactory.stop();
					if (finalTransport != null) finalTransport.stop();
					
				}
			});

			int scheduledRunInterval = runInterval > 0 ? runInterval : consumer.getRunInterval();
			scheduler.scheduleWithFixedDelay(slaContainer, 5,  scheduledRunInterval, TimeUnit.SECONDS);
			scheduler.scheduleAtFixedRate(new Runnable(){
				public void run() {
					slaContainer.syncResources();
				}
			}, VSOProperties.getSlaSyncInterval(), VSOProperties.getSlaSyncInterval(), TimeUnit.SECONDS);
			
			
			while (true){
				Thread.sleep(1000);
			}
		} catch (Throwable t) {
			LOGGER.error(String.format("%s SLAContainerMain Failed:%s", TAG, t.getMessage()), t);
			throw new RuntimeException("SLAContainerFailed:" + t.getMessage(), t);
		}
	}


	private static ScheduledExecutorService registerAdminOnLookupSpace(SLAContainer slaContainer, String serviceToRun, String fullBundleName, String notifyFilter,
			String serviceCriteria, ProxyFactoryImpl proxyFactory, final LookupSpace lookupSpace, ResourceSpace resourceSpace,
			Consumer consumer, final Logger LOGGER) {
		
		final String adminId = slaContainer.getOwnerId() + "_ADMIN";
		/**
		 * Add management
		 */
		SLAContainerAdmin jmxAdminBean = new SLAContainerAdmin(null, consumer, resourceSpace, fullBundleName, serviceToRun, notifyFilter, serviceCriteria);
		jmxAdminBean.setContainer(slaContainer);
		JmxHtmlServerImpl jmxHtmlServer = new JmxHtmlServerImpl(proxyFactory.getAddress().getPort()+2, true);
		jmxHtmlServer.start();
		
		proxyFactory.registerMethodReceiver(adminId, jmxAdminBean);
		
		/**
		 * Wire into vscape
		 */
		final ServiceInfo serviceInfo = new ServiceInfo(adminId, proxyFactory.getAddress().toString(), SLAContainerAdminMBean.class.getName(), jmxHtmlServer.getURL(), fullBundleName, "", "UNKNOWN", VSOProperties.getResourceType());
		final String serviceLease = lookupSpace.registerService(serviceInfo, VSOProperties.getLUSpaceServiceLeaseInterval());
		
		final ScheduledExecutorService scheduler = ExecutorService.newScheduledThreadPool(5, new NamingThreadFactory("sla-sched"));
		LeaseRenewalService leaseManager = new LeaseRenewalService(lookupSpace, scheduler);
		
		leaseManager.add(new Renewer(lookupSpace, new Registrator() {
			public String register() {
				return lookupSpace.registerService(serviceInfo, VSOProperties.getLUSpaceServiceLeaseInterval());
			}
			public String info() {
				return lookupSpace.toString();
			}
			public void registrationFailed(int failedCount) {
			}}, serviceLease, VSOProperties.getLUSpaceServiceLeaseInterval(), adminId, LOGGER),
			VSOProperties.getLUSpaceServiceRenewInterval(), serviceLease);
		
		Runtime.getRuntime().addShutdownHook(new Thread(){
			public void run() {
				LOGGER.warn("Shutdown Scheduler:" + adminId);
				scheduler.shutdownNow();
			}
		});

		return scheduler;
	}



	private static void setLookupOnConsumer(Class<Consumer> aconsumerClass, Object consumer, LookupSpace lookupSpace, ProxyFactory proxyFactory, ResourceSpace resourceSpace, BundleHandler bundleHandler, WorkAssignment workAssignment, String lookupSpaceAddress) {
		try {
			Method method = aconsumerClass.getMethod("setLookupSpace", LookupSpace.class);
			method.invoke(consumer, lookupSpace);
		} catch (Throwable t) {
		}
		try {
			Method method = aconsumerClass.getMethod("setLookupSpaceAddress", String.class);
			method.invoke(consumer, lookupSpaceAddress);
		} catch (Throwable t) {
		}
		
		try {
			Method method = aconsumerClass.getMethod("setResourceSpace", ResourceSpace.class);
			method.invoke(consumer, resourceSpace);
		} catch (Throwable t) {
		}
		
		try {
			Method method = aconsumerClass.getMethod("setProxyFactory", ProxyFactory.class);
			method.invoke(consumer, proxyFactory);
		} catch (Throwable t) {}
		
		try {
			Method method = aconsumerClass.getMethod("setBundleHandler", BundleHandler.class);
			method.invoke(consumer, bundleHandler);
		} catch (Throwable t) {}
		
		try {
			Method method = aconsumerClass.getMethod("setWorkAssignment", WorkAssignment.class);
			method.invoke(consumer, workAssignment);
		} catch (Throwable t) {}
		
	}


	static String getArg(String key, String[] args, String defaultResult, boolean nullable) {
		for (String arg : args) {
			if (arg.startsWith(key)) return arg.replace(key, "");
		}
		if (defaultResult != null || nullable) return defaultResult;
		throw new RuntimeException("Argument:" + key + " was not found in:" + Arrays.toString(args));
	}

	public String getOwnerId() {
		return fullBundleName +"_" + this.consumerName;
	}
	
	public int getResourceCount() {
		return allocatedResources.size();
	}

	public SLA getSla() {
		return sla;
	}
	public void setServiceCriteria(String serviceCriteria) {
		this.serviceCriteria = serviceCriteria;
	}

	public void handleMessage(String message) {
		LOGGER.info(TAG + " SLAEVENT -> Message received: " + message);
	}
	public void log(String msg) {
		if (this.serviceToRun == null) LOGGER.info(this.serviceToRun + " " + msg);
		else LOGGER.info(msg);
	}

	public ServiceStatus getServiceStatus() {
		return this.status;
	}


	public void failed(String resourceId, String errorMsg) {
		LOGGER.warn(String.format("%s Consumer %s failed to use resource %s", TAG, consumerName, resourceId));
		currentlyAdding.remove(resourceId);
		allocatedResources.remove(resourceId);
		String workId = resourceId + ":" + serviceToRun;
		try {
			this.workAllocator.update(workId, "status replaceWith " + LifeCycle.State.ERROR + " AND errorMsg replaceWith '" + errorMsg + "'");
		} catch (Exception e) {
			LOGGER.error(e);
		}
		// free up the allocation
		resourceSpace.releasedResources(Arrays.asList(resourceId));
	}


	public void success(String resourceId) {
		String requestId = "SLAC_LST_SUCC" + getOwnerId() + counter++;
		Integer priority = currentlyAdding.remove(resourceId);
		if (priority == null) {
			LOGGER.warn("Failed to get priority for allocd resourceID:" + resourceId);
			LOGGER.warn("AddingResourceIdSet:" + currentlyAdding.keySet());
			priority = 10;
		}
		workAssignment.setWorkingDirectory(workingDirectory);
		workAssignment.addVariables(slaVariables);
		workAssignment.setPriority(priority);
		workAllocator.assignWork(requestId, resourceId, workAssignment);
	}
	public void pending(String requestId, List<String> resourceIds, String owner, int priority) {
	}
	public void satisfied(String requestId, String owner, List<String> resourceIds) {
	}
	
	private void pause() {
		try {
			Thread.sleep(100);
		} catch (InterruptedException e1) {}
	}
	

	static void setupStatLogging(ScheduledExecutorService newScheduledThreadPool, final SLAContainer slaContainer, final Logger LOGGER) {
		newScheduledThreadPool.scheduleWithFixedDelay(new Runnable() {
			public void run() {
				logStats(slaContainer, LOGGER);
			}
			
		}, 5, 5, TimeUnit.MINUTES);
	}


	static void logStats(SLAContainer slaContainer, Logger LOGGER){
		try {
			ResourceProfile profile = slaContainer.resourceProfile;
			if (profile == null) {
				profile = new ResourceProfile();
				slaContainer.resourceProfile = profile;
				profile.oneOffUpdate();
			}
			profile = slaContainer.resourceProfile;
			profile.updateValues();
			
			String cpuMsg = profile.getSystemStats(SLAContainer.class.getSimpleName(), profile.getHostName() + "/"+ slaContainer.getConsumer().toString());
			LOGGER.info(cpuMsg);
			String memMsg = String.format(Resource.MEM_FORMAT, SLAContainer.class.getSimpleName(), profile.getHostName() + "/" + slaContainer.getConsumer().toString(), profile.getMemoryMax(), profile.getMemoryCommitted(), profile.getMemoryUsed(), profile.getMemoryAvailable(), profile.physMemFreeMb, ResourceAgentImpl.timeDelta, profile.getCoreCount());
			LOGGER.info(memMsg);
					
			if (profile.getMemoryAvailable() < 2) {
				LOGGER.warn(memMsg);
			}
			
			
		} catch (Throwable t) {
			LOGGER.warn(t);
		}
	}
}
