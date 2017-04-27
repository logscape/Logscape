package com.liquidlabs.vso.deployment.bundle;

import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.liquidlabs.common.UID;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.serialization.ObjectTranslator;
import com.liquidlabs.vso.SpaceService;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.agent.ResourceProfile;
import com.liquidlabs.vso.resource.ResourceSpace;
import com.liquidlabs.vso.work.WorkAssignment;

/**
 * Used to assign background work assignments - <br>
 * i.e. WorkAssignments that run on everything that registers
 */
public class BundleRRegListenerImpl implements BundleRRegListener {
	
	private final static Logger LOGGER = Logger.getLogger(BundleRRegListenerImpl.class);
	public static final String NAME = BundleRRegListenerImpl.class.getSimpleName();
	
	String id = getClass().getSimpleName() + UID.getSimpleUID("");
	static int reqId = 0;

	SpaceService workToAllocate;
	Map<String, String> variables;
	ObjectTranslator query = new ObjectTranslator();
//	Map<String, ResourceRequest> outstandingRequests = new ConcurrentHashMap<String, ResourceRequest>();

	private URI endPoint;
	private ResourceSpace resourceSpace;
	private String serviceListenerId;
	
	
	public BundleRRegListenerImpl() {
	}

	public BundleRRegListenerImpl(ResourceSpace resourceSpace, SpaceService pendingWork, Map<String, String> variables, URI endPoint) {
		this.resourceSpace = resourceSpace;
		this.workToAllocate = pendingWork;
		this.variables = variables;
		this.endPoint = endPoint;
	}

	/**
	 * When a ResourceIsRegistered try and assign work to it, then let the AllocListener take over
	 */
	private volatile boolean isRegistering;
	public  void register(String resourceId, ResourceProfile resourceProfile) {
		
		
		if (VSOProperties.isFailoverNode()) return;
		if (isRegistering) return;
		
		try {
			isRegistering = true;
			
			if (LOGGER.isDebugEnabled()) LOGGER.debug(String.format(" * Received [%s]\n\tOutstanding:%d\n\tWorkToAlloc:%s", resourceProfile, -1, workToAllocate.size()));
				
			int satisfiedCount = 0;
			int count = 0;
			Set<String> workIds = workToAllocate.keySet(WorkAssignment.class);
			for (String workId : workIds) {
				try {
					WorkAssignment workAssignment = workToAllocate.findById(WorkAssignment.class, workId);
					
					if (workAssignment.isBackground()) continue;
					if (workAssignment.getAllocationsOutstanding() <= 0) {
						continue;
					}
				
					boolean match = query.isMatch(workAssignment.getResourceSelection(), resourceProfile);
					if (match){
					
					
							WorkAssignment workCopy = workAssignment.copy();
							workCopy.setResourceId(resourceId);
							if (resourceProfile.getWorkIdsAsList().contains(workCopy.getId())) continue;
							
							LOGGER.info(String.format("%d) Checking Work:%s Needed:%d Resource:%s", count++, workAssignment.getId(), workCopy.getAllocationsOutstanding(), resourceId));
							
							
							// Explicitly Request this Resource, ResourceSpace will callback onto ServiceAllocator
							try {
								// we hid the request id in the resourceId
								String requestId = workAssignment.getResourceId();
								satisfiedCount = resourceSpace.requestResources(requestId, workCopy.getAllocationsOutstanding(), workCopy.getPriority(), "resourceId equals " + resourceProfile.getResourceId(), workAssignment.getId(), VSOProperties.getLUSpaceServiceLeaseInterval(), serviceListenerId, "");
								// slow the events down a bit - its possible to have multiple incoming event updates which thrash the allocation requests and means we end up with too much being allocated
								Thread.sleep(VSOProperties.getAllocDelay());
							} catch (Exception e) {
								LOGGER.error("Failed to requestResource:", e);
							}
							if (satisfiedCount == 0){
								LOGGER.info("Resource DENIED - Count:" + satisfiedCount + " work:" + workAssignment.getId() + " resource:" + resourceId);
							} else {
								LOGGER.info("Resource ALLOCD - Count:" + satisfiedCount + " work:" + workAssignment.getId() + " resource:" + resourceId);					
							}
							workAssignment.decrementAllocationsOutstanding(satisfiedCount);
								LOGGER.info(" Outstanding request partially satisfied, reqId:" + workAssignment.getResourceId()  + " *updated* workAssignment:" + workAssignment);
							if (satisfiedCount > 0){
								resourceProfile.setCustomProperties(workAssignment.getProperies());
							}
							workToAllocate.store(workAssignment, -1);
					} else {
						LOGGER.debug(String.format("No resource match on[%s] AND Resource [%s]", workAssignment.getResourceSelection(),resourceProfile));
						LOGGER.debug(String.format("\tDeployedBundles:%s", resourceProfile.getDeployedBundles()));
						LOGGER.debug(String.format("\tCustomProperties:%s", resourceProfile.getCustomProperties()));
					}
				} catch (Throwable t) {
					LOGGER.error("Failed to Assign Work:" + workId, t);
				}
				if (satisfiedCount > 0) continue;
				}
		} finally {
			isRegistering = false;
		}
	}
	

	public void unregister(String resourceId, ResourceProfile resourceProfile) {
		LOGGER.debug("Unassign Work from:" + resourceId);
	}
	public String getId(){
		return BundleRRegListenerImpl.NAME + id;
	}

	public void setServiceListenerId(String serviceListenerId) {
		this.serviceListenerId = serviceListenerId;
	}
}
