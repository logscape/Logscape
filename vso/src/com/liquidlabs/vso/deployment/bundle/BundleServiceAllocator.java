package com.liquidlabs.vso.deployment.bundle;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.vso.SpaceService;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.resource.AllocListener;
import com.liquidlabs.vso.resource.ResourceSpace;
import com.liquidlabs.vso.work.WorkAllocator;
import com.liquidlabs.vso.work.WorkAssignment;

public class BundleServiceAllocator implements AllocListener {
	String id = VSOProperties.getBasePort() + "-" + NetworkUtils.getHostname();
	public static final String NAME = "BundleSvcAlloc";
	private static Logger LOGGER = Logger.getLogger(BundleServiceAllocator.class);
	private transient SpaceService workToAllocate;
	private transient Map<String, String> variables;
	private transient WorkAllocator workAllocator;
	private final ResourceSpace resourceSpace;
	private long requestId = 0;
	
	public BundleServiceAllocator(SpaceService pendingWork, Map<String, String> variables, BundleRRegListener bundleRRegListener, WorkAllocator workAllocator, URI endPoint, final ResourceSpace resourceSpace, ScheduledExecutorService scheduler) {
		this.workToAllocate = pendingWork;
		this.variables = variables;
		this.workAllocator = workAllocator;
		this.resourceSpace = resourceSpace;
		
		// we may have stale items from before
		this.resourceSpace.releaseAllocsForOwner(getId());
		
		
		scheduler.scheduleAtFixedRate(new Runnable(){
			public void run() {
				try {
					resourceSpace.renewAllocLeasesForOwner(getId(), VSOProperties.getLUSpaceServiceLeaseInterval());
				} catch (Exception e) {
					LOGGER.warn("Failed to renewAllocLeases:" + e.toString(), e);
				}
			}
		}, 1, VSOProperties.getLUSpaceServiceRenewInterval(), TimeUnit.SECONDS);
		
	}
	public String getId(){
		return BundleServiceAllocator.NAME + id;
	}
	
	/**
	 * Same as below
	 */
	public void take(String requestId, String owner, List<String> resourceIds) {
		release(requestId, resourceIds, 1);
	}
	public void pending(String requestId, List<String> asList, String owner, int priority) {
	}
	public void satisfied(String requestId, String owner, List<String> asList) {
	}
	
	/**
	 * Request has been made such that the resources must be released.
	 * In which case this service is the owner and otherwise it would be given to an SLAContainer - so we
	 * grab the workAssignment
	 */
	public java.util.List<String> release(String requestIdString, java.util.List<String> resourceIds, int requiredCount) {
		if (LOGGER.isInfoEnabled()) LOGGER.info(String.format("%s Release:%s requiredCount:%d", requestIdString, resourceIds, requiredCount));
		int releasedCount = 0;
		
		ArrayList<String> result = new ArrayList<String>();
		
		for (String resourceId : resourceIds) {
			if (releasedCount++ < releasedCount) {
				result.add(resourceId);
				List<WorkAssignment> workAssignmentsForQuery = workAllocator.getWorkAssignmentsForQuery("resourceId equals " + resourceId);
				int count = 0;
				if (workAssignmentsForQuery == null || workAssignmentsForQuery.size() == 0) continue;
				if (LOGGER.isDebugEnabled()) LOGGER.debug("Got WorkAssignment:" + workAssignmentsForQuery);
				for (WorkAssignment reallocWork : workAssignmentsForQuery) {
					if (reallocWork.isBackground()) continue;
					
					String requestId2 = "BundleService-Release:" + requestId + "--" + requestIdString;
					
					LOGGER.info(count++ + ") Releasing:" + workAssignmentsForQuery + " ResourceSel:" + reallocWork.getResourceSelection());
					
					reallocWork.setAllocationsOutstanding(1);
					this.workToAllocate.store(reallocWork, -1);
					workAllocator.unassignWork(requestId2, reallocWork.getId());
					try {
						resourceSpace.requestResources(requestId2, reallocWork.getAllocationsOutstanding(), reallocWork.getPriority(), reallocWork.getResourceSelection(), reallocWork.getId(), VSOProperties.getLUSpaceServiceLeaseInterval(), getId(), "");
					} catch (Exception e) {
						LOGGER.error("Failed to requestResource:", e);
					}
					
					requestId++;
				}
			}
		}
		if (LOGGER.isDebugEnabled()) LOGGER.debug("ResourceRelease returning allocation:" + result); 
		return result;
	}

	/**
	 * Call-back when a Resource has been allocated to us, 
	 */
	public void add(String requestId, List<String> resourceIds, String owner, int priority) {
		if (requestId == null) throw new RuntimeException("Cannot process NULL requestId, for owner:" + owner);
		LOGGER.info("Add: (Resource allocated)" + resourceIds + " Going to start RequestId:" + requestId);
		for (String resourceId : resourceIds) {
			
			try {
				String[] findIds = workToAllocate.findIds(WorkAssignment.class, "resourceId equals " + requestId);
				if (findIds == null || findIds.length == 0) {
					
					LOGGER.error(String.format("\n\t**** WorkQueue did not contain Items for RequestId[%s] Resource[%s] -Freeing ResourceSpace ALLOC", requestId, resourceId));
					LOGGER.error(String.format("Pending WorkQueue Items[%s]", workToAllocate.size()));
//					resourceSpace.releasedResources(Arrays.asList(resourceId));
					
					return;
				}
				WorkAssignment workInfo = workToAllocate.findById(WorkAssignment.class, findIds[0]);
				
				if (LOGGER.isDebugEnabled()) LOGGER.debug(String.format("AddResource, RequestID:%s ResourceId:%s Work:%s AllocOutstanding[%d]", requestId,resourceId,workInfo.getId(), workInfo.getAllocationsOutstanding()));
				// decrement satisfied work allocation and update the map
				workInfo.setAllocationsOutstanding(workInfo.getAllocationsOutstanding()-1);
				workToAllocate.store(workInfo, -1);
	
				// if the workInfo is satisfied and not background the remove it
				if (workInfo.getAllocationsOutstanding() == 0 && !workInfo.isBackground()){
					workToAllocate.remove(WorkAssignment.class, workInfo.getId());
					LOGGER.info("WorkRequestsSatisfied - removing:" + workInfo.getId() + " outstanding:" + workInfo.getAllocationsOutstanding());
				}
				if (LOGGER.isDebugEnabled()) LOGGER.info(String.format("====>>>Work:%s requestId:%s resourceId:%s outstanding:%d", workInfo.getId(), requestId, resourceId, workInfo.getAllocationsOutstanding()));
				updateVariableListAndScript(variables, workInfo);
				
				workAllocator.assignWork(requestId, resourceId, workInfo);
				
				// now apply the pauseSeconds property 
				// that forces workAssignements to wait for specified period
				if (LOGGER.isDebugEnabled()) LOGGER.debug(">>Pause for :" + workInfo.getPauseSeconds());
				pause(workInfo.getPauseSeconds());
				if (LOGGER.isDebugEnabled()) LOGGER.debug("<<Pause done:" + workInfo.getPauseSeconds());
			} catch (Exception e) {
				LOGGER.error("Failed to allocate work:" + resourceId, e);		
			}
		}
	}
	public boolean isWorkAssignmentPending(WorkAssignment workToCheckOn){
		WorkAssignment findById = this.workToAllocate.findById(WorkAssignment.class, workToCheckOn.getId());
		// TODO: should this be 1?
		return findById != null && findById.getAllocationsOutstanding() > 1;
		
////		Collection<WorkAssignment> workToAllocate = this.workToAllocate.values();
//		for (WorkAssignment workAssignment : workToAllocate) {
////			if (!workAssignment.isBackground()) return true;
//			if (workAssignment.equals(workToCheckOn) && workAssignment.getAllocationsOutstanding() > 1) return true;
//		}
//		return false;		
	}
	
	private void pause(int pauseSeconds) {
		try {
			Thread.sleep(pauseSeconds * 1000);
		} catch (InterruptedException e) {
		}
	}
	

	void updateVariableListAndScript(Map<String, String> variables2, WorkAssignment workInfo) {
		variables2.put(workInfo.getServiceName(), workInfo.getResourceId().replaceAll("-", ":"));
		variables2.put(workInfo.getServiceName()+ "_Id", workInfo.getResourceId());
		variables2.put(workInfo.getServiceName()+ "_host", workInfo.getResourceId().split("-")[0]);
		for (String key : variables2.keySet()) {
			String workInfoScript = workInfo.getScript();
			if (workInfoScript != null && workInfoScript.contains( "${" + key + "}")) {
				LOGGER.debug("Updating script " + workInfo.getId() + " with variable:" + key);
				workInfo.setScript(workInfoScript.replaceAll("\\$\\{" + key + "\\}", variables2.get(key)));
			}
		}
	}
}
