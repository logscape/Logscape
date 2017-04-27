package com.liquidlabs.vso.deployment.bundle;

import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.liquidlabs.common.UID;
import com.liquidlabs.common.concurrent.NamingThreadFactory;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.serialization.ObjectTranslator;
import com.liquidlabs.vso.SpaceService;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.agent.ResourceProfile;
import com.liquidlabs.vso.resource.ResourceRegisterListener;
import com.liquidlabs.vso.work.WorkAllocator;
import com.liquidlabs.vso.work.WorkAssignment;

/**
 * Allocated background tasks to Resources
 * @author neil
 */
public class BackgroundServiceAllocator implements ResourceRegisterListener {
	
	private static final String NAME = BackgroundServiceAllocator.class.getSimpleName();

	private static Logger LOGGER = Logger.getLogger(BackgroundServiceAllocator.class);
	
	SpaceService workToAllocate;
	String id = getClass().getSimpleName() + UID.getSimpleUID("");
	Map<String, Map<String, String>> allocatedTo = new ConcurrentHashMap<String, Map<String, String>>();
	
	Map<String, String> justAssignedTasks = new ConcurrentHashMap<String, String>();
	
	private WorkAllocator workAllocator;
	static int reqId = 0;
	ObjectTranslator query = new ObjectTranslator();
	private final ServiceFinder serviceFinder;

	private final ScheduledExecutorService scheduler;

	private BackgroundServiceAllocatorJMX jmx;

	private ExecutorService dispatcher;

	public BackgroundServiceAllocator(SpaceService pendingWork, Map<String, String> variables, WorkAllocator workAllocator, URI endPoint, ServiceFinder serviceFinder, ScheduledExecutorService scheduler) {
		this.workToAllocate = pendingWork;
		this.workAllocator = workAllocator;
		this.serviceFinder = serviceFinder;
		this.scheduler = scheduler;
		jmx = new BackgroundServiceAllocatorJMX(this);
		dispatcher = Executors.newSingleThreadExecutor(new NamingThreadFactory("BG-Handler"));
	}
	
	public void register(String resourceId, final ResourceProfile profile) {
		
 		dispatcher.execute(new Runnable() {
			public void run() {
				for (String key : workToAllocate.keySet(WorkAssignment.class)) {
					handleAssigment(key, profile);
				}
			}
		});
	}

	private void handleAssigment(String key, ResourceProfile profile) {
		String resourceId = profile.getResourceId();
		StringBuilder logOutput = new StringBuilder();
		boolean wasAssignedSoLogMsg = false;

		
		if (LOGGER.isDebugEnabled()) LOGGER.debug(String.format(" *************** Register %s deployed:%s workIds:%s tstamp:%s", resourceId, profile.getDeployedBundles(), profile.getWorkIds(), profile.getLastUpdated()));
		if (LOGGER.isDebugEnabled()) log(logOutput, String.format(" *************** Register %s deployed:%s workIds:%s", resourceId, profile.getDeployedBundles(), profile.getWorkIds()));
		if (LOGGER.isDebugEnabled()) log(logOutput, "AllWork:" + workToAllocate.keySet(WorkAssignment.class));

		
		try {
			
			WorkAssignment workAssignment = workToAllocate.findById(WorkAssignment.class, key);
			if (workAssignment == null) {
				LOGGER.warn("Failed to get WorkAssignment for:" + key);
				return;
			}
			if (!workAssignment.isBackground()) return;
			
			// a lazy hack - something from the bundle must be running in BG, however, something else like an SLA maycome along
			// and shutdown the firstBG Task to replace it with another Task from the same bundle (i.e. stop Downloader start Uploader)
			
			boolean profileMatch = query.isMatch(workAssignment.getResourceSelection(), profile);
			
			if (!profileMatch) return;
			
			// need to copy it so the workId is matching for this agent
			WorkAssignment workCopy = workAssignment.copy();
			workCopy.setResourceId(resourceId);
            String workKey = workCopy.getId() + profile.getStartTime();
			
			boolean wasJustAssigned = justAssignedTasks.containsKey(workKey);
			if (wasJustAssigned) return;
			
			boolean isAlreadyRunning = profile.getWorkIds().contains(workCopy.getId());
			if (isAlreadyRunning) return;
			
			boolean waitingForDeps = isWaitingForDependencies(workAssignment);
			if (waitingForDeps) return;

			// if we get this far we should probably assign the work to the agent because it hasnt started it!
			boolean shouldReAllocate = shouldReAllocate(resourceId, key, workCopy, allocatedTo, profile.getWorkIds());
			boolean isAllocated = allocatedToAgent(key, profile.getResourceId(), profile.getWorkIds(), workCopy.getId(), profile.getLastUpdated());
			
			if (workAssignment.getAllocationsOutstanding() == 0 && shouldReAllocate) {
				LOGGER.info("Found previously allocd:" + workAssignment + " missing, so realloc to:" + resourceId);
				workAssignment.setAllocationsOutstanding(1);
			}
			
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Checking:" + workAssignment.getId() +
				"\n    Match:" + profileMatch +
				"\n    ReAlloc:" + shouldReAllocate +
				"\n    IsAllocated:" + isAllocated +
				"\n    Selection:" + workAssignment.getResourceSelection() +
				"\n    IsWaitingForDeps:" + waitingForDeps);
			}
//			if (shouldReAllocate || !isAllocated) {
			
				
				
				if (LOGGER.isDebugEnabled()) log(logOutput, String.format(" [%s] **** Matched on resourceSelection [%s] ResourceProfile[%s]", workAssignment.getServiceName(), workAssignment.getResourceSelection(), profile));
				
				
				LOGGER.info(String.format(" Service[%s] ReAlloc?[%b] OutS[%d] **** Matched on Selection[%s] Profile[%s]", 
										workAssignment.getServiceName(),
										shouldReAllocate,
										workAssignment.getAllocationsOutstanding(),
										workAssignment.getResourceSelection(), 
										profile));
				
				// make use of AllocationsOutStanding
				if (workAssignment.getAllocationsOutstanding() > 0 || workAssignment.getAllocationsOutstanding() == -1) {
				
					if (workAssignment.getAllocationsOutstanding() != -1) {
							workAssignment.setAllocationsOutstanding(workAssignment.getAllocationsOutstanding() - 1);
							// Allocations Value was changed - so store it back
							workToAllocate.store(workAssignment, -1);
					}
					
					addToAllocated(key, resourceId, profile.getLastUpdated());
					workCopy.setAgentId(profile.getAgentId());
					workCopy.setProfileId(0);
					wasAssignedSoLogMsg = true;
					LOGGER.info(String.format("Assigning Background WorkRequest:%s => %s r:%s", workAssignment.getId(), resourceId, profile.getLastUpdated()));
					assignWork(resourceId, workCopy, workKey);
					
					
				}
//			}
		} catch (Throwable t) {
			LOGGER.warn("Failed to handle assignment", t);
		}
		if (wasAssignedSoLogMsg) 
			LOGGER.info(logOutput.toString());

	}

	private void assignWork(String resourceId, final WorkAssignment workCopy, final String workKey) {
		workAllocator.assignWork(getClass().getSimpleName() + "-" + reqId++, resourceId, workCopy);
		justAssignedTasks.put(workKey, workCopy.getId());
		scheduler.schedule(new Runnable() {
			public void run() {
				justAssignedTasks.remove(workKey);
			}
			
		}, 2, TimeUnit.MINUTES);
		
	}
	
	/**
	 * Check to see if the Profile is missing the expected workId
	 */
	private boolean shouldReAllocate(String resourceId, String workKey, WorkAssignment workAssignment, Map<String, Map<String, String>> allocatedTo2, String workIds) {
		Map<String, String> assignments = allocatedTo2.get(workKey);
		boolean wasAssigned = assignments == null ? false : assignments.containsKey(resourceId);
		boolean isAllocated = workIds.contains(workAssignment.getId());
		return (!isAllocated && wasAssigned);
	}

	private void log(StringBuilder logOutput, String format) {
		logOutput.append(format).append("\r\n\t");
	}

	private boolean isWaitingForDependencies(WorkAssignment workAssignment) {
		return serviceFinder.isWaitingForDependencies(workAssignment.getServiceName(), workAssignment.getBundleId());
	}

	synchronized private void addToAllocated(String workId, String resourceId, String lastUpdatedTime) {
		if (!allocatedTo.containsKey(workId)) {
			allocatedTo.put(workId, new ConcurrentHashMap<String, String>());
		}
		allocatedTo.get(workId).put(resourceId, lastUpdatedTime);
	}

	private boolean allocatedToAgent(String workId, String resourceId, String startedWorkIds, String workIdForThisProfile, String lastUpdated) {
		Map<String, String> allocsForWorkId = allocatedTo.get(workId);
		if (allocsForWorkId == null) {
			return false;
		}
		boolean contains = allocsForWorkId.containsKey(resourceId);
		if (contains) {
			boolean alreadyProcessedThisResourceUpdate = allocsForWorkId.get(resourceId).equals(lastUpdated);
			if (alreadyProcessedThisResourceUpdate) return true;
			if (!startedWorkIds.contains(workIdForThisProfile)) {
				LOGGER.info(String.format("Already allocated %s to %s but its workIds are [%s]", workId, resourceId, startedWorkIds));
				return false;
			}
			return true;
		}
		return false;
	}

	// remove BG tasks in-case are being bounced etc
	public void unregister(String resourceId, ResourceProfile resourceProfile) {
		if (LOGGER.isInfoEnabled()) LOGGER.info("Removing work from:" + resourceId);
		
		Set<Entry<String,Map<String, String>>> entrySet = allocatedTo.entrySet();
		for (Entry<String, Map<String, String>> entry : entrySet) {
			if (entry.getValue().remove(resourceId) != null) {
				WorkAssignment workAssignment = workToAllocate.findById(WorkAssignment.class, entry.getKey());
				if (workAssignment != null) {
					if (LOGGER.isInfoEnabled()) LOGGER.info("Removing " + resourceId + " from workAssignment " + workAssignment.getServiceName());
					synchronized(workAssignment) {
						if (workAssignment.getAllocationsOutstanding() != -1) {
							workAssignment.setAllocationsOutstanding(workAssignment.getAllocationsOutstanding() + 1);
						}
					}
				}
			}
		}
	}
	
	public String getId(){
		return BackgroundServiceAllocator.NAME + id;
	}

}
