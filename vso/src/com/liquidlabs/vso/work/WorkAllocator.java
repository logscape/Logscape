package com.liquidlabs.vso.work;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.proxy.clientHandlers.Broadcast;
import com.liquidlabs.transport.proxy.clientHandlers.Cacheable;
import com.liquidlabs.transport.proxy.clientHandlers.Decoupled;
import com.liquidlabs.transport.proxy.ReplayOnAddressChange;

import java.util.List;

public interface WorkAllocator extends LifeCycle {
	
	String NAME = WorkAllocator.class.getSimpleName();
	
	/**
	 * 
	 * @param requestId
	 * @param resourceId - should be of the format [hostname-[port]-[instance]]  i.e. alteredcarbon.local-12000-0
	 * @param workInfo
	 */
	void assignWork(String requestId, String resourceId, WorkAssignment workInfo);
	void assignWork(String requestId, String workingDirectory, List<String> resourceIds, int priority, WorkAssignment workAssignmentForService, State pending);
	int unassignWorkFromBundle(String bundleId);
	void unassignWorkFromResource(String requestId, String resourceName, String fullBundleName, String serviceName);
	void unassignWork(String requestId, String workAssignmentId);
	String bounce(String serviceId);
	
	@Decoupled
	void update(String id, String updateStatement) throws Exception;
	
	List<WorkAssignment> getWorkAssignmentsForBundle(String bundleId);
	
	@Cacheable(ttl=30)
	int getWorkIdCountForQuery(String query);
	
	List<Integer> getWorkIdCountForQuery(String... workIdQueries);
	String[] getWorkIdsForQuery(String query);
	String[] getWorkIdsAssignedToResource(String id);
	List<WorkAssignment> getWorkAssignmentsForQuery(String query);
	
	@Broadcast
	@ReplayOnAddressChange
	void registerWorkListener(WorkListener workListener, String resourceId, String listenerId, boolean replayMissedEvents) throws Exception;
	void unregisterWorkListener(String listenerId);
	URI getEndPoint();
	void saveWorkAssignment(WorkAssignment workInfo, String resourceId);
	int unassignWorkForQuery(String requestId, String query);
	
	boolean renewWorkLeases(String ownerId, int timeSeconds);
	boolean renewLease(String workListenerLease, int expires) throws Exception;
	void suspendWork(String requestId, String workAssignmentId);
	
	void removeWorkAssignmentsForResourceId(String agentId, long beforeTime);
	
}
