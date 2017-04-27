package com.liquidlabs.vso.resource;

import com.liquidlabs.common.Pair;
import com.liquidlabs.common.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.space.lease.Leasor;
import com.liquidlabs.transport.proxy.clientHandlers.Cacheable;
import com.liquidlabs.transport.proxy.clientHandlers.FailFastOnce;
import com.liquidlabs.transport.proxy.ReplayOnAddressChange;
import com.liquidlabs.vso.agent.ResourceProfile;

public interface ResourceSpace extends LifeCycle, Leasor {
	
	
	String NAME = "ResourceSpace";


	@ReplayOnAddressChange
	String registerAllocListener(AllocListener owner, String listenerId, String ownerId);
	
	@FailFastOnce
	void unregisterAllocListener(String listenerId);
	
	/**
	 * Return the number of resources expected to be returned
	 * @param requestId TODO
	 * @param ownerLabel TODO
	 * @return
	 */
	int requestResources(String requestId, int count, int priority, String template, String workIntent, int timeoutSeconds, String owner, String ownerLabel);
	
	/**
	 * Called by ResourceOwner when resources have been Removed or Released
	 * @param resourceIds
	 */
	void releasedResources(List<String> resourceIds);
	
	
	/**
	 * Called to renew allocLease relating to this owner
	 */
	void renewAllocLeasesForOwner(String owner, int timeSeconds) throws Exception;

	void assignResources(String requestId, List<String> resourceIds, String owner, int priority, String intent, int leaseTimeout);
	void forceFreeResourceAllocation(String owner, String requestId, String resourceId);
	String getLeaseForAllocation(String allocId);
	
	@ReplayOnAddressChange
	void registerResourceRegisterListener(ResourceRegisterListener rrListener, String listenerId, String resourceSelection, int timeout) throws Exception;
	
	@FailFastOnce
	void unregisterResourceRegisterListener(String id);

	int getResourceCount(String serviceCriteria);
	List<String> findResourceIdsBy(String template);
	String registerResource(ResourceProfile resourceProfile, int expires) throws Exception;
	void unregisterResource(String owner, String resourceId);
	List<ResourceProfile> findResourceProfilesBy(String query);
	ResourceProfile getResourceDetails(String key);

	String[] getAllocIdsForQuery(String string);
	List<Allocation> getAllocsForQuery(String string);

	URI getEndPoint();
	void registerResourceGroup(ResourceGroup resourceGroup);
	void unRegisterResourceGroup(String name);
	List<ResourceGroup> getResourceGroups();
	ResourceGroup findResourceGroup(String name);

	/**
	 * Takes clist of the form host123,host234,host*567,*lonrc*,group:Linux
	 * @return
	 */
	@Cacheable(ttl=20)	
	Set<String> expandIntoResourceIds(Set<String> hostsList);
	
	@Cacheable(ttl=20)
	Set<String> expandGroupIntoHostnames(String groupName);
	
	String[] getResourceIdsForAssigned(String ownerId);
	int removeResources(String requestId, String query);

	@ReplayOnAddressChange
	void registerRequestEventListener(RequestEventListener listener, String listenerId);
	void unregisterRequestEventListener(String id);
	
	/**
	 * Numeric counter used to index/id each resource in the system
	 * @return
	 */
	int getSystemResourceId();
	
	/**
	 * BG Request alloc handling
	 * 
	 */
	void releasedBGResources(String TAG, List<String> resourceIds, String owner);
	int requestBGResources(String requestId, int count, int priority, String template, String workIntent, int timeoutSeconds, String owner,	String ownerLabel);
	
	Collection<String> getLostResources();

	void resourceGroupListener(ResourceGroupListener listener, String listenerId);

	Set<String> expandSubstringIntoHostnames(String substring);

	void releaseAllocsForOwner(String id);

	boolean isStarted();


    static final String ID_0 = " workdId contains Indexer AND type notContains Management AND type notContains Failover";
    static final String ID_1 = " workdId contains IndexStore AND type notContains Management AND type notContains Failover";

    String exportConfig(String filter);
    void  importConfig(String config);

    BloomMatcher expandGroupIntoBloomFilter(String groupName, String... resourceGroups);

	void clearLostAgents();

    void setLLC(long llc);
}
