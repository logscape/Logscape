package com.liquidlabs.vso.container;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.liquidlabs.vso.work.InvokableUI;


public interface Consumer {

	Metric[] collectMetrics();

	int getUsedResourceCount();
	
	/**
	 * SLA driven release (called from Remove event)
	 * @param template TODO
	 */
	List<String> getResourceIdsToRelease(String template, Integer resourcesToFree);
	
	/**
	 * Called when you are being told either by VScape to release a resource
	 */
	void take(String requestId, List<String> resourceIds);
	
	/**
	 * Request for Resources to be released
	 * Choose (requiredCount) any resourceIds from the given list. The pending
	 * list is held and once actually released the same resourceIds
	 * should be returned through getReleasedResources;
	 * @param requestId TODO
	 */
	List<String> release(String requestId, List<String> resourceIds, int requiredCount);
	/**
	 * Once the resource is actually free to be removed it is added to this list.
	 * Each call should clear the consumers free list.
	 */
	List<String> getReleasedResources();

	
	/**
	 * Consumer MUST callback onto the addListener.success/fail
	 */
	void add(String requestId, List<String> resourceIds, AddListener addListener);

	void setVariables(Map<String,String> propertyMap);

	Set<String> collectResourceIdsForSync();

	InvokableUI getUI();

	void setInfo(String consumerId, String workIntent, String fullBundleName);

	/**
	 * called every 60 seconds to alloc the consumer to correct its expectations against its vscape allocations (i.e. engines versus resources).
	 * @param set 
	 */
	void synchronizeResources(Set<String> expectedResourceIds);
	
	int getRunInterval();


}
