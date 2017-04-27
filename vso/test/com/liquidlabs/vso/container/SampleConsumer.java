package com.liquidlabs.vso.container;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.vso.work.InvokableUI;
import com.liquidlabs.vso.work.WorkAllocator;

public class SampleConsumer implements Consumer{
	private double queueLength;
	private Set<String> resources = new HashSet<String>();
	private Map<String, String> propertyMap;
	
	class GenericMetric implements Metric {
		
		public String name() {
			return "queueLength";
		}

		public Double value() {
			return queueLength;
		}
		
	}
	
	// WorkAllocator
	public void add(String requestId, List<String> resourceIds, AddListener addListener) {
		resources.addAll(resourceIds);
	}
	
	public Metric[] collectMetrics() {
		return new Metric [] {new GenericMetric()};
	}

	public String name() {
		return "Sample";
	}
	public void setName(String name) {
	}
	public Set<String> collectResourceIdsForSync() {
		return resources;
	}

	public void take(String requestId, List<String> resourceIds) {
		resources.removeAll(resourceIds);
	}
	
	CopyOnWriteArrayList<String> releasedResources = new CopyOnWriteArrayList<String>();

	public List<String> release(String requestId, List<String> resourceIds, int requiredCount) {
		ArrayList<String> results = new ArrayList<String>();
		for (int i = 0; i < requiredCount; i++){
			resources.remove(resourceIds.get(i));
			results.add(resourceIds.get(i));
		}
		releasedResources.addAll(results);
		return results;
	}
	
	public List<String> getReleasedResources() {
		ArrayList<String> results = new ArrayList<String>(releasedResources);
		releasedResources.clear();
		return results;
	}

	public List<String> getResourceIdsToRelease(String template, Integer resourcesToFree) {
		ArrayList<String> result = new ArrayList<String>();
		for (String resourceId : resources) {
			if (result.size() < resourcesToFree) {
				result.add(resourceId);
			}
		}
		this.resources.removeAll(result);
		this.releasedResources.addAll(result);
		return result;
	}
	public int getUsedResourceCount() {
		return resources.size();
	}

	
	public void setVariables(Map<String, String> propertyMap) {
		this.propertyMap = propertyMap;
	}
	/**
	 *  UI method
	 */
	public String getUsername(String arg0){
		return "a username";
	}
	/**
	 * Return XML describing the UI
	 */	
	public InvokableUI getUI() {
		return null;
	}
	public void setInfo(String consumerId, String serviceToRun, String fullBundleName) {
	}
	public void synchronizeResources(Set<String> set) {
	}
	public int getRunInterval() {
		return 60;
	}
}
