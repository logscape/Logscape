package com.liquidlabs.vso.container;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.liquidlabs.vso.resource.ResourceSpace;

public class Remove implements Action{

	private final static Logger LOGGER = Logger.getLogger(Remove.class);
	private final String resourceTemplate;
	private final Integer countToFree;
	private String label = "";
	
	public Remove(String resourceTemplate, Integer resources) {
		this.resourceTemplate = resourceTemplate;
		this.countToFree = resources;
	}
	public void perform(ResourceSpace resourceSpace, String consumerId, Consumer consumer, String workIntent, String fullBundleName, SLAContainer slaContainer) {
		if (Boolean.getBoolean("is.validating")) return;
		
		List<String> resourceNames =  new ArrayList<String>(consumer.getResourceIdsToRelease(resourceTemplate, this.countToFree));
		for (String resourceId : resourceNames) {
			String requestId = consumerId +"_SLARemove:" + label + slaContainer.counter++;
			resourceSpace.forceFreeResourceAllocation(consumerId, requestId, resourceId);
		}
	}
	@Override
	public String toString() {
		return getClass().getName() + " templ:" + resourceTemplate + " #:" + countToFree;
	}
	public void setPriority(int priority) {
	}
	public void setResourceGroups(List<String> resourceGroups) {
	}
	public void setLabel(String label) {
		this.label = label.replaceAll(" ", "_");
	}

}
