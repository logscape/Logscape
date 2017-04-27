package com.liquidlabs.vso.container;

import java.util.List;

import org.apache.log4j.Logger;

import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.resource.ResourceSpace;

public class Add implements Action {

	private static final Logger LOGGER = Logger.getLogger(Add.class);
	
	private final String resourceTemplate;
	protected Integer priority;
	protected final Integer resources;
	private List<String> resourceGroups;
	protected String label = "";

	public Add(String resourceTemplate, Integer resources) {
		this.resourceTemplate = resourceTemplate;
		this.resources = resources;
	}
	
	public void perform(ResourceSpace resourceSpace, String consumerId, Consumer consumer, String workIntent, String fullBundleName, SLAContainer slaContainer) {
		
		if (Boolean.getBoolean("is.validating")) return;
		
		String requestId = String.format("%s_SLAAdd_%s_%d", consumerId,label,slaContainer.counter++);
		String resourceRequest = "(" + resourceTemplate  + ") AND deployedBundles contains '" + fullBundleName + "' AND customProperties notContains ServiceType=System";
		try {
			LOGGER.info(String.format("ResourceSpace RequestResources %s", requestId));
			slaContainer.pendingAddCounter.set(1);
			int satisfiedCount = requestResources(resourceSpace, consumerId, workIntent, requestId, resourceRequest);
			LOGGER.info(requestId + " Given:" + satisfiedCount);
			if (satisfiedCount == 0) slaContainer.pendingAddCounter.set(0);
		} catch (Exception e) {
			slaContainer.pendingAddCounter.set(0);
			throw new RuntimeException(e); 
		}
	}

	protected int requestResources(ResourceSpace resourceSpace, String consumerId, String workIntent, String requestId, String resourceRequest) {
		return resourceSpace.requestResources(requestId, resources, priority, resourceRequest, workIntent, VSOProperties.getResourceRequestTimeout(), consumerId, label);
	}
	

	public String toString(){
		return getClass().getName() + " templ:" + resourceTemplate + " #:" + resources + " priority:" + priority;
	}

	public void setPriority(int priority) {
		this.priority = priority;
	}

	public void setResourceGroups(List<String> resourceGroups) {
		this.resourceGroups = resourceGroups;
	}
	public void setLabel(String label) {
		this.label = label.replaceAll(" ", "_");
	}

}
