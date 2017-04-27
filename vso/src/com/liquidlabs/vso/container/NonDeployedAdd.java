package com.liquidlabs.vso.container;

import java.util.List;

import org.apache.log4j.Logger;

import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.resource.ResourceSpace;

/**
 * Same as {@link Add} but doesn't need bundleDeployment
 *
 */
public class NonDeployedAdd implements Action {

	private static final Logger LOGGER = Logger.getLogger(NonDeployedAdd.class);
	
	private final String resourceTemplate;
	public Integer priority;
	private final Integer resources;
	private List<String> resourceGroups;

	public int satisfied;

	private String label = "";

	public NonDeployedAdd(String resourceTemplate, Integer resources) {
		this.resourceTemplate = resourceTemplate;
		this.resources = resources;
	}
	
	public void perform(ResourceSpace resourceSpace, String consumerId, Consumer consumer, String workIntent, String fullBundleName, SLAContainer slaContainer) {
		
		if (Boolean.getBoolean("is.validating")) return;
		
		String requestId = String.format("%s_SLAAdd_%s_%d", consumerId, label, slaContainer.counter++);
		String resourceRequest = resourceTemplate;
		try {
			LOGGER.info(String.format("ResourceSpace RequestResources:%s",requestId));
			slaContainer.pendingAddCounter.set(1);
			satisfied = resourceSpace.requestResources(requestId, resources, priority, resourceRequest, workIntent, VSOProperties.getResourceRequestTimeout(), consumerId, label);
			LOGGER.info(requestId + " Given:" + satisfied);
			if (satisfied == 0) slaContainer.pendingAddCounter.set(0);
		} catch (Exception e) {
			slaContainer.pendingAddCounter.set(0);
			throw new RuntimeException(e);
		}
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
