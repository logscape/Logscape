package com.liquidlabs.vso.container;

import java.util.List;

import com.liquidlabs.vso.resource.ResourceSpace;

public interface Action {
	void perform(ResourceSpace resourceSpace, String consumerId, Consumer consumer, String workIntent, String fullBundleName, SLAContainer slaContainer);

	void setPriority(int priority);

	void setResourceGroups(List<String> resourceGroups);
	
	void setLabel(String label);

}
