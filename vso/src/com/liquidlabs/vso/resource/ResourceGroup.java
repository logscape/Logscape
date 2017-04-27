package com.liquidlabs.vso.resource;

import java.util.List;
import java.util.Map;

import com.liquidlabs.orm.Id;

public class ResourceGroup {
	@Id
	private String name;
	private String resourceSelection;
	private String description;
	private String date;

	public ResourceGroup() {}
	
	public ResourceGroup(String name, String resourceSelection, String description, String date) {
		this.name = name;
		this.resourceSelection = resourceSelection;
		this.description = description;
		this.date = date;
	}

	public String getName() {
		return name;
	}

	public String getResourceSelection() {
		return resourceSelection;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public void setResourceSelection(String resourceSelection) {
		this.resourceSelection = resourceSelection;
	}
	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getDate() {
		return date;
	}

	public boolean hasChanged(ResourceSpace resourceSpace, Map<String, String> resourceGroupEval) {
		List<String> allResourceIds = resourceSpace.findResourceIdsBy(this.resourceSelection);
		String newValue = allResourceIds.toString();
		boolean changed = !resourceGroupEval.containsKey(this.name) || !newValue.equals(resourceGroupEval.get(this.name));
		resourceGroupEval.put(this.name, newValue);
		return changed;
	}

	
	
}
