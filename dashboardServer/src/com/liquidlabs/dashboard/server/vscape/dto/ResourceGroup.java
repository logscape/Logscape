package com.liquidlabs.dashboard.server.vscape.dto;


public class ResourceGroup {
	public String name;
	public String selection;
	public String description;
	public String date;

	public ResourceGroup() {}
	
	public ResourceGroup(String name, String resourceSelection, String description, String date) {
		this.name = name;
		this.selection = resourceSelection;
		this.description = description;
		this.date = date;
	}

	
	
}
