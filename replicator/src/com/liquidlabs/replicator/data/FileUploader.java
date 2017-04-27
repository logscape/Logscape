package com.liquidlabs.replicator.data;

import com.liquidlabs.orm.Id;

public class FileUploader {
	@Id
	private String agentAddress;
	
	public FileUploader() {}
	
	public FileUploader(String agentAddress) {
		this.agentAddress = agentAddress;
	}
	
	public String getAgentAddress() {
		return agentAddress;
	}
}
