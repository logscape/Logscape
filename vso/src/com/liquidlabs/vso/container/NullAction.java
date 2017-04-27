package com.liquidlabs.vso.container;

import java.util.List;

import org.apache.log4j.Logger;

import com.liquidlabs.vso.resource.ResourceSpace;

public class NullAction implements Action {

	private final static Logger LOGGER = Logger.getLogger(NullAction.class);
	private String msg;
	
	
	public NullAction(String msg){
		this.msg = msg;
	}
	public void perform(ResourceSpace resourceSpace, String consumerId, Consumer consumer, String workIntent, String fullBundleName, SLAContainer slaContainer) {
		// LOGGER.info("Executing Null Action:" + msg);
	}

	public void setPriority(int priority) {
	}
	public String toString(){
		return String.format("%s msg[%s]", getClass().getSimpleName(), msg);
	}
	public void setResourceGroups(List<String> resourceGroups) {
	}
	public void setLabel(String label) {
	}

}
