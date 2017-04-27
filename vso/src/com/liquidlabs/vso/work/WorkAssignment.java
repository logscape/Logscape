package com.liquidlabs.vso.work;

import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTimeUtils;
import org.joda.time.format.DateTimeFormat;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.collection.PropertyMap;
import com.liquidlabs.orm.Id;
import com.liquidlabs.transport.serialization.Convertor;
import com.liquidlabs.vso.agent.ResourceAgent;
import com.liquidlabs.vso.deployment.bundle.Bundle;
import com.liquidlabs.vso.deployment.bundle.Service;

/**
 * Represents a work being assigned to a particular {@link ResourceAgent}
 * WorkAssignments are created when {@link Bundle} is deployed and {@link Service}s
 * are processed and resources allocated.
 * <br>
 * <br>
 * Note: background means that the workassignment request will hang around forever.
 * This is used by the WorkAllocator.ResourceRegisterListener to assign work to everything
 * that registers
 */

public class WorkAssignment {
	@Id
	String id ="";
	String resourceId;
	String agentId;
	String bundleId;
	String serviceName;
	String properties;
	String script;
	private int allocationsOutstanding;
	int pauseSeconds;
	boolean background;
	String resourceSelection;
	boolean fork;
	private String workingDirectory = ".";
	private int priority;
	private String variables;
	private long startTimeMs;
	private String startTime;
	private String slaFilename;
	private String overridesService ="";
	private int profileId;
	private int pid; 
	
	private String status = LifeCycle.State.STOPPED.name();
	private String errorMsg = "";
	private boolean systemService;
	private String invokableEP;
	private double costPerUnit;
	
	String timestamp;
	long timestampMs;
	
	public WorkAssignment(){
	}
	public WorkAssignment(String agentId, String resourceId, int profileId, String bundleId, String serviceName, String script, int priority) {
		this.agentId = agentId;
		this.resourceId = resourceId;
		this.profileId = profileId;
		this.bundleId = bundleId;
		this.serviceName = serviceName;
		this.script = script;
		this.priority = priority;
		this.id = setId(resourceId, bundleId, serviceName);
	}
	
	public WorkAssignment(WorkAssignment clone) {
		this.id = clone.id;
		this.resourceId = clone.resourceId;
		this.agentId = clone.agentId;
		this.profileId = clone.profileId;
		this.bundleId = clone.bundleId;
		this.serviceName = clone.serviceName;
		this.resourceSelection = clone.resourceSelection;
		this.priority = clone.priority;
		this.script = clone.script;
		this.startTime = clone.startTime;
		this.slaFilename = clone.slaFilename;
		this.background = clone.background;
		this.fork = clone.fork;
		this.status = clone.status;
		this.invokableEP = clone.invokableEP;
		this.timestamp = clone.timestamp;
		this.errorMsg = clone.errorMsg;
	}
	public static String setId(String resourceId, String bundleId, String serviceName){
		return getId(resourceId, bundleId, serviceName);
	}
	public static String getId(String resourceId, String fullBundleName, String serviceName){
		return resourceId + ":" + fullBundleName + ":" + serviceName;
	}
	
	/**
	 * Set resourceId and change the 'id' value
	 * @param resourceId
	 */
	public void setResourceId(String resourceId) {
		this.resourceId = resourceId;
		this.id = getId(resourceId, bundleId, serviceName);
	}
	public String getId() {
		return id;
	}
	public String getScript() {
		return script;
	}
	public void setScript(String script) {
		this.script = script;
	}
	public String getResourceId() {
		return resourceId;
	}
	public String getBundleId() {
		return bundleId;
	}
	
	public String getServiceName() {
		return serviceName;
	}
	
	public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }
    public Map<String,String> getProperies() {
		return new PropertyMap(properties);
	}
    
	public String getProperties() {
        return this.properties;
    }
    public void setProperties(String properties) {
        this.properties = properties;
    }
    public String getWorkingDirectory() {
		return workingDirectory;
	}
	public void setProperty(String property) {
		PropertyMap propertyMap = new PropertyMap(this.properties);
		propertyMap.put(property);
		this.properties = propertyMap.toString();
	}
	public boolean isBackground() {
		return background;
	}
	public void setBackground(boolean background) {
		this.background = background;
	}
	public void setResourceSelection(String resourceSelection) {
		this.resourceSelection = resourceSelection.replaceAll("\n", "");
	}
	public String getResourceSelection() {
		String bundleSelect = " deployedBundles contains " + bundleId;
		if (resourceSelection == null || resourceSelection.trim().length() == 0) return bundleSelect;
		return resourceSelection + " AND " + bundleSelect;
	}
	public void setWorkingDirectory(String workingDirectory) {
		this.workingDirectory = workingDirectory;	
	}
	public int getAllocationsOutstanding() {
		return allocationsOutstanding;
	}
	public void setAllocationsOutstanding(int allocationsOutstanding) {
		this.allocationsOutstanding = allocationsOutstanding;
	}
	public boolean isFork() {
		return fork;
	}
	public void setFork(boolean fork) {
		this.fork = fork;
	}
	public int getPauseSeconds() {
		return pauseSeconds;
	}
	public void setPauseSeconds(int pauseSeconds) {
		this.pauseSeconds = pauseSeconds;
	}
	public void setBundleId(String bundleId) {
		this.bundleId = bundleId;
	}
	public void setPriority(int priority) {
		this.priority = priority;
	}
	public int getPriority() {
		return priority;
	}
	public void setStartTime(long startTimeMs) {
		this.startTimeMs = startTimeMs;
		this.startTime = DateTimeFormat.mediumTime().print(startTimeMs);
	}
	public long getStartTimeMs() {
		return this.startTimeMs;
	}
	public String getStartTime(){
		return this.startTime;
	}
	public String getSlaFilename() {
		return slaFilename;
	}
	public String getOverridesService() {
		if (overridesService == null) overridesService = "";
		return overridesService;
	}
	public void setOverridesService(String overridesService) {
		this.overridesService = overridesService;
	}
	public void setSlaFilename(String slaFilename) {
		this.slaFilename = slaFilename;
	}
	
	public void setAgentId(String agentId) {
		this.agentId = agentId;
	}
	public void setProfileId(int profileId) {
		this.profileId = profileId;
	}
	public String getAgentId() {
		return agentId;
	}
	public int getProfileId() {
		return profileId;
	}
	public void addVariable(String name, String value) {
		PropertyMap propertyMap = new PropertyMap(this.variables);
		propertyMap.put(name, value);
		this.variables = propertyMap.toString();
	}
	public Map<String, String> getVariables(){
		return new PropertyMap(variables);
	}
	public LifeCycle.State getStatus() {
		return LifeCycle.State.valueOf(status);
	}
	public void setStatus(LifeCycle.State status) {
		this.status = status.name();
	}
	/**
	 * @param variables - a=b,c=d etc
	 */
	public void addVariables(String variables) {
		PropertyMap newVariables = new PropertyMap(variables);
		PropertyMap existingVariables = new PropertyMap(this.variables);
		existingVariables.putAll(newVariables);
		this.variables = existingVariables.toString();
	}
	public void setErrorMsg(String errorMsg) {
		this.errorMsg = errorMsg;
	}
	public String getErrorMsg() {
		return errorMsg;
	}
	@Override
	public String toString() {
		return "\t" + getClass().getSimpleName() + "[workId:" + getId() + " resource:" + resourceId + "\tbundle:" + bundleId + "\tservice:" + serviceName + "\tp:" + priority + "\toutStandAllocs:" + allocationsOutstanding +  " " + getStatus() +  " " + timestamp + "]";
	}
	public void setSystemService(boolean systemService) {
		this.systemService = systemService;
	}
	
	public boolean isSystemService() {
		return systemService;
	}
	
	public String getInvokableEndPoint() {
		return this.invokableEP;
	}
	public void setInvokableEndPoint(String url){
		this.invokableEP = url;
	}
	public static String getFullBundleName(String workId) {
		String[] split = workId.split(":");
		return split[1];
	}
	public static String getServiceName(String workId) {
		String[] split = workId.split(":");
		return split[2];
	}
	public void setTimeStamp(String timestamp) {
		this.timestamp = timestamp;
	}
	public void setTimestampMs(long timestampMs) {
		this.timestampMs = timestampMs;
	}
	public long getTimestampMs() {
		return timestampMs;
	}
	public String getTimestamp() {
		return timestamp;
	}
	
	public void setPid(int pid) {
		this.pid = pid;
	}
	public int getPid() {
		return pid;
	}
	public void setCostPerUnit(double costPerUnit) {
		this.costPerUnit = costPerUnit;
	}
	public double getCostPerUnit() {
		return costPerUnit;
	}


    public void decrementAllocationsOutstanding() {
        if (allocationsOutstanding > 0) allocationsOutstanding--;
    }
    public void decrementAllocationsOutstanding(int amount) {
    	allocationsOutstanding -= amount;
    	if (allocationsOutstanding < 0) allocationsOutstanding = 0;
    }

	public void setVariables(HashMap<String, Object> variables2) {
		try {
//			this.variables = "not-set";
			this.variables = variables2.toString();
			this.variables = this.variables.replaceAll(", ", "\n");
			this.variables = this.variables.replaceAll("[ \"']", "");
			
		} catch (Throwable t){
			
		}
	}
	public String getVarString() {
		return variables;
	}
	public WorkAssignment copy() {
		byte[] serialize;
		try {
			serialize = Convertor.serialize(this);
			return (WorkAssignment) Convertor.deserialize(serialize);
		} catch (Exception e) {
			throw new RuntimeException("Failed to clone:" + e, e);
		}
		
	}
	public boolean isSchedulingUserScript() {
		return isBackground() && !isSystemService() && getPauseSeconds() > 0;
	}
    public static WorkAssignment fromService(Service service, String agentId) {
            WorkAssignment workInfo = new WorkAssignment(agentId, agentId + "-0", 0, service.getBundleId(), service.getName(), service.getScript(), service.getPriority());
            workInfo.setProperty(service.getProperty());
            workInfo.setBackground(service.isBackground());
            workInfo.setSystemService(service.isSystem());
            String resourceSelection = service.getResourceSelection();
            if (service.isBackground()) {
                if (resourceSelection.trim().equals("")) {
                    resourceSelection = "workId notContains " + service.getId();
                } else {
                    resourceSelection += " AND workId notContains " + service.getId();
                }
            }
            workInfo.setResourceSelection(resourceSelection);
            workInfo.setPauseSeconds(service.getPauseSeconds());
            workInfo.setFork(service.isFork());
            workInfo.setAllocationsOutstanding(service.getInstanceCountAsInteger());
            workInfo.setSlaFilename(service.getSlaFilename());
            workInfo.setStatus(LifeCycle.State.ASSIGNED);
            workInfo.setTimeStamp(DateTimeFormat.shortDateTime().print(DateTimeUtils.currentTimeMillis()));
            return workInfo;
    }
	
}