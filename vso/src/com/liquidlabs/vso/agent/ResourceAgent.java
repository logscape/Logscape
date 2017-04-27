package com.liquidlabs.vso.agent;

import java.util.List;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.transport.proxy.Remotable;
import com.liquidlabs.vso.deployment.DeploymentListener;
import com.liquidlabs.vso.work.WorkListener;

public interface ResourceAgent extends  LifeCycle, WorkListener, Remotable, DeploymentListener  {
	
	String NAME = ResourceAgent.class.getSimpleName();
	
	String getId();
	void addDeployedBundle(String bundleNameAndVersion, String releaseDate);
	void removeBundle(String bundleName);
	
	// Must be a one-way-call because it exits and cannot send a reply
	void bounce(boolean shouldSleep);
	void systemReboot(long seedTime);
	List<ResourceProfile> getProfiles();
	void go();
	void updateStatus(String workId, LifeCycle.State status, String msg);
	
	void editResourceProperties(String type, String location, String maxHeap);

}
