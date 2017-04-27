package com.liquidlabs.vso.deployment;

import com.liquidlabs.common.LifeCycle;

public interface DeploymentService extends LifeCycle {

	public static final String NAME = DeploymentService.class.getSimpleName();
	
	void deployAllBundles();
	void deploy(String bundleName, String hash, boolean isSystem);
	void undeploy(String bundleName, String hash) throws Exception;
	void addDeploymentListener(DeploymentListener listener);
	String getFailure(String bundleId);
}