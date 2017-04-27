package com.liquidlabs.vso.deployment;

public interface DeploymentListener {
	public void successfullyDeployed(String bundleName, String hash);
	public void errorDeploying(String bundleName, String hash, String errorMessage);
	public void unDeployed(String bundleName, String hash);
	public String getId();
}
