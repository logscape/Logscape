package com.liquidlabs.vso.deployment.bundle;

public interface ServiceFinder {

	public abstract boolean isServiceRunning(String serviceId);

	public abstract boolean isWaitingForDependencies(String serviceName,
			String bundleName);

}