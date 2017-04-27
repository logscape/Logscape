package com.liquidlabs.vso.deployment.bundle;

import com.liquidlabs.vso.work.WorkAssignment;


public interface BundleHandler {

	void deployBundle(String bundleDescriptor, String bundleId,String workingDirectory);

	Bundle loadBundle(String string);

	void install(Bundle bundle) throws Exception;

	void deploy(String bundleId, String workingDirectory);

	void undeployBundle(String bundleId);

	WorkAssignment getWorkAssignmentForService(String serviceId);

}
