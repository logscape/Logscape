package com.liquidlabs.vso.deployment.bundle;

public interface BackgroundServiceAllocatorJMXMBean {

	String listAllocatedTo();

	String listJustAssigned();

	String listPendingWork();
	

}
