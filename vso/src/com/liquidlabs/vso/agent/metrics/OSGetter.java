package com.liquidlabs.vso.agent.metrics;


public interface OSGetter {

	int getCPULoadPercentage();

	int getTotalCPUCoreCount();

	String getCPUModel();

	String getSubnetMask();

	String getDomain();
	
	String getGateway();

	String getCPUSpeed();

	int getTotalMemoryMb();

	int getAvailMemMb();
	
}
