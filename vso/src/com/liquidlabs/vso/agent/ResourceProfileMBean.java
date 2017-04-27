package com.liquidlabs.vso.agent;


public interface ResourceProfileMBean {

	long getMemoryCommitted();

	long getMemoryUsed();

	long getMemoryAvailable();

	long getMemoryMax();

	String getHostName();

	int getPort();

	String getResourceId();

	long getStartTimeSecs();

	String getLastUsed();

	int getCurrentTemp();

	int getCoreCount();

	int getMflops();

	String getZone();

	String getDomain();

	String getSubnet();

	String getEndPoint();

	String getOsName();

	String getOsArchitecture();

	String getOsVersion();

	String getCpuModel();

	int getCpuUtilisation();

	double getSysMemUtil();

	double getDiskUtil();

	double getSwapUtil();

	int getCurrentPowerDraw();

	int getMaxPowerDraw();

	int getCpuCount();

	String getSubnetMask();

	String getOwnership();

	String getCwd();

	String getWorkId();

	String getActiveServiceName();

	String getActiveBundleId();

	String getJmxHttpURL();

	String getBootHash();

	int getSystemId();

	String getSystemStats(String tag, String id);

	String getIpAddress();

	String getGateway();

	int getPid();

	String getLastUpdated();

	int getDiskFreeSpaceMb();

	double getCpuTime();

	String getStartTime();

}
