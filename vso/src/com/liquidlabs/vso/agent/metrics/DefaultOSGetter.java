package com.liquidlabs.vso.agent.metrics;

import com.liquidlabs.vso.agent.MyOperatingSystemMXBean;

public class DefaultOSGetter implements OSGetter {

	MyOperatingSystemMXBean os = new MyOperatingSystemMXBean();

	public int getCPULoadPercentage() {
		return 0;
	}

	public String getCPUModel() {
		return "unknown";
	}

	public String getCPUSpeed() {
		return "unknown";
	}
	
	public int getTotalMemoryMb() {
		return (int) (os.getTotalPhysicalMemorySize()/(1024 * 1024));
	}
	public int getAvailMemMb() {
		return (int) (os.getFreePhysicalMemorySize()/(1024 * 1024));
	}

	public String getDomain() {
		return "unknown";
	}

	public String getGateway() {
		return "unknown";
	}

	public String getSubnetMask() {
		return "unknown";
	}

	public int getTotalCPUCoreCount() {
		return 2;
	}
    public static boolean isA() {
        return System.getProperty("os.name").toUpperCase().contains("WIN");
    }

}
