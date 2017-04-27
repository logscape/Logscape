package com.liquidlabs.vso.agent;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

/**
 * Created by neil on 11/08/2015.
 */
public class MyOperatingSystemMXBean {
    OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();

    static {
        refresh();
    }

    public static void refresh() {
        try  {
            Class<?> aClass = Class.forName(System.getProperty("os.mbean", "com.sun.management.OperatingSystemMXBean"));
            OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
            if (operatingSystemMXBean instanceof com.sun.management.OperatingSystemMXBean) {
                com.sun.management.OperatingSystemMXBean oss = (com.sun.management.OperatingSystemMXBean) operatingSystemMXBean;
                freeSwapSpaceSize =  oss.getFreePhysicalMemorySize();
                freePhysicalMemorySize = oss.getFreePhysicalMemorySize();
                totalSwapSpaceSize =  oss.getTotalSwapSpaceSize();
                totalPhysicalMemorySize =  oss.getTotalPhysicalMemorySize();
                committedVirtualMemorySize =  oss.getCommittedVirtualMemorySize();
                processCpuTime = oss.getProcessCpuTime();
                systemCpuLoad = oss.getSystemCpuLoad();
            }

        }  catch (final Exception e) {
            e.printStackTrace();
        }
    }

    static private long freeSwapSpaceSize;
    static private long freePhysicalMemorySize;
    static private long totalSwapSpaceSize;
    static private long totalPhysicalMemorySize;
    static private long committedVirtualMemorySize;
    static private long processCpuTime;
    static private double systemCpuLoad;

    public int getAvailableProcessors() {
        return os.getAvailableProcessors();
    }

    public String getName() {
        return os.getName();
    }

    public static double getSystemCpuLoad() {
        return systemCpuLoad;
    }

    public double getSystemLoadAverage() {
        return os.getSystemLoadAverage();
    }

    public long getTotalSwapSpaceSize() {
        return totalSwapSpaceSize;
    }

    public long getFreeSwapSpaceSize() {
        return freeSwapSpaceSize;
    }

    public long getFreePhysicalMemorySize() {
        return freePhysicalMemorySize;
    }

    public long getTotalPhysicalMemorySize() {
        return totalPhysicalMemorySize;
    }

    public long getCommittedVirtualMemorySize() {
        return committedVirtualMemorySize;
    }

    public long getProcessCpuTime() {
        return processCpuTime;
    }
}
