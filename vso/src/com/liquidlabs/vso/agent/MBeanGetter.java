package com.liquidlabs.vso.agent;

import java.io.File;
import java.lang.management.*;

public class MBeanGetter {

	MyOperatingSystemMXBean os = new MyOperatingSystemMXBean();
	MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
	RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
	private long lastNanoTime =-1;
	private long lastCpuTime = -1;
	private static int coreCount;

	public MBeanGetter() {
	}

	public static int getCoreCount() {
		return new MyOperatingSystemMXBean().getAvailableProcessors();
	}

	public int getAvailableProcessors() {
		return os.getAvailableProcessors();
	}
	
	public long getHeapMemoryUsage(){
		MemoryUsage heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();
		return (heapMemoryUsage.getUsed()/1024)/1024;
	}
	
	public long getHeapMemoryMax(){
		MemoryUsage heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();
		return (heapMemoryUsage.getMax()/1024)/1024;
	}
	public long getHeapMemoryCommitted(){
		MemoryUsage heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();
		return (heapMemoryUsage.getCommitted()/1024)/1024;
	}
	public long getHeapMemoryAvailable(){
		MemoryUsage heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();
		return ((heapMemoryUsage.getMax() - heapMemoryUsage.getUsed())/1024)/1024;
	}
	public int getPid() {
		return Integer.parseInt(runtimeMXBean.getName().split("@")[0]);
	}
	public int getSwapTotalMb() {
		return (int) (os.getTotalSwapSpaceSize()/(1024 * 1024));
	}
	public int getSwapFreeMb() {
		return (int) (os.getFreeSwapSpaceSize()/(1024 * 1024));
	}
	public int getPhysicalMemFreeMb() {
		return (int) (os.getFreePhysicalMemorySize()/(1024 * 1024));
//		return os.getFreePhysicalMemorySize();
	}
	public int getPhysicalMemTotalMb() {

		return (int) (os.getTotalPhysicalMemorySize()/(1024l * 1024l));
	}
	public int getVMCommitedMb() {
		return (int) (os.getCommittedVirtualMemorySize()/(1024 * 1024));
	}
	
	public String getName() {
		return os.getName();
	}
	
	public int getDiskTotalSpaceMb() {
		return (int) (new File(".").getTotalSpace()/ (1024 * 1024));
	}
	public int getDiskUsableSpaceMb() {
		return (int) (new File(".").getUsableSpace() / (1024 * 1024));
	}
	public int getDiskFreeSpaceMb() {
		return (int) (new File(".").getFreeSpace() / (1024 * 1024));
	}
	
	public static void main(String[] args) {
			MBeanGetter beanGetter = new MBeanGetter();
			System.out.println("Name:"+ beanGetter.getName());
			System.out.println("JVM:" + beanGetter.getJvm());
			System.out.println("proc:" + beanGetter.getAvailableProcessors());
			System.out.println("diskFree:" +beanGetter.getDiskFreeSpaceMb());
			System.out.println("diskTotal:" +beanGetter.getDiskTotalSpaceMb());
			System.out.println("DiskUsable:" + beanGetter.getDiskUsableSpaceMb());
			System.out.println(beanGetter.getHeapMemoryAvailable());
			System.out.println(beanGetter.getHeapMemoryCommitted());
			System.out.println(beanGetter.getHeapMemoryMax());
			System.out.println(beanGetter.getHeapMemoryUsage());
			System.out.println("physicalFree:" + beanGetter.getPhysicalMemFreeMb());
			System.out.println("physicalTotal:" + beanGetter.getPhysicalMemTotalMb());
			System.out.println(beanGetter.getPid());
			System.out.println(beanGetter.getSwapFreeMb());
			System.out.println(beanGetter.getSwapTotalMb());
			System.out.println(beanGetter.getVMCommitedMb());
			System.out.println("sysloadAv:" + beanGetter.getSystemLoadAverage());
	}

	private double getSystemLoadAverage() {
		return os.getSystemLoadAverage();
	}

	public String getJvm() {
		return runtimeMXBean.getVmName() + " [" + runtimeMXBean.getVmVendor() + "] " + runtimeMXBean.getVmVersion();
		
	}

	public double getCpuTime() {
		os.refresh();
		double result = 0;
		long now = System.nanoTime();
		long currentTime = os.getProcessCpuTime();
		if(lastNanoTime != -1) {
			long spent = currentTime - lastCpuTime;
			long nanos = now - lastNanoTime;
			result = spent * 100.0 / nanos;
		}


		
		return result;
	}
}
