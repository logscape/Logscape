package com.liquidlabs.vso.agent;

import junit.framework.TestCase;

public class MBeanGetterTest extends TestCase {
	
	
	public void testMBeanShouldDoIt() throws Exception {
		MBeanGetter beanGetter = new MBeanGetter();
		System.out.println(beanGetter.getAvailableProcessors());
		System.out.println(beanGetter.getDiskFreeSpaceMb());
		System.out.println(beanGetter.getDiskTotalSpaceMb());
		System.out.println(beanGetter.getDiskUsableSpaceMb());
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
		
	}
	
	public void testShouldGetPid() throws Exception {
		assertTrue(0 < new MBeanGetter().getPid());
	}
	public void testShouldGetMemValues() throws Exception {
		
		MBeanGetter beanGetter = new MBeanGetter();
		long mem = beanGetter.getHeapMemoryMax();
		assertTrue(mem > 0);
		
	}
	public void testShouldPostUsedMemValues() throws Exception {
		
		MBeanGetter beanGetter = new MBeanGetter();
		long mem = beanGetter.getHeapMemoryAvailable();
		assertTrue(mem > 0);
		
	}

}
