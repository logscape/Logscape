package com.liquidlabs.vso.agent;

import java.util.concurrent.Executors;

import com.liquidlabs.vso.agent.metrics.WindowsOSGetter;

import junit.framework.TestCase;

public class WindowsOSGetterTest extends TestCase {
	
	
	private WindowsOSGetter getter;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		getter = new WindowsOSGetter(Executors.newScheduledThreadPool(1));
	}
	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
	}
	
	public void testCPUUtilisationStuff() throws Exception {
		System.out.println("CPU:" + getter.getCPULoadPercentage());
		System.out.println("maxMem:" + getter.getTotalMemoryMb());
		System.out.println("Model:" + getter.getCPUModel());
		System.out.println("Speed:" + getter.getCPUSpeed());
		System.out.println("Domain:" + getter.getDomain());
		System.out.println("Subnet:" + getter.getSubnetMask());
		System.out.println("CoreCount:" + getter.getTotalCPUCoreCount());
	}
	
	public void testShouldGetNewCPUMethod() throws Exception {
		Thread.sleep(1000);
		double loadPercentage = 0.0;
		for (int i = 0; i < 1000; i++){
			double newLPercentage = getter.getCPULoadPercentage();
			if (newLPercentage != loadPercentage) {
				System.err.println(loadPercentage);
			}
			loadPercentage = newLPercentage;
			int j = 0;
			while (j++ < 10000){
				double f =Math.random() * 100.11 * 12.33 * Math.random() * 12.33333333333 * 566666666666.66666;
			}
		}
		
	}

}
