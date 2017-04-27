package com.liquidlabs.vso.agent;

import com.liquidlabs.vso.agent.metrics.OSXOSGetter;
import com.liquidlabs.vso.agent.metrics.OSXOSGetter.CPUGetter;

import junit.framework.TestCase;

public class OSXOSGetterTest extends TestCase {
	
	private OSXOSGetter getter;

	@Override
	protected void setUp() throws Exception {
		getter = new OSXOSGetter();
	}
	
	public void testCPULineIsParsedOk() throws Exception {
		String cpuLine = "9  7 84  0.86 1.58 1.74";
		CPUGetter getter  = new OSXOSGetter.CPUGetter();
		
		int parseCpuPercent = getter.parseCpuPercent(cpuLine);
		assertEquals(16, parseCpuPercent);
		
	}
	
	public void testCPUUtilisationStuff() throws Exception {
		System.out.println("Gateway:" + getter.getGateway());
		System.out.println("CPU:" + getter.getCPULoadPercentage());
		System.out.println("Model:" + getter.getCPUModel());
		System.out.println("Speed:" + getter.getCPUSpeed());
		System.out.println("Domain:" + getter.getDomain());
		System.out.println("Subnet:" + getter.getSubnetMask());
		System.out.println("CoreCount:" + getter.getTotalCPUCoreCount());
	}
	

}
