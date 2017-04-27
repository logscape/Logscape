package com.liquidlabs.vso.agent.metrics;

import com.liquidlabs.vso.agent.metrics.LinuxOSGetter.CPUGetter;

import junit.framework.TestCase;

public class LinuxOSGetterTest extends TestCase {
	
	
	private LinuxOSGetter osGetter;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		osGetter = new LinuxOSGetter();
	}
	
	public void testCPUFormattingWorks() throws Exception {
		CPUGetter getter = new LinuxOSGetter.CPUGetter();
		int cpuPercent = getter.parseCpuPercent("2.33 0.08 0.85 0.02 96.72");
		assertEquals(3, cpuPercent);
		
	}

}
