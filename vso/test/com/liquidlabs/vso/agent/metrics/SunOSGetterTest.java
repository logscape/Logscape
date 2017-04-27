package com.liquidlabs.vso.agent.metrics;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.liquidlabs.vso.agent.metrics.SunOSGetter.CPUGetter;

public class SunOSGetterTest {
	
	@Before
	public void setup() {
	}
	
	@Test
	public void testShouldGetCPU() throws Exception {
		String[] lines = new String[] {
					"     cpu",
			 "us sy wt id",
			  "4  1  0 95"
		};
		
		int cpu = 0;
		CPUGetter cpuGetter = new SunOSGetter.CPUGetter();
		for (String string : lines) {
			int v = cpuGetter.parseCpuPercent(string);
			 if (v > 0) cpu = v;
		}
		Assert.assertEquals(5, cpu);
	}

}
