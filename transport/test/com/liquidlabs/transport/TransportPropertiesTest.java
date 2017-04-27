package com.liquidlabs.transport;

import static org.junit.Assert.*;
import junit.framework.Assert;

import org.junit.Test;

public class TransportPropertiesTest {
	
	
	@Test
	public void shouldDoGoodPortScanning() throws Exception {
		
		if (true) return;
		
		int basePort = 62000;
		int portRange = 1000;
		for (int i = 0; i < 10 * 1000; i++) {
			int portBeingUsed = TransportProperties.getClientBasePort();
			TransportProperties.updateBasePort(portBeingUsed +1);
//			if (portBeingUsed % 5 == 0) System.out.println("Checking:" + portBeingUsed);
			Assert.assertTrue(portBeingUsed >= basePort);
			Assert.assertTrue(portBeingUsed <= basePort + portRange);
		}
		
	}

}
