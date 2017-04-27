package com.liquidlabs.vso.lookup;

import com.liquidlabs.common.NetworkUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class Lookup_DOT_ZONES_Test {
	private LookupSpaceImpl lookupSpaceA;

	@Before
	public void setUp() throws Exception {
		com.liquidlabs.common.concurrent.ExecutorService.setTestMode();
        final int port = NetworkUtils.determinePort(15000);
        lookupSpaceA = new LookupSpaceImpl(port, port);
		lookupSpaceA.start();
		Thread.sleep(500);
		ServiceInfo serviceInfo4 = new ServiceInfo("ldn.myServiceName", "tcp://locationFO:8000", "", "", "Failover");
		lookupSpaceA.registerService(serviceInfo4, 60);
		
		ServiceInfo serviceInfo2 = new ServiceInfo("ldn.uat.myServiceName", "tcp://location2:8000", "", "", "");
		lookupSpaceA.registerService(serviceInfo2, 60);
		
		ServiceInfo serviceInfo1 = new ServiceInfo("ldn.myServiceName", "tcp://location1:8000", "", "", "");
		lookupSpaceA.registerService(serviceInfo1, 60);
		
		ServiceInfo serviceInfo3 = new ServiceInfo("ldn.uat.dc1.myServiceName", "tcp://location3:8000", "", "", "");
		lookupSpaceA.registerService(serviceInfo3, 60);

        ServiceInfo serviceInfo5 = new ServiceInfo("ldn.uat.dc3.myServiceName", "tcp://location5:8000", "", "", "");
        lookupSpaceA.registerService(serviceInfo5, 60);

        ServiceInfo serviceInfo6 = new ServiceInfo("ldn.uat.dc3.myServiceName", "tcp://location6:8000", "", "", "");
        lookupSpaceA.registerService(serviceInfo6, 60);

        ServiceInfo serviceInfo7 = new ServiceInfo("ldn.uat.dc3.solo.myServiceName", "tcp://location6:8000", "", "", "");
        lookupSpaceA.registerService(serviceInfo7, 60);


    }
	
	@After
	public void tearDown() throws Exception {
		lookupSpaceA.stop();
	}
	
	@Test
	public void shouldMatchParts() throws Exception {
		assertTrue(lookupSpaceA.isMatch("LDN", "LDN", false));
		assertTrue(lookupSpaceA.isMatch("LDN", "LDN", true));
		assertTrue(lookupSpaceA.isMatch("com.lon.dc1", "com.lon", false));
		assertTrue(lookupSpaceA.isMatch("com.lon.dc1", "com.lon.dc1", false));
		assertFalse(lookupSpaceA.isMatch("com.lon.dc1", "com.lon.dc2", false));
		assertFalse(lookupSpaceA.isMatch("com.lon", "com.lon.dc1", false));
	}
	
	@Test
	public void shouldLookupWithPreferredLocationFirst() throws Exception {
		
		// direct match found
		String[] serviceAddresses = lookupSpaceA.getServiceAddresses("ldn.uat.dc1.myServiceName", "", false);
		assertEquals(1, serviceAddresses.length);
		assertEquals("Should have direct match first", "tcp://location3:8000", serviceAddresses[0]);
	}

    @Test
	public void shouldFallback() throws Exception {
		String[] serviceAddresses = lookupSpaceA.getServiceAddresses("ldn.uat.dc2.myServiceName", "", false);
		assertEquals(1, serviceAddresses.length);
		assertEquals("tcp://location2:8000", serviceAddresses[0]);

	}
	
}
