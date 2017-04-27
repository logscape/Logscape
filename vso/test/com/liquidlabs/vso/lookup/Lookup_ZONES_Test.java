package com.liquidlabs.vso.lookup;

import static org.junit.Assert.*;

import com.liquidlabs.common.NetworkUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.common.jmx.JmxHtmlServerImpl;

import java.util.ArrayList;
import java.util.List;

public class Lookup_ZONES_Test {
	private LookupSpaceImpl lookupSpaceA;
	List<ServiceInfo> infos = new ArrayList<ServiceInfo>();
    private boolean strictMatch = false;

    @Before
	public void setUp() throws Exception {
		ExecutorService.setTestMode();
        final int port = NetworkUtils.determinePort(15000);
        lookupSpaceA = new LookupSpaceImpl(port, port);
		lookupSpaceA.start();
		Thread.sleep(500);


		ServiceInfo serviceInfo1 = new ServiceInfo("myServiceName", "tcp://location1:8000", JmxHtmlServerImpl.locateHttpUrL(), "LDN", "");
		lookupSpaceA.registerService(serviceInfo1, 60);
		infos.add(serviceInfo1);
		
		ServiceInfo serviceInfo3 = new ServiceInfo("myServiceName", "tcp://location2:8000", JmxHtmlServerImpl.locateHttpUrL(), "LDN.UAT", "");
		lookupSpaceA.registerService(serviceInfo3, 60);
		infos.add(serviceInfo3);

        ServiceInfo serviceInfo5 = new ServiceInfo("myServiceName", "tcp://location3:8000", JmxHtmlServerImpl.locateHttpUrL(), "LDN.UAT", "");
        lookupSpaceA.registerService(serviceInfo5, 60);
		infos.add(serviceInfo5);

		ServiceInfo serviceInfo2 = new ServiceInfo("myServiceName", "tcp://location5:8000", JmxHtmlServerImpl.locateHttpUrL(), "LDN.UAT.test", "");
		lookupSpaceA.registerService(serviceInfo2, 60);
		infos.add(serviceInfo2);

        ServiceInfo serviceInfo6 = new ServiceInfo("myServiceName", "tcp://location4:8000", JmxHtmlServerImpl.locateHttpUrL(), "LDN.PROD", "");
        lookupSpaceA.registerService(serviceInfo6, 60);
		infos.add(serviceInfo6);

    }

	@Test
	public void pruneUseCase() throws Exception {
		lookupSpaceA.registerService(new ServiceInfo("prod.Manager", "tcp://lookup:8000", JmxHtmlServerImpl.locateHttpUrL(), "", ""), 60);
		lookupSpaceA.registerService(new ServiceInfo("prod.INDEX.IndexStore", "tcp://indexStore:8000", JmxHtmlServerImpl.locateHttpUrL(), "", ""), 60);
		String clientRole = "prod.INDEX.app1.Forwarder";

		assertEquals(1, lookupSpaceA.getServiceAddresses("prod.INDEX.appl1.IndexStore","", strictMatch).length);
		assertEquals(1, lookupSpaceA.getServiceAddresses("prod.INDEX.appl1.Manager","", strictMatch).length);



	}


	@Test
	public void fallBackToManager() throws Exception {

		// lab[A,B,C] -> lab.abc[A,B,C]
		// lab.  <=  lab.abc
		String[] serviceAddresses = lookupSpaceA.getServiceAddresses("LDN.myServiceName", "", strictMatch);
		assertEquals(1, serviceAddresses.length);
		assertEquals("Should have direct match first", "tcp://location1:8000", serviceAddresses[0]);
	}


	@Test
	public void fallBackToManagerWithMissingService() throws Exception {

		// lab[A,B,C] -> lab.abc[A,B,C]
		// lab.  <=  lab.abc
		String[] serviceAddresses = lookupSpaceA.getServiceAddresses("LDN.test.myServiceName", "", strictMatch);
		assertEquals("Should have direct match first", "tcp://location1:8000", serviceAddresses[0]);
		assertEquals(1, serviceAddresses.length);

	}

	@Test
	public void shouldFallBackToManagerWithMissingService_AND_Zones() throws Exception {

		// lab[A,B,C] -> lab.abc[A,B,C]
		// lab.  <=  lab.abc
		String[] serviceAddresses = lookupSpaceA.getServiceAddresses("LDN.TEST.a.myServiceName", "", strictMatch);
		assertEquals(1, serviceAddresses.length);

	}


	@Test
	public void fallBackToSameZone() throws Exception {
		String[] serviceAddresses = lookupSpaceA.getServiceAddresses("LDN.UAT.myServiceName", "", strictMatch);
		assertEquals(2, serviceAddresses.length);
	}
	@Test
	public void fallBackToSubZoneOnly() throws Exception {
		String[] serviceAddresses = lookupSpaceA.getServiceAddresses("LDN.UAT.test.myServiceName", "", strictMatch);
		assertEquals(1, serviceAddresses.length);

	}



	@After
	public void tearDown() throws Exception {
		lookupSpaceA.stop();
	}
	
}
