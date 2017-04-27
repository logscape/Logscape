package com.liquidlabs.vso.lookup;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.transport.TransportFactory;
import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.liquidlabs.common.jmx.JmxHtmlServerImpl;
import com.liquidlabs.orm.ORMapperClient;
import com.liquidlabs.transport.proxy.ProxyFactoryImpl;

public class LookupFunctionalTest {
	String HOST = NetworkUtils.getIPAddress();
	private LookupSpaceImpl lookupSpaceA;
	private String location = "";
	private String agentType = "Management";
    private int port;

    @Before
	public void setUp() throws Exception {
		System.setProperty("transport", TransportFactory.TRANSPORT.RABBIT.name());

		com.liquidlabs.common.concurrent.ExecutorService.setTestMode();
        port = NetworkUtils.determinePort(15000);
        lookupSpaceA = new LookupSpaceImpl(port, port);
		lookupSpaceA.start();
		Thread.sleep(3 * 1000);
	}
	
	@After
	public void tearDown() throws Exception {
		lookupSpaceA.stop();
	}
	
	@Test
	public void testLookitUpLots() throws Exception {
		ServiceInfo serviceInfo = new ServiceInfo("myServiceName", "location", JmxHtmlServerImpl.locateHttpUrL(), location, agentType );
		lookupSpaceA.registerService(serviceInfo, 60);
		
		int limit = 10;
		for (int i = 0; i < limit; i++) {
			
			ProxyFactoryImpl pf = new ProxyFactoryImpl(18000, Executors.newCachedThreadPool(), "LookupFunctionTest");
			pf.start();
			
			LookupSpace remoteService = LookupSpaceImpl.getRemoteService("stcp://"  + HOST + ":" + port, pf, true, "ctx");
			List<ServiceInfo> findService = remoteService.findService("");
			System.out.println(i + " services:" + findService);
			
			pf.stop();
		}
	}
	
	@Test
	public void testRegisterAndRenewAndThereIsCorrectNumberOfKeys() throws Exception {
		ServiceInfo serviceInfo = new ServiceInfo("myServiceName", "location", JmxHtmlServerImpl.locateHttpUrL(), location, agentType);
		String registerService = lookupSpaceA.registerService(serviceInfo, 60);
		
	}

	@Test
	public void testShouldNotHaveMultipleRegistrationsRegisterAndLookupInSameInstance() throws Exception {
		ORMapperClient mapperClient = lookupSpaceA.mapperFactory.getORMapperClient();
		Set<String> keySet = mapperClient.keySet();
		Set<String> leaseKeySet = mapperClient.leaseKeySet();
		String lease1 = lookupSpaceA.registerService(new ServiceInfo("myServiceName", "location", "http://" + HOST, location, agentType), -1);
		String lease2 = lookupSpaceA.registerService(new ServiceInfo("myServiceName", "location", "http://" + HOST, location, agentType), -1);
		String lease3 = lookupSpaceA.registerService(new ServiceInfo("myServiceName", "location", "http://" + HOST, location, agentType), -1);
				Set < String > keySet2 = mapperClient.keySet();
		Set<String> leaseKeySet2 = mapperClient.keySet();
		
		String[] serviceAddresses = lookupSpaceA.getServiceAddresses("myServiceName", location, false);
		Assert.assertEquals(1, serviceAddresses.length);
		
		Assert.assertEquals(keySet.toString(), keySet2.toString());
		Assert.assertEquals(leaseKeySet.toString(), leaseKeySet2.toString());
	}
}
