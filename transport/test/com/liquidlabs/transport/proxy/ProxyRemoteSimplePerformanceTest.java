package com.liquidlabs.transport.proxy;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.TransportFactoryImpl;

public class ProxyRemoteSimplePerformanceTest {
	private ProxyFactoryImpl proxyFactoryA;
	boolean enableOutput = false;
	private ProxyFactoryImpl proxyFactoryB;
	private URI proxyBAddress;
	private DummyService remoteService;
	TransportFactory transportFactory = new TransportFactoryImpl(Executors.newFixedThreadPool(5), "test");
	ExecutorService executor = Executors.newFixedThreadPool(5);
	
	@Before
	public void setUp() throws Exception {
		transportFactory.start();
		
		proxyFactoryA = new ProxyFactoryImpl(transportFactory,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", 11111, "testService"), executor, "");
		proxyFactoryA.start();
		
		Thread.sleep(100);
		
		DummyServiceImpl.verbose = false;
		
		proxyFactoryB = new ProxyFactoryImpl(transportFactory,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", 22222, "testService"), executor, "");
		proxyFactoryB.registerMethodReceiver("methodReceiver", new DummyServiceImpl());
		proxyFactoryB.start();
		
		Thread.sleep(100);
		proxyBAddress = proxyFactoryB.getAddress();
		DummyServiceImpl.callCount = 0;
		remoteService = proxyFactoryA.getRemoteService("methodReceiver", DummyService.class, new String[] { proxyBAddress.toString() } );
	}
	@After
	public void tearDown() throws Exception {
		transportFactory.stop();
		proxyFactoryA.stop();
		proxyFactoryB.stop();
		Thread.sleep(50);
	}
	
	@Test
	public void testShouldPassTwoString() throws Exception {
		HashMap<String, String> map = new HashMap<String, String>();
		map.put("stuff1", "stuff2");
		//Thread.sleep(10 * 1000);
		System.out.println("starting");
		
		DummyServiceImpl.verbose = false;
		long start = DateTimeUtils.currentTimeMillis();
		int limit = 100;//00;
		for (int i = 0; i < limit; i++) {
			String msg = "crap" + i;
			String mapString = remoteService.twoWay(msg);
			Assert.assertTrue(mapString.contains(msg));
		//	Map mapString2 = remoteService.passAMap(map);
			if (i % 10000 == 0 && i > 0) {
				long now = DateTimeUtils.currentTimeMillis();
				double secs = (now - start)/1000.0;
				System.out.println(i + " throughput:" + (double)i/secs + "/sec");
			}
		}
		long end = DateTimeUtils.currentTimeMillis();
		System.out.println("Elapsed:" + (end - start));
//		assertTrue(mapString.contains("stuff1"));
//		assertTrue(mapString.contains("stuff2"));
		
	}
	

}
