package com.liquidlabs.transport.proxy;

import com.liquidlabs.common.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.joda.time.DateTimeUtils;

import junit.framework.TestCase;

import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.TransportFactoryImpl;
import com.liquidlabs.transport.proxy.DummyServiceImpl.UserType;
import com.liquidlabs.transport.proxy.events.Event;
import com.liquidlabs.transport.proxy.events.Event.Type;

public class ProxyRemoteMapPerformanceTest extends TestCase {
	private ProxyFactoryImpl proxyFactoryA;
	boolean enableOutput = false;
	private ProxyFactoryImpl proxyFactoryB;
	private URI proxyBAddress;
	private DummyService remoteService;
	TransportFactory transportFactory = new TransportFactoryImpl(Executors.newFixedThreadPool(5), "test");
	ExecutorService executor = Executors.newFixedThreadPool(5);
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		transportFactory.start();
		
		proxyFactoryA = new ProxyFactoryImpl(transportFactory,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", 11111, "testServiceA"), executor, "");
		proxyFactoryA.start();
		
		Thread.sleep(100);
		
		proxyFactoryB = new ProxyFactoryImpl(transportFactory,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", 22222, "testServiceB"), executor, "");
		proxyFactoryB.registerMethodReceiver("methodReceiver", new DummyServiceImpl());
		proxyFactoryB.start();
		
		Thread.sleep(100);
		proxyBAddress = proxyFactoryB.getAddress();
		DummyServiceImpl.callCount = 0;
		remoteService = proxyFactoryA.getRemoteService("methodReceiver", DummyService.class, new String[] { proxyBAddress.toString() } );
	}
	@Override
	protected void tearDown() throws Exception {
		transportFactory.stop();
		proxyFactoryA.stop();
		proxyFactoryB.stop();
		Thread.sleep(50);
	}
	
	public void testShouldPassAMap() throws Exception {
		HashMap<String, String> map = new HashMap<String, String>();
		map.put("stuff1", "stuff2");
		//Thread.sleep(10 * 1000);
		System.out.println("starting");
		
		long start = DateTimeUtils.currentTimeMillis();
		int limit = 1000;
		for (int i = 0; i < limit; i++) {
			Map mapString = remoteService.passAMap(map);
			if (i % 1000 == 0 && i > 0) {
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
