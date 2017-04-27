package com.liquidlabs.transport.proxy;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.TransportFactoryImpl;
import com.liquidlabs.transport.serialization.Convertor;

public class ProxyRemoteInvocationCachingTest {
	private ProxyFactoryImpl proxyFactoryA;
	boolean enableOutput = false;
	private ProxyFactoryImpl proxyFactoryB;
	private URI proxyBAddress;
	private DummyService remoteService;
	TransportFactory transportFactory ;
	ExecutorService executor = Executors.newFixedThreadPool(5);
	Convertor c = new Convertor();
	
	@Before
	public void setUp() throws Exception {
		
		transportFactory = new TransportFactoryImpl(Executors.newFixedThreadPool(5), "test");
		proxyFactoryA = new ProxyFactoryImpl(transportFactory,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", 11111, "testServiceA"), executor, "");
		proxyFactoryA.start();
		
		Thread.sleep(100);
		
		TransportFactoryImpl transportFactory2 = new TransportFactoryImpl(Executors.newFixedThreadPool(5), "test");
		proxyFactoryB = new ProxyFactoryImpl(transportFactory2,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", 22222, "testServiceB"), executor, "");
		proxyFactoryB.registerMethodReceiver("methodReceiver", new DummyServiceImpl());
		proxyFactoryB.start();
		
		Thread.sleep(100);
		proxyBAddress = proxyFactoryB.getAddress();
		DummyServiceImpl.callCount = 0;
		remoteService = proxyFactoryA.getRemoteService("methodReceiver", DummyService.class, new String[] { proxyBAddress.toString() } );
		
		DummyServiceImpl.callCount = 0;
		System.out.println("********************************* "+ "TEST" + " ***************************");
	}
	@After
	public void tearDown() throws Exception {
		transportFactory.stop();
		proxyFactoryA.stop();
		proxyFactoryB.stop();
		Thread.sleep(50);
	}
	
	@Test
	public void testZeroArgs() throws Exception {
		String result1 = remoteService.twoWayCached();
		String result2 = remoteService.twoWayCached();
		Thread.sleep(2000);
		String result3 = remoteService.twoWayCached();
		
		assertEquals(result1, result2);
		assertFalse(result2.equals(result3));
		assertEquals(2, DummyServiceImpl.callCount);	
	}
	
	@Test
	public void testTwoWayWithDiffArgs() throws Exception {
		String result1 = remoteService.twoWayCached("one");
		String result2 = remoteService.twoWayCached("two");
		String result3 = remoteService.twoWayCached("three");
		
		assertFalse(result1.equals(result2));
		assertFalse(result2.equals(result3));
		assertEquals(3, DummyServiceImpl.callCount);		
	}
	
	@Test
	public void testTwoWayWithArgs() throws Exception {
		String result1 = remoteService.twoWayCached("one");
		String result2 = remoteService.twoWayCached("one");
		Thread.sleep(2000);
		String result3 = remoteService.twoWayCached("one");
		
		assertEquals(result1, result2);
		assertFalse(result2.equals(result3));
		assertEquals(2, DummyServiceImpl.callCount);		
	}
	@Test
	public void testTwoWayWithNullArgs() throws Exception {
		String result1 = remoteService.twoWayCached(null);
		String result2 = remoteService.twoWayCached(null);
		Thread.sleep(2000);
		String result3 = remoteService.twoWayCached(null);
		
		assertEquals(result1, result2);
		assertFalse(result2.equals(result3));
		assertEquals(2, DummyServiceImpl.callCount);		
	}

}
