package com.liquidlabs.transport.proxy;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.PIDGetter;
import com.liquidlabs.common.concurrent.NamingThreadFactory;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.TransportFactoryImpl;
import com.liquidlabs.transport.serialization.Convertor;

public class ProxyRemoteOneWayTest {
	private ProxyFactoryImpl proxyFactoryA;
	boolean enableOutput = false;
	private ProxyFactoryImpl proxyFactoryB;
	private URI proxyBAddress;
	private DummyService remoteService;
	TransportFactory transportFactory ;
	ExecutorService executor = Executors.newFixedThreadPool(5);
	Convertor c = new Convertor();
	private TransportFactoryImpl transportFactory2;
	
	@Before
	public void setUp() throws Exception {
		
		transportFactory = new TransportFactoryImpl(Executors.newFixedThreadPool(5), "test");
		proxyFactoryA = new ProxyFactoryImpl(transportFactory,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", NetworkUtils.determinePort(11111), "testServiceA"), executor, "");
		proxyFactoryA.start();
		
		Thread.sleep(100);
		
		transportFactory2 = new TransportFactoryImpl(Executors.newFixedThreadPool(5), "test");
		proxyFactoryB = new ProxyFactoryImpl(transportFactory2,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", NetworkUtils.determinePort(22222), "testServiceB"), executor, "");
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
		String pid = PIDGetter.getPID();
		System.out.println("PID::::::::::" + pid);
		
		transportFactory.stop();
		transportFactory2.stop();
		proxyFactoryA.stop();
		proxyFactoryB.stop();
		
		
		Thread.sleep(50);
	}
	
	@Test
	public void shouldNotWaitForAReplyOnOneWayCall() throws Exception {
		
		// prime the sockets
		remoteService.twoWay("");
		DummyServiceImpl.callCount = 0;
		
		System.out.println("Calling >>>");
		DummyServiceImpl.verbose = true;
		remoteService.oneWay("doStuff");
		System.out.println("Called <<<");
		// should have returned before the sleep'd call increments the counter to 1
		assertEquals(0, DummyServiceImpl.callCount);
		Thread.sleep(1200);
		assertEquals(1, DummyServiceImpl.callCount);
	}
}
