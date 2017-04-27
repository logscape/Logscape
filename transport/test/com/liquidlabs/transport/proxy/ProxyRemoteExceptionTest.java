package com.liquidlabs.transport.proxy;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.TransportFactoryImpl;
import com.liquidlabs.transport.serialization.Convertor;

public class ProxyRemoteExceptionTest {
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
	}
	@After
	public void tearDown() throws Exception {
		transportFactory.stop();
		proxyFactoryA.stop();
		proxyFactoryB.stop();
		Thread.sleep(50);
	}
	
	@Test
	public void testExceptionsGetReturnedToCaller() throws Exception {
		
		try {
			remoteService.doThrowException();
			fail("should have blown up");
		} catch (RuntimeException e) {
			assertNotNull(e);
			String message = e.getMessage();
			assertNotNull(e.toString(), message);
		}
	}


	@Test
	public void testInvokeOneWayWithExceptionSig() throws Exception {
		remoteService.oneWayWithEx("doStuff");
		Thread.sleep(100);
		assertTrue(DummyServiceImpl.callCount == 1);
	}
	
	@Test
	public void shouldDisableOnDisconnectedException() throws Exception {
		
	}

}
