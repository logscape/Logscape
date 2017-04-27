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

public class ProxyRemoteExceptionFailoverTest {
	private ProxyFactoryImpl proxyFactoryA;
	boolean enableOutput = false;
	private ProxyFactoryImpl proxyFactoryB;
	private URI proxyBAddress;
	private DummyService remoteService;
	TransportFactory transportFactory ;
	ExecutorService executor = Executors.newFixedThreadPool(5);
	Convertor c = new Convertor();
	private ProxyFactoryImpl proxyFactoryC;
	
	@Before
	public void setUp() throws Exception {
		
		transportFactory = new TransportFactoryImpl(Executors.newFixedThreadPool(5), "test");
		proxyFactoryA = new ProxyFactoryImpl(transportFactory,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", 11111, "testServiceA"), executor, "");
		proxyFactoryA.start();
		
		Thread.sleep(100);
		
		TransportFactoryImpl transportFactory2 = new TransportFactoryImpl(Executors.newFixedThreadPool(5), "test");
		proxyFactoryB = new ProxyFactoryImpl(transportFactory2,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", 22222, "testServiceB"), executor, "");
		proxyFactoryB.start();
		
		TransportFactoryImpl transportFactory3 = new TransportFactoryImpl(Executors.newFixedThreadPool(5), "test");
		proxyFactoryC = new ProxyFactoryImpl(transportFactory3,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", 33333, "testServiceB"), executor, "");
		proxyFactoryC.registerMethodReceiver("methodReceiver", new DummyServiceImpl());
		proxyFactoryC.start();
		
		
		Thread.sleep(100);
		proxyBAddress = proxyFactoryB.getAddress();
		DummyServiceImpl.callCount = 0;
		
		// TEST - register BAD and GOOD Endpoint
		remoteService = proxyFactoryA.getRemoteService("methodReceiver", DummyService.class, new String[] { proxyBAddress.toString(), proxyFactoryC.getAddress().toString()  } );
		
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
	public void testExceptionsGetReturnedToCaller() throws Exception {
		
		// Should fail on 2222, retry on 3333
		String fromServer = remoteService.twoWay("from client");
		assertNotNull(fromServer);
	}

}
