package com.liquidlabs.transport.proxy;

import com.liquidlabs.common.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import junit.framework.TestCase;

import com.liquidlabs.transport.Config;
import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.TransportFactoryImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class MultiInvocationTest  {
	private ProxyFactoryImpl proxyFactoryA;
	boolean enableOutput = false;
	private ProxyFactoryImpl proxyFactoryB;
	private URI proxyBAddress;
	private DummyService remoteService;
	
	long callCount;
	private ExecutorService executor;
	private TransportFactory transportFactory;
	private DummyServiceImpl dummyService;

	@Before
	public void setUp() throws Exception {
		executor = Executors.newFixedThreadPool(5);
		transportFactory = new TransportFactoryImpl(executor, "test");
		proxyFactoryA = new ProxyFactoryImpl(transportFactory,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", Config.TEST_PORT, "multiITestA"), executor, "");
		proxyFactoryA.start();
		
		Thread.sleep(100);
		
		proxyFactoryB = new ProxyFactoryImpl(transportFactory,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", Config.TEST_PORT+10000, "multiITestB"), executor, "");

		dummyService = new DummyServiceImpl();
		proxyFactoryB.registerMethodReceiver("methodReceiver", dummyService);
		proxyFactoryB.start();
		proxyBAddress = proxyFactoryB.getAddress();
		DummyServiceImpl.callCount = 0;
		remoteService = proxyFactoryA.getRemoteService("methodReceiver", DummyService.class, new String[] { proxyBAddress.toString() });
	}
	@After
	public void tearDown() throws Exception {
		proxyFactoryA.stop();
		proxyFactoryB.stop();
		dummyService.stop();
		transportFactory.stop();
	}


	synchronized public void  incrementCallCount(){
		callCount++;
	}

	@Test
	public void testMultiInvocationIsHandledSafely() throws Exception {
		
		Thread firstInvocationThread = new Thread(){
			@Override
			public void run() {
				String result = remoteService.twoWayWithPause("doFirst", 2000l);
				if (result != null && result.equals("doFirst")) incrementCallCount();
				else System.err.println("Did NOT get valid result, received:" + result);
			}
		};
		
		// start the first long running request that takes 5 seconds
		firstInvocationThread.start();
		
		// make the same call but with quicker result
		String secondResult = remoteService.twoWayWithPause("doSecond", 100l);
		if (secondResult != null && secondResult.equals("doSecond")) incrementCallCount();
		firstInvocationThread.join();
		
		assertNotNull(secondResult);
		assertEquals("doSecond", secondResult);
		assertEquals("callCount should have been 2 but was:" + callCount, callCount, 2);		
	}

}
