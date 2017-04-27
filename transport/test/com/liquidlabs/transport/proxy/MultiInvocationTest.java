package com.liquidlabs.transport.proxy;

import com.liquidlabs.common.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import junit.framework.TestCase;

import com.liquidlabs.transport.Config;
import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.TransportFactoryImpl;

public class MultiInvocationTest extends TestCase {
	private ProxyFactoryImpl proxyFactoryA;
	boolean enableOutput = false;
	private ProxyFactoryImpl proxyFactoryB;
	private URI proxyBAddress;
	private DummyService remoteService;
	
	long callCount;
	private ExecutorService executor;
	private TransportFactory transportFactory;
	
	
	@Override
	protected void tearDown() throws Exception {
		proxyFactoryA.stop();
		proxyFactoryB.stop();
		super.tearDown();
	}
	@Override
	protected void setUp() throws Exception {
		executor = Executors.newFixedThreadPool(5);
		transportFactory = new TransportFactoryImpl(executor, "test");
		super.setUp();
		proxyFactoryA = new ProxyFactoryImpl(transportFactory,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", Config.TEST_PORT, "multiITestA"), executor, "");
		proxyFactoryA.start();
		
		Thread.sleep(100);
		
		proxyFactoryB = new ProxyFactoryImpl(transportFactory,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", Config.TEST_PORT+10000, "multiITestB"), executor, "");
		
		proxyFactoryB.registerMethodReceiver("methodReceiver", new DummyServiceImpl());
		proxyFactoryB.start();
		proxyBAddress = proxyFactoryB.getAddress();
		DummyServiceImpl.callCount = 0;
		remoteService = proxyFactoryA.getRemoteService("methodReceiver", DummyService.class, new String[] { proxyBAddress.toString() });
	}
	
	synchronized public void  incrementCallCount(){
		callCount++;
	}
	
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
