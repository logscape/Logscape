package com.liquidlabs.transport.proxy;

import com.liquidlabs.common.net.URI;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

import org.joda.time.DateTimeUtils;

import com.liquidlabs.common.UID;
import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.TransportFactoryImpl;

public class ProxyRemoteManyProxyCreationTest extends TestCase {
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
		
		DummyServiceImpl.verbose = false;
		
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
	
	boolean fail = false;
	public void testShouldConcurrentlyBeGood() throws Exception {
		HashMap<String, String> map = new HashMap<String, String>();
		map.put("stuff1", "stuff2");
		//Thread.sleep(10 * 1000);
		System.out.println("starting");
		
		DummyServiceImpl.verbose = false;
		long start = DateTimeUtils.currentTimeMillis();
		int limit = 100;
		final AtomicInteger received = new AtomicInteger();
		ExecutorService executor = Executors.newCachedThreadPool();
		for (int i = 0; i < limit; i++) {
			final int c = i;
			
			executor.submit(new Runnable() {
				public void run() {
					try {
						DummyService remoteService = proxyFactoryA.getRemoteService("methodReceiver", DummyService.class, new String[] { proxyBAddress.toString() } );
						String msg = "crap" + c;
						String mapString = remoteService.twoWay(msg);
						System.out.println("Got:" + mapString);
						received.incrementAndGet();
						proxyFactoryA.stopProxy(remoteService);
						if (!mapString.contains(msg)) {
							System.out.println("FAILED:" + msg);
							fail = true;
						}
					} catch (Throwable t) {
						t.printStackTrace();
					}
				}
			});
		}
		executor.shutdown();
		executor.awaitTermination(60, TimeUnit.SECONDS);
		
		long end = DateTimeUtils.currentTimeMillis();
		System.out.println("Received:" + received + " Elapsed:" + (end - start));
		assertTrue("Pooo - it failed", !fail);
	}
	

}
