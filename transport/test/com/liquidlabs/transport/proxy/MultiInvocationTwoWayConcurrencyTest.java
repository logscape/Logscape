package com.liquidlabs.transport.proxy;

import com.liquidlabs.common.net.URI;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

import com.liquidlabs.transport.Config;
import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.TransportFactoryImpl;

public class MultiInvocationTwoWayConcurrencyTest extends TestCase {
	private ProxyFactoryImpl proxyFactoryA;
	boolean enableOutput = false;
	private ProxyFactoryImpl proxyFactoryB;
	private URI proxyBAddress;
	private DummyService remoteService;
	
	AtomicInteger callCount = new AtomicInteger();
	private ExecutorService executor;
	private TransportFactory transportFactory;
	private DummyServiceImpl dummyService;


	@Override
	protected void tearDown() throws Exception {
		proxyFactoryA.stop();
		proxyFactoryB.stop();
		dummyService.stop();
		transportFactory.stop();
	}
	@Override
	protected void setUp() throws Exception {
		executor = Executors.newCachedThreadPool();
		transportFactory = new TransportFactoryImpl(executor, "test");
		super.setUp();
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
	
	public void testMultiInvocationIsHandledSafely() throws Exception {
		int concurrency = 120;
		final CountDownLatch latch = new CountDownLatch(concurrency);
		
		ExecutorService executor = Executors.newCachedThreadPool();
		for (int i = 0; i < concurrency; i++) {
			final int c = i;
			executor.submit(new Runnable() {
				String payload;
				
				public void run() {
					try { 
						payload = Thread.currentThread().getName() + "-" +c;
						String result = remoteService.twoWayWithPause(payload, 1l);
						if (result != null && result.equals(payload)) callCount.incrementAndGet();
						System.err.println("CallCount:" + callCount);
					} catch (Exception ex) {
						ex.printStackTrace();
						System.err.println("Stopping: " + Thread.currentThread().getName());
					} finally {
						latch.countDown();
					}
				}
				@Override
				public String toString() {
					return super.toString() + " p:" + payload;
				}
			});
		}
		latch.await(60, TimeUnit.SECONDS);
		
		List<Runnable> shutdownNow = executor.shutdownNow();
		for (Runnable runnable : shutdownNow) {
			System.err.println("r:" + runnable.toString());
			
		}
		
		assertEquals(concurrency, callCount.intValue());		
	}

}
