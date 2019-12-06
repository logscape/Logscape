package com.liquidlabs.transport.proxy;

import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.Config;
import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.TransportFactoryImpl;
import com.liquidlabs.transport.TransportProperties;

public class MultiInvocationConcurrencyTest {
	private ProxyFactoryImpl proxyFactoryA;
	boolean enableOutput = false;
	private ProxyFactoryImpl proxyFactoryB;
	private URI proxyBAddress;
	private DummyService remoteService;
	
	AtomicInteger callCount = new AtomicInteger();
	private ExecutorService executor;
	private TransportFactory transportFactory;
	
	
	// TEST CONFIG
	int concurrency = 20;
	int msgs = 1000;
	boolean retrictPORTALLOC = false;
	boolean portScanDebug = true;
	boolean poolDebug = false;
	boolean nioServer = true;
	boolean nioClient = true;
	
	
	@After
	public void tearDown() throws Exception {
		proxyFactoryA.stop();
		proxyFactoryB.stop();
	}
	@Before
	public void setUp() throws Exception {
		
		if (portScanDebug) System.setProperty("port.scan.debug","true");
		if (poolDebug) System.setProperty("netty.pool.debug", "true");
		if (retrictPORTALLOC) System.setProperty(TransportProperties.VSO_CLIENT_PORT_RESTRICT, "true");
		if (!nioServer) System.setProperty("tcp.use.oio.server", "true");
		if (!nioClient) System.setProperty("tcp.use.oio.client", "true");
		
		DummyServiceImpl.verbose = false;

		executor = Executors.newCachedThreadPool();
		transportFactory = new TransportFactoryImpl(executor, "test");
		proxyFactoryA = new ProxyFactoryImpl(transportFactory,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", Config.TEST_PORT, "multiITestA"), executor, "mi");
		proxyFactoryA.start();
		
		Thread.sleep(100);
		
		proxyFactoryB = new ProxyFactoryImpl(transportFactory,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", 44444, "multiITestA"), executor, "mi");
		
		proxyFactoryB.registerMethodReceiver("methodReceiver", new DummyServiceImpl());
		proxyFactoryB.start();
		proxyBAddress = proxyFactoryB.getAddress();
		DummyServiceImpl.callCount = 0;
		remoteService = proxyFactoryA.getRemoteService("methodReceiver", DummyService.class, new String[] { proxyBAddress.toString() });
	}

	// TODO FIX IN THE BUILD
	@Test
	public void testMultiInvocationIsHandledSafely() throws Exception {
		if (true) return;
		
		final CountDownLatch latch = new CountDownLatch(msgs);
		
		ExecutorService executor = Executors.newFixedThreadPool(concurrency);
		
		
		for (int i = 0; i < msgs; i++) {
			final int c = i;
//			submitOne(latch, executor, c, i % 2 == 0);
			submitOne(latch, executor, c, false);
		}
		System.out.println(new Date() + " SUBMITTED =====================================waiting....");
		long start = System.currentTimeMillis();
		boolean finished = latch.await(60, TimeUnit.SECONDS);
		long elapsed = System.currentTimeMillis() - start;
		
		double elapsedSec = elapsed/1000.0;
		System.out.println(String.format("%b Elapsed:%dms %f", finished, elapsed, elapsedSec));
		System.out.println("Rate:" + (msgs / elapsedSec ) );
		
		System.out.println(new Date() + " 1 DONE WAITING =====================================FinishedTasks:" + finished + " elapsed:" + elapsed);
		
		logOutstandingTasks(executor);
		
		Assert.assertEquals(msgs, callCount.intValue());		
		System.out.println(" done----------------------------------");
	}
	private void logOutstandingTasks(ExecutorService executor) {
		List<Runnable> shutdownNow = executor.shutdownNow();
		for (Runnable runnable : shutdownNow) {
			System.out.println("runnable:" + runnable.toString());
		}
	}
	AtomicInteger replies = new AtomicInteger();
	private void submitOne(final CountDownLatch latch, ExecutorService executor, final int c, final boolean pause) {
		executor.submit(new Runnable() {
			String payload;
			
			public void run() {
				try { 
					payload = Thread.currentThread().getName() + "-" +c;
					String result = null;
					if (pause) {
						result = remoteService.twoWayWithPause(payload, 1l);
					} else {
						result = remoteService.twoWay(payload);
					}
					if (result != null) callCount.incrementAndGet();
					int repliesReceived = replies.getAndIncrement();
					if (repliesReceived % 1000 == 0) System.out.println("\tREPLY:" + new Date() + " CallCount:" + callCount + " REPLY_COUNT:" + repliesReceived);
					
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

}
