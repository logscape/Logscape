package com.liquidlabs.transport.proxy;

import com.liquidlabs.common.concurrent.NamingThreadFactory;
import com.liquidlabs.common.net.URI;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

import org.joda.time.DateTimeUtils;

import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.TransportFactoryImpl;

public class ProxyRemoteP2PScalePerformanceTest extends TestCase {
	private ProxyFactoryImpl proxyFactoryA;
	boolean enableOutput = false;
	TransportFactory transportFactory = null;
	ExecutorService executor = Executors.newFixedThreadPool(5);
	
	@Override
	protected void setUp() throws Exception {
		System.setProperty("tcp.use.oio.client", "true");
		transportFactory = new TransportFactoryImpl(Executors.newFixedThreadPool(5), "test");
		transportFactory.start();
		
		proxyFactoryA = new ProxyFactoryImpl(transportFactory,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", 11111, "testService"), executor, "");
		proxyFactoryA.start();
		
		Thread.sleep(100);
		
		DummyServiceImpl.verbose = false;
		
		Thread.sleep(100);
		DummyServiceImpl.callCount = 0;
	}
	@Override
	protected void tearDown() throws Exception {
		transportFactory.stop();
		proxyFactoryA.stop();
		Thread.sleep(50);
	}
	
	AtomicInteger count = new AtomicInteger(0);
	
	public void testShouldPassTwoString() throws Exception {
		HashMap<URI, ProxyFactory> proxyMap = new HashMap<URI, ProxyFactory>();
		HashMap<URI, DummyService> serviceMap = new HashMap<URI, DummyService>();
		
		int peerAmount = 1;
		for (int i = 0; i < peerAmount; i++) {
			ProxyFactoryImpl proxyFactoryB = new ProxyFactoryImpl(transportFactory,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", 10000 + i, "testService"), executor, "");
			proxyFactoryB.registerMethodReceiver("methodReceiver", new DummyServiceImpl());
			proxyFactoryB.start();
			URI proxyBAddress = proxyFactoryB.getAddress();
			DummyService remoteService = proxyFactoryA.getRemoteService("methodReceiver", DummyService.class, new String[] { proxyBAddress.toString() } );
			
			proxyMap.put(proxyBAddress, proxyFactoryB);
			serviceMap.put(proxyBAddress, remoteService);
		}
		
		
		HashMap<String, String> map = new HashMap<String, String>();
		map.put("stuff1", "stuff2");
		//Thread.sleep(10 * 1000);
		System.out.println("starting");
		
		final int messageLimit = 100;//00;
		final int total = messageLimit * peerAmount;
		final CountDownLatch countDownLatch = new CountDownLatch(total);
		
		long start = DateTimeUtils.currentTimeMillis();
		
		ExecutorService executors = Executors.newCachedThreadPool(new NamingThreadFactory("XXXX", true, Thread.NORM_PRIORITY));
		for (final DummyService remoteService : serviceMap.values()) {
			executors.execute(new Runnable() { 
				public void run() {
					for (int i = 0; i < messageLimit; i++) {
						String msg = "crap" + i;
						remoteService.twoWay(msg);
//						remoteService.oneWay(msg);
						countDownLatch.countDown();
						int cCount = count.incrementAndGet();
						if (cCount % 100 == 0) {
							System.out.println("Count:" + cCount);
						}
					}
					
				}	
			});
		}
		System.out.println("Send:" + total);
		countDownLatch.await(60, TimeUnit.SECONDS);
		long end = DateTimeUtils.currentTimeMillis();
		double secs = (end - start)/1000.0;
		System.out.println("************" + total + " throughput:" + (double)total/secs + "/sec");
		Thread.sleep(5000);
			
			
//				if (i % 1000 == 0 && i > 0) {
//				}
		System.out.println("Elapsed:" + (end - start));
	}
	

}
