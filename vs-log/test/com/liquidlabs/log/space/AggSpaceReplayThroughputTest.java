package com.liquidlabs.log.space;

import com.liquidlabs.common.concurrent.NamingThreadFactory;
import com.liquidlabs.log.LogRequestBuilder;
import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.ReplayEvent;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.transport.proxy.ProxyFactoryImpl;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.LookupSpaceImpl;
import junit.framework.TestCase;
import org.jmock.Mockery;
import org.joda.time.DateTimeUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class AggSpaceReplayThroughputTest extends TestCase {
	
	Mockery context = new Mockery();

	private AggSpace aggSpace;
	private LookupSpace lookupSpace = new LookupSpaceImpl(11000, 15000);
	private ORMapperFactory mapperFactory;

	@Override
	protected void setUp() throws Exception {
		
		lookupSpace.start();
		Thread.sleep(1 * 1000);
		
		mapperFactory = new ORMapperFactory();
		aggSpace = new AggSpaceImpl("providerId", 
				new SpaceServiceImpl(lookupSpace ,mapperFactory, AggSpace.NAME, mapperFactory.getProxyFactory().getScheduler(), false, false, false), 
				new SpaceServiceImpl(lookupSpace,mapperFactory, AggSpace.NAME_REPLAY, mapperFactory.getProxyFactory().getScheduler(), false, false, false), 
				new SpaceServiceImpl(lookupSpace,mapperFactory, AggSpace.NAME_REPLAY, mapperFactory.getProxyFactory().getScheduler(), false, false, true), 
				mapperFactory.getScheduler());
		
		aggSpace.start();
	}
	@Override
	protected void tearDown() throws Exception {
		aggSpace.stop();
	}
	
	public List<LogEvent>  replays = new ArrayList<LogEvent>();
	
	public class MyLogEventListener implements LogEventListener {
		public void handle(LogEvent event) {
			replays.add(event);
		}

		public String getId() {
			return "myId";
		}
	}
	
	public class MyLogReplayListener implements LogReplayHandler {
		public AtomicInteger count = new AtomicInteger();

		public String getId() {
			return "myId";
		}

		public void handle(Bucket event) {
		}
		public int handle(String providerId, String subscriber, int size, Map<String, Object> histo) {
			return 1;
		}
		public void handle(ReplayEvent event) {
			synchronized (this) {
				count.getAndIncrement();
				latch.countDown();
				if (count.get() % 50 == 0) System.out.println("----" + " - " + count.get() + " latch" + latch.getCount());
			}
		}
		public int handle(List<ReplayEvent> events) {
			for (ReplayEvent replayEvent : events) {
				handle(replayEvent);
			}
			return 100;
		}

		public int status(String provider, String subscriber, String msg) {
			return 1;
		}

		@Override
		public void handleSummary(Bucket bucketToSend) {
			// TODO Auto-generated method stub
			
		}
	}
	CountDownLatch latch = new CountDownLatch(100);
	String line = "2008-11-11 17:02:20,806 DEBUG main (SpaceServiceImpl.java:180)	 - Using AdminSpace:[stcp://mobius.local:15010/_startTime=1226386908203] ProxyAddress [stcp://mobius.local:12005/_startTime=1226386939526]\n";
	
	public void testShouldWriteBucketEventsToHandlerAfterBlowup() throws Exception {
		final int replayCount = 100;//00;
		int concurrency = 3;
		latch = new CountDownLatch(replayCount * concurrency);
		
		final String subscriber = "XXXX";
		ArrayList<String> logFilters = new ArrayList<String>();
		logFilters.add("ERROR");
		final LogRequest request = new LogRequestBuilder().getLogRequest(subscriber, logFilters, "", 100, 200);
//		request.setVerbose(true);
		MyLogReplayListener replayHandler = new MyLogReplayListener();
		
		ProxyFactoryImpl proxyFactory = new ProxyFactoryImpl(12080, Executors.newFixedThreadPool(10), "serviceName");
		proxyFactory.start();

		
		
		AggSpace remoteAggSpace = AggSpaceImpl.getRemoteService("me", lookupSpace, proxyFactory);
		LogReplayHandler remoteReplayHandler = replayHandler;
		
		
		Thread.sleep(2000);
		System.out.println("=================== search ================");
		long start = DateTimeUtils.currentTimeMillis();

		remoteAggSpace.search(request, replayHandler.getId(), remoteReplayHandler);
//		aggSpace.search(request, replayHandler.getId(), replayHandler);
		
		final AtomicInteger sent = new AtomicInteger();
		
		ExecutorService executor = Executors.newFixedThreadPool(concurrency, new NamingThreadFactory("sender", true, Thread.NORM_PRIORITY + 1));
		for (int c = 0; c < concurrency; c++) {
			final int conc = c;
			executor.execute(new Runnable() {

				public void run() {
					try {
						for (int i = 0; i < replayCount; i++) {
							int write = aggSpace.write(new ReplayEvent("sourceURI", conc+ i, 0, 0, subscriber, 1000, ""), false, "", 0, 0);
							//Thread.yield();
							if (i % 100 == 0) System.out.println(conc + " SENT>>>>>>>" + i + " size:" + write + " sent:" + sent.get());
							sent.incrementAndGet();
						}
						System.out.println(" ========================= EXITING =====================");
					} catch (Throwable t) {
						t.printStackTrace();
					}
				}
			});
		}
		
		boolean latchCompleted = latch.await(300, TimeUnit.SECONDS);
		long end = DateTimeUtils.currentTimeMillis();
		long delta = end - start;
		
		System.out.println("\n\n\t\t\t ** ReplayCompleted*" + latchCompleted + " ********* Received:" + replayHandler.count.get() + " elapsed:" + (end - start) + " perSecond:" + replayHandler.count.get()/(delta/1000.0));
		assertTrue(latchCompleted);
		assertEquals(concurrency * replayCount, replayHandler.count.get());
		
		
	}
	
	

}
