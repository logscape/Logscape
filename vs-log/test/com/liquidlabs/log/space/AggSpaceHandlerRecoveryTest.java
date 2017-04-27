package com.liquidlabs.log.space;

import com.liquidlabs.log.LogRequestBuilder;
import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.ReplayEvent;
import com.liquidlabs.log.search.functions.Function;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.ServiceInfo;
import junit.framework.TestCase;
import org.jmock.Expectations;
import org.jmock.Mockery;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AggSpaceHandlerRecoveryTest extends TestCase {
	
	Mockery context = new Mockery();

	private AggSpace aggSpace;
	private LookupSpace lookupSpace;
	private ORMapperFactory mapperFactory;

	@Override
	protected void setUp() throws Exception {
		
		lookupSpace = context.mock(LookupSpace.class);
		
		
		mapperFactory = new ORMapperFactory();
		aggSpace = new AggSpaceImpl("providerId", 
				new SpaceServiceImpl(lookupSpace ,mapperFactory, LogSpace.NAME, mapperFactory.getProxyFactory().getScheduler(), false, false, false), 
				new SpaceServiceImpl(lookupSpace,mapperFactory, LogSpace.NAME, mapperFactory.getProxyFactory().getScheduler(), false, false, false),
				new SpaceServiceImpl(lookupSpace ,mapperFactory, LogSpace.NAME, mapperFactory.getProxyFactory().getScheduler(), false, false, true),
                mapperFactory.getProxyFactory().getScheduler());
		
		context.checking(new Expectations() {{
			atLeast(1).of(lookupSpace).registerService(with(any(ServiceInfo.class)), with(any(long.class)));
		}});
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
	
	public List<Map<String, Object>>  histos = new ArrayList<Map<String, Object>>();
	boolean blowup = false;

	private String fileIncludes;
	public class MyLogReplayListener implements LogReplayHandler {

		public String getId() {
			return "myId";
		}
		public void handle(ReplayEvent event) {
		}

		public void handle(Bucket event) {
		}
		public int handle(String providerId, String subscriber, int size, Map<String, Object> histo) {
			System.out.println("Received HISTO: " + histo);
			if (blowup) {
				System.out.println("Boooooooooooooooom!");
				throw new RuntimeException("booooooom ! = blowing up");
			}
			histos.add(histo);
			return 1;
		}
		public int handle(List<ReplayEvent> events) { return 100;
		}
		public int status(String provider, String subscriber, String msg) {
			return 1;
		}
		public void handleSummary(Bucket bucketToSend) {
		}
	}
	
	public void testShouldWriteBucketEventsToHandlerAfterBlowup() throws Exception {
		
		String subscriber = "SUB";
		ArrayList<String> logFilters = new ArrayList<String>();
		logFilters.add("ERROR");
		LogRequest request = new LogRequestBuilder().getLogRequest(subscriber, logFilters, "", 100, 200);
		MyLogReplayListener replayHandler = new MyLogReplayListener();
		
		System.out.println("=================== search ================");
		Thread.sleep(100);

		aggSpace.search(request, replayHandler.getId(), replayHandler);
		
		// BLOWUP - for this item
		System.out.println("=================== write bucket 1 ================");
		blowup = true;
		aggSpace.write(getBucket(subscriber, request), false, "", 0,0);
		
		Thread.sleep(3 * 1000);
		
		
		// DO NOT BLOWUP - for this search
		aggSpace.search(request, replayHandler.getId(), replayHandler);
		blowup = false;
		System.out.println("=================== write bucket 2 ================");
		aggSpace.write(getBucket(subscriber, request), false, "", 0,0);
		
		Thread.sleep(3 * 1000);
		
		System.out.println("Histos Size:" + histos.size());
		assertTrue(histos.size() > 0);
	}
	
	private Bucket getBucket(String subscriber, LogRequest request) {
		Bucket bucket = new Bucket(100, 200, new ArrayList<Function>(), 0, "ERROR", "sourceURI", subscriber, "");
		bucket.increment(100);
		return bucket;
	}
	public void testShouldWriteBucketEventsToHandler() throws Exception {
		
		String subscriber = "SUB";
		ArrayList<String> logFilters = new ArrayList<String>();
		logFilters.add("ERROR");
		LogRequest request = new LogRequestBuilder().getLogRequest(subscriber, logFilters, "", 100, 200);
		MyLogReplayListener replayHandler = new MyLogReplayListener();
		
		System.out.println("=================== search ================");
		Thread.sleep(100);
		
		aggSpace.search(request, replayHandler.getId(), replayHandler);
		
		System.out.println("=================== write bucket ================");
		aggSpace.write(getBucket(subscriber, request), false, "", 0,0);
		
		Thread.sleep(3 * 1000);
		
		System.out.println("Histos Size:" + histos.size());
		assertEquals(1, histos.size());
	}
    public void testShouldWriteMultipleBucketEventsToHandler() throws Exception {

        String subscriber = "SUB";
        ArrayList<String> logFilters = new ArrayList<String>();
        logFilters.add("ERROR");
        LogRequest request = new LogRequestBuilder().getLogRequest(subscriber, logFilters, "", 100, 200);
        MyLogReplayListener replayHandler = new MyLogReplayListener();

        System.out.println("=================== search ================");
        Thread.sleep(100);

        aggSpace.search(request, replayHandler.getId(), replayHandler);

        System.out.println("=================== write bucket ================");
        aggSpace.write(getBucket(subscriber, request), false, "", 0,0);
        Thread.sleep(1000);
        aggSpace.write(getBucket(subscriber, request), false, "", 0,0);

        Thread.sleep(1000);

        System.out.println("Histos Size:" + histos.size());
        assertEquals(2, histos.size());
    }
	

}
