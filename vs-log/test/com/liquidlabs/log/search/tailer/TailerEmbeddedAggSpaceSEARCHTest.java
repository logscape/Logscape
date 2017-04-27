package com.liquidlabs.log.search.tailer;

import com.liquidlabs.common.monitor.LoggingEventMonitor;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.Query;
import com.liquidlabs.log.search.SearchRunnerI;
import com.liquidlabs.log.search.functions.Function;
import com.liquidlabs.log.space.AggSpace;
import com.liquidlabs.log.space.LogRequest;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

public class TailerEmbeddedAggSpaceSEARCHTest {
	
	Mockery context = new Mockery();
	private TestSearchRunner searchDispatcher;
	private AggSpace aggSpace;
	private TailerEmbeddedAggSpace tailerAggSpace;
	FieldSet fieldSet = FieldSets.get();
	
	@Before
	public void setup() {
		
		aggSpace = context.mock(AggSpace.class);
		
		searchDispatcher = new TestSearchRunner();
		tailerAggSpace = new TailerEmbeddedAggSpace(aggSpace, searchDispatcher, Executors.newScheduledThreadPool(10), "host", new LoggingEventMonitor());;
	}
	
	@Test
	public void testShouldPerformSearch() throws Exception {
		
		context.checking(new Expectations() {
			{
				// batch size 2 means we should see 2 writes
				atLeast(1).of(aggSpace).write(with(any(List.class)));
				one(aggSpace).status(with(any(String.class)), with(any(String.class)), with(any(long.class)), with(any(int.class)));
				one(aggSpace).status(with(any(String.class)), with(any(String.class)), with(any(long.class)), with(any(int.class)));
			}
		});
		
		LogRequest request = new LogRequest("subscriber", new DateTime().minusHours(1).getMillis(), new DateTime().getMillis());
		request.addQuery(new Query(0, "pattern"));
		request.setBucketCount(10);
		request.setVerbose(true);
		tailerAggSpace.search(request, "", null);
		
		Thread.sleep(1000);
		
		System.out.println("Checking running");
//		Assert.assertFalse("Isnt running:" + request.subscriber(), tailerAggSpace.isRunning(request.subscriber()));
		
		context.assertIsSatisfied();
		
	}
	
	public class TestSearchRunner implements SearchRunnerI {

		public int search(LogRequest request) {
			long start = request.getStartTimeMs();
			long end   = request.getEndTimeMs();
			long delta = (end - start)/request.getBucketCount();
			
			for (int i = 0; i < request.getBucketCount(); i++) {
				long s = request.getStartTimeMs() + (i * delta);
				long e = s + delta;
				Bucket bucket = new Bucket(s, e, new ArrayList<Function>(),
						0, "pattern", "source", request.subscriber(), "");
				bucket.increment();
				tailerAggSpace.write(bucket, false, "", 0,0);
			}
			tailerAggSpace.cancelRequest(request.subscriber());
			return 0;
		}

		@Override
		public void removeCompleteTasks() {
		}
		
	}

}
