package com.liquidlabs.log.space.agg;

import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.log.LogRequestBuilder;
import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.ReplayEvent;
import com.liquidlabs.log.space.LogReplayHandler;
import com.liquidlabs.log.space.LogRequest;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HistoAggregatorEventListenerTest {

	String hostname = "locahost";
	String filename = "file";
	String subscriber = "subscriber";
	String pattern = "subscriber";
	DateTime now = new DateTime();
	private LogRequest request;
	LogReplayHandler replayHandler = new MyReplayHandler();

	
	
	@Test
	public void testTimeIsRecent() throws Exception {
		request = new LogRequestBuilder().getLogRequest(subscriber, Arrays.asList(pattern + " | groupBy(1) trans(stuff,1) verbose(true) buckets(5)" ), null, now.minusMinutes(5).getMillis(), now.getMillis());
		HistoAggEventListener handler = new HistoAggEventListener("","",request, replayHandler, "id", false);
		
		assertFalse(handler.isTimeRecent(0));
		
		assertTrue(handler.isTimeRecent(DateTimeUtils.currentTimeMillis()));
		
		assertFalse(handler.isTimeRecent(DateTimeUtils.currentTimeMillis() - 20 * 1000));
	}
	
	public static class MyReplayHandler implements LogReplayHandler {

		private List<ReplayEvent> events;

		public String getId() {
			return null;
		}

		public void handle(ReplayEvent event) {
		}

		public void handle(Bucket event) {
		}

		public int handle(String providerId, String subscriber, int size, Map<String, Object> histo) {
			return 1;
		}

		public int handle(List<ReplayEvent> events) {
			this.events = events; return 100;
		}


		public int status(String provider, String subscriber, String msg) {
			return 1;
		}

		@Override
		public void handleSummary(Bucket bucketToSend) {
			// TODO Auto-generated method stub
			
		}
		
	}

}
