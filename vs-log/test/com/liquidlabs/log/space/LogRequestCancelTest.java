package com.liquidlabs.log.space;

import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.log.LogRequestBuilder;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class LogRequestCancelTest {
	
	@Test
	public void shouldAllowCopyiesOfRequestsToCancel() throws Exception {
		DateTime timeA = new DateTime(2009, 01, 01, 14, 54,57,00);
		DateTime timeB = new DateTime(2009, 01, 01, 16, 54,57,00);
//		LogRequest logRequest = new LogRequest("crap", "file", timeA.getMillis(), timeB.getMillis());
		
		ArrayList<LogRequest> requests = new ArrayList<LogRequest>();
		LogRequestBuilder builder = new LogRequestBuilder();
		LogRequest logRequest = builder.getLogRequest("sub", Arrays.asList(".* | stuff", ".** | stuff2"), null, timeA.getMillis(), timeB.getMillis());
		logRequest.createSummaryBucket(null);
		
		requests.add(logRequest);
		LogRequest copy1 = logRequest.copy();
		requests.add(copy1);
		Assert.assertNotNull(copy1.summaryBucket);
		requests.add(copy1.copy(100, 200));
		requests.add(copy1.copy(1000, 2000, 0));
		requests.add(copy1.copyWithBucketCalc(timeA.getMillis(), timeB.getMillis(), 2));
		
		logRequest.cancel();
		
		int count = 0;
		for (LogRequest logRequest2 : requests) {
			System.out.println(count++ + " - checking:" + logRequest);
			Assert.assertTrue(logRequest2.isCancelled());
		}
	}

}
