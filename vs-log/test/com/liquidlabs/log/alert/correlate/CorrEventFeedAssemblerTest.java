package com.liquidlabs.log.alert.correlate;

import com.liquidlabs.log.alert.ScheduleHandler;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.search.ReplayEvent;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class CorrEventFeedAssemblerTest {
	
	
	@Test
	public void shouldCreateTrigger() throws Exception {
		ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
		CorrEventFeedAssembler eventFeedAssembler = new CorrEventFeedAssembler(scheduler, new ArrayList<ScheduleHandler>(), null);
		CorrEventFeed eventFeed = eventFeedAssembler.createTrigger("alertName", 3, "corr: time:5 type:sequence sequence:INFO,INFO,INFO field:level key:filename", "id", java.util.Arrays.asList(FieldSets.getBasicFieldSet()));
		Assert.assertEquals("level", eventFeed.fieldName());
		Assert.assertEquals("filename", eventFeed.keyField());
		
		verify(scheduler).scheduleAtFixedRate(any(Runnable.class), eq(3L), eq(3L), eq(TimeUnit.SECONDS));
		
		// now play a ReplayEvent
		ReplayEvent replayEvent = new ReplayEvent("src", 0, 0, 0,"sub", 0, "rawLine" );
		replayEvent.setSubscriber("id");
		eventFeed.handle(replayEvent);
		eventFeed.fireNext();
	}

}
