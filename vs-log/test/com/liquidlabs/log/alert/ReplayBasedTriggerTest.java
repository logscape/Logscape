package com.liquidlabs.log.alert;

import com.liquidlabs.log.search.ReplayEvent;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

public class ReplayBasedTriggerTest {
	
	List<String> events = new ArrayList<String>();
	@Test
	public void shouldCollectSomeEvents() throws Exception {
		TriggerFiredCallBack firedCallback = new TriggerFiredCallBack() {
			public void fired(List<String> leadingEvents) {
				System.out.println("Events;" + leadingEvents);
				events.addAll(leadingEvents);
			}
		};
		Schedule schedule = new Schedule();
		
		List<ScheduleHandler> handlers = new ArrayList<ScheduleHandler>();
		ReplayBasedTrigger replayBasedTrigger = new ReplayBasedTrigger(true, "TEST", "Search", schedule, 1, handlers, firedCallback, Executors.newScheduledThreadPool(2));
		replayBasedTrigger.handle(new ReplayEvent());
		replayBasedTrigger.handle(new ReplayEvent());
		replayBasedTrigger.handle(new ReplayEvent());
		Thread.sleep(5500);
		Assert.assertEquals(3, events.size());
	}
}
