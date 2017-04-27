/**
 * 
 */
package com.liquidlabs.log.alert.correlate;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.log.alert.Schedule;
import com.liquidlabs.log.alert.ScheduleHandler;
import com.liquidlabs.log.alert.TriggerFiredCallBack;
import com.liquidlabs.log.search.ReplayEvent;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class HellRaiser implements EventRaiser {
	
	static final Logger STD_LOGGER = Logger.getLogger(HellRaiser.class);
	static final Logger LOGGER = Logger.getLogger("CorrLogger");
	private final String alertName;
	private final List<ScheduleHandler> handlers;
	private final TriggerFiredCallBack firedCallBack;
	
	public HellRaiser(String alertName, List<ScheduleHandler> handlers, TriggerFiredCallBack firedCallBack) {
		this.alertName = alertName;
		this.handlers = handlers;
		this.firedCallBack = firedCallBack;
	}
	
	@Override
	public void fire(Rule rule, List<Event> events, long windowStartTime,
                     long windowEndTime) {

		STD_LOGGER.info(String.format(" %s Schedule[%s] TRIGGERED[%d] ThresholdPassed[%s]", Schedule.TAG, alertName, 1, 1));
		
		LOGGER.info(String.format("Id:%s EventFired key:%s. Window Time %s <=> %s. Event Count %d. Rule %s", alertName, events.get(0).getKey(), DateUtil.shortDateTimeFormat3.print(windowStartTime),DateUtil.shortDateTimeFormat3.print(windowEndTime), events.size(), rule.describe(events)));
		List<String> leadingEvents = new ArrayList<String>();
		Map<Long, ReplayEvent> replays = new HashMap<Long, ReplayEvent>();
		for(Event event : events) {
			LOGGER.info(event);
			ReplayEvent replay = event.getReplay();
			long time = replay.getTime();
			while (replays.containsKey(time)) time++;
			replays.put(time, replay);
			leadingEvents.add(replay.getRawData());
		}
		if (firedCallBack != null) firedCallBack.fired(leadingEvents);
		
		for(ScheduleHandler handler : handlers) {
			handler.handle(events.get(0).getReplay(), replays, 1, events.size());
		}
		
	}
}