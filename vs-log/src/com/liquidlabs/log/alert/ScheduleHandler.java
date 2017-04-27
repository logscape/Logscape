package com.liquidlabs.log.alert;

import java.util.Map;

import com.liquidlabs.log.search.ReplayEvent;

public interface ScheduleHandler {

	void handle(ReplayEvent event, Map<Long, ReplayEvent> logEvents, int trigger, int triggerCount);

}
