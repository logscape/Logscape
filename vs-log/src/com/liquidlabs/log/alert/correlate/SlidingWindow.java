package com.liquidlabs.log.alert.correlate;

import java.util.ArrayList;
import java.util.List;

public class SlidingWindow implements Window {
    private Rule rule;
    private long windowStartTime;
    private long windowSizeSeconds;
    private EventRaiser eventRaiser;
    private List<Event> events = new ArrayList<Event>();

    public SlidingWindow(Rule rule, long windowStartTime, long windowSizeSeconds, EventRaiser eventRaiser) {
        this.rule = rule;
        this.windowStartTime = windowStartTime;
        this.windowSizeSeconds = windowSizeSeconds;
        this.eventRaiser = eventRaiser;
    }

    public SlidingWindow eventReceived(Event event) {
        if (event.occurredBeforeOrAt(windowStartTime + windowSize())) {
            if(rule.evaluate(event)) {
                return raiseEventIfComplete(event);
            } else {
                return new SlidingWindow(rule.copy(), event.time(), windowSizeSeconds, eventRaiser);
            }
        }
        return new SlidingWindow(rule.copy(), event.time(), windowSizeSeconds, eventRaiser).eventReceived(event);
    }

    private SlidingWindow raiseEventIfComplete(Event event) {
        events.add(event);
        if (rule.complete()) {
            sendAggregateEvent();
            return new SlidingWindow(rule.copy(), event.time(), windowSizeSeconds, eventRaiser);
        } else {
            return this;
        }
    }


    private void sendAggregateEvent() {
        eventRaiser.fire(rule, events, windowStartTime, events.get(events.size() -1).time());
    }

    private long windowSize() {
        return windowSizeSeconds * 1000;
    }

	@Override
	public Window copy() {
		return new SlidingWindow(rule.copy(), windowStartTime, windowSizeSeconds, eventRaiser);
	}


}
