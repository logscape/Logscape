package com.liquidlabs.log.alert.correlate;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CopyOfSlidingWindow implements Window {
    private static final Logger LOGGER = Logger.getLogger(CopyOfSlidingWindow.class);
    private Rule rule;
    private long windowStartTime;
    private long windowSizeSeconds;
    private EventRaiser eventRaiser;
    private List<Event> events = new ArrayList<Event>();

    public CopyOfSlidingWindow(Rule rule, long windowStartTime, long windowSizeSeconds, EventRaiser eventRaiser) {
        this.rule = rule;
        this.windowStartTime = windowStartTime;
        this.windowSizeSeconds = windowSizeSeconds;
        this.eventRaiser = eventRaiser;
    }

    public CopyOfSlidingWindow(Rule copy, long windowStartTime, long windowSizeSeconds,
			EventRaiser eventRaiser, List<Event> events) {
		this.rule = copy;
		this.windowStartTime = windowStartTime;
		this.windowSizeSeconds = windowSizeSeconds;
		this.eventRaiser = eventRaiser;
		for(Event e : events) {
			if(e.occuredAtOrAfter(windowStartTime)){
				this.events.add(e);
			}
		}
		
	}

	public CopyOfSlidingWindow eventReceived(Event event) {
    	if (event.occurredBeforeOrAt(windowStartTime + windowSize())) {
    		events.add(event);
            return this;
        }
        if(rule.evaluate(events)) {
        	sendAggregateEvent();
        	return new CopyOfSlidingWindow(rule.copy(), event.time(), windowSizeSeconds, eventRaiser, Arrays.asList(event));
        }
        events.add(event);
        long slideTo = event.time() - windowSize();
        return new CopyOfSlidingWindow(rule.copy(), slideTo, windowSizeSeconds, eventRaiser, events);
    }

   


    private void sendAggregateEvent() {
        eventRaiser.fire(rule, events, windowStartTime, windowStartTime + windowSize());
    }

    private long windowSize() {
        return windowSizeSeconds * 1000;
    }

	@Override
	public Window copy() {
		return new CopyOfSlidingWindow(rule.copy(), windowStartTime, windowSizeSeconds, eventRaiser);
	}


}
