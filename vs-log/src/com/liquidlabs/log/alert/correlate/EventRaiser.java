package com.liquidlabs.log.alert.correlate;

import java.util.List;

public interface EventRaiser {
    void fire(Rule rule, List<Event> events, long windowStartTime, long windowEndTime);
}
