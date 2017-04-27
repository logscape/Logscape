package com.liquidlabs.log.alert.correlate;

import java.util.List;

public interface Rule {
    boolean complete();

    Rule copy();

    boolean evaluate(Event event);

	boolean evaluate(List<Event> events);
    String describe(List<Event> events);
}
