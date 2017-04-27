package com.liquidlabs.common.monitor;

import com.liquidlabs.common.monitor.Event;

public interface EventMonitor {
    void raise(Event e);

    void raiseWarning(Event event, Throwable throwable);
}
