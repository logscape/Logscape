package com.liquidlabs.common.monitor;

import com.liquidlabs.common.monitor.Event;
import com.liquidlabs.common.monitor.EventMonitor;
import org.apache.log4j.Logger;

import java.io.PrintWriter;
import java.io.StringWriter;

public class LoggingEventMonitor implements EventMonitor {
    private static final Logger eventLogger = Logger.getLogger("EventMonitor");

    @Override
    public void raise(Event e) {
        eventLogger.info(e);
    }

    @Override
    public void raiseWarning(Event event, Throwable throwable) {
        if (throwable == null) {
            eventLogger.warn(event);
        } else {
            final StringWriter stringWriter = new StringWriter();
            throwable.printStackTrace(new PrintWriter(stringWriter));
            eventLogger.warn(event.with("message", throwable.getMessage()).with("stack", stringWriter.toString()));
        }
    }
}
