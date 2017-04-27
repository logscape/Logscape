package com.liquidlabs.vso.agent.outtage;

import com.liquidlabs.common.Logging;
import com.liquidlabs.common.concurrent.ExecutorService;
import org.apache.log4j.Logger;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tries the appropriate 'task' and another tasks when the first one fails
 */
public class OuttageReactor {
    static Logging auditLogger = Logging.getAuditLogger("VAuditLogger", OuttageReactor.class);
    static Integer outtage = Integer.getInteger("outtage.interval.sec", 30);
    private final static Logger LOGGER = Logger.getLogger(OuttageReactor.class);

    private final ScheduledExecutorService scheduler = ExecutorService.newScheduledThreadPool("services");
    AtomicInteger failCount = new AtomicInteger();

    public OuttageReactor(final int limit, final Runnable detectorAction, final Runnable failedAction) {

        this.scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    if (LOGGER.isDebugEnabled()) LOGGER.debug("Testing:" + detectorAction);
                    detectorAction.run();
                    if (LOGGER.isDebugEnabled()) LOGGER.debug("Success:" + detectorAction);
                } catch (Throwable t) {
                    LOGGER.warn("Failed:" + detectorAction + " t:" + t.toString());
                    failCount.incrementAndGet();
                    auditLogger.emit("Failed-" + failCount.get() , t.toString());
                }
                if (failCount.get() > limit) {
                    auditLogger.emit("Failed-Exceeed","FailAction-" + failedAction);
                    failedAction.run();
                }
            }
        }, outtage, outtage, TimeUnit.SECONDS);
    }
}
