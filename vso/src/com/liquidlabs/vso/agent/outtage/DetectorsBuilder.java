package com.liquidlabs.vso.agent.outtage;

import com.liquidlabs.common.Logging;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.lookup.LookupSpace;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by neil on 29/01/16.
 */
public class DetectorsBuilder {

    private final static Logger LOGGER = Logger.getLogger(DetectorsBuilder.class);
    static Logging auditLogger = Logging.getAuditLogger("VAuditLogger", "Outtage");
    private final List<OuttageReactor> detectors = new ArrayList<OuttageReactor>();
    private final Runnable stopTask;

    private OuttageReactor outtageReactor;

    public DetectorsBuilder(String lookupAddress, ProxyFactory proxyFactory, Runnable stopTask, Runnable ... failDetectorTasks) {
        this.stopTask = stopTask;
        LOGGER.info("Created:" + lookupAddress);

        if (lookupAddress.contains(",")) {
            // Manager
            if (VSOProperties.isManagerOnly()) {
                LOGGER.debug("Manager - no actions");
                // Manager
                // nothing to do - the failover should bounce itself when a) manager dies b) manager is older than failover
            } else if (VSOProperties.isFailoverNode()) {
                LOGGER.debug("Failover - actions");
                // Failover

                if (managerExists(lookupAddress, proxyFactory)) {
                    detectors.add(bounceWhenManagerDies(lookupAddress, proxyFactory));
                }

                detectors.add(bounceWhen(bounceWhenManagerComesBackOnline(lookupAddress, proxyFactory)));
            } else {
                // Agent
                LOGGER.debug("Agent - actions");
                detectors.add(bounceWhenManagerDies(lookupAddress, proxyFactory));
                // bounce again when manager is renewed
                detectors.add(bounceWhen(bounceWhenManagerComesBackOnline(lookupAddress, proxyFactory)));
            }
        } else {
            // Non--HA anything
            detectors.add(bounceWhenManagerDies(lookupAddress, proxyFactory));
        }
        detectors.add(bounceWhen(new NicChangedDetector()));
        for (Runnable task : failDetectorTasks) {
            detectors.add(bounceWhen(task));
        }
    }

    private boolean managerExists(String lookupAddress,  final ProxyFactory proxyFactory) {
        try {
            LookupSpace managerLookup = proxyFactory.getRemoteService(LookupSpace.NAME, LookupSpace.class, lookupAddress.split(",")[0]);
            String ping1 = managerLookup.ping(VSOProperties.getResourceType(), System.currentTimeMillis());
            return true;
        } catch (Throwable t) {
            return false;
        }
    }


    private Runnable bounceWhenManagerComesBackOnline(final String lookupAddress, final ProxyFactory proxyFactory) {
        LOGGER.info("Detect manager failure, address:" + lookupAddress);
        return new Runnable() {
            @Override
            public void run() {
                try {

                    LOGGER.debug("Check for NewerManager");
                    // bounce when failover was already up
                    long failoverPing = 0;
                    try {
                        LookupSpace failoverLookup = proxyFactory.getRemoteService(LookupSpace.NAME, LookupSpace.class, lookupAddress.split(",")[1]);
                        String ping = failoverLookup.ping(VSOProperties.getResourceType(), System.currentTimeMillis());
                        failoverPing = Long.parseLong(ping);
                    } catch (Throwable t) {
                        LOGGER.debug("FailoverPing failed", t);

                    }
                    LOGGER.debug("FailoverPing" + new DateTime(failoverPing));
                    long managerTime = 0;
                    try {

                        LookupSpace managerLookup = proxyFactory.getRemoteService(LookupSpace.NAME, LookupSpace.class, lookupAddress.split(",")[0]);
                        String ping1 = managerLookup.ping(VSOProperties.getResourceType(), System.currentTimeMillis());
                        managerTime = Long.parseLong(ping1);
                    } catch (Throwable t) {
                        LOGGER.debug("ManagerPing failed", t);

                    }
                    LOGGER.debug("ManagerPing" + new DateTime(managerTime));
                    if (failoverPing != 0 && managerTime != 0 && failoverPing < managerTime) {
                        LOGGER.info("Detected Manager started after we did, Bouncing Myself");
                        auditLogger.emit("Bouncing", "FailoverOlderThanManager");
                        stopTask.run();
                    }

                } catch (Throwable t) {
                    t.printStackTrace();
                }


            }
        };
    }

    private OuttageReactor bounceWhenManagerDies(String lookupAddress, ProxyFactory proxyFactory) {
        return bounceWhen(new ManagerFailDetector(lookupAddress, proxyFactory));
    }
    public OuttageReactor bounceWhen(Runnable task) {
        // detect something failed and then go boom
         return new OuttageReactor(1, task, new Runnable() {
            @Override
            public void run() {
                LOGGER.warn("DetectedFailure");
                auditLogger.emit("Running","StopTask");
                stopTask.run();
                try {
                    Thread.sleep(10 * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                LOGGER.warn("System. EXIT:10");
                auditLogger.emit("Running","SysExit(10)");
                System.exit(10);

            }
        });
    }
}
