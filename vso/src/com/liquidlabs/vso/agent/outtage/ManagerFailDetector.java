package com.liquidlabs.vso.agent.outtage;

import com.liquidlabs.common.Logging;
import com.liquidlabs.common.TestModeSetter;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.lookup.LookupSpace;
import org.apache.log4j.Logger;

/**
 * Created by neil on 29/01/16.
 */
public class ManagerFailDetector implements  Runnable {
    protected Logging auditLogger = Logging.getAuditLogger("VAuditLogger", ManagerFailDetector.class);
    private final static Logger LOGGER = Logger.getLogger(ManagerFailDetector.class);

    protected LookupSpace managerLookup;
    private String startPing;

    public ManagerFailDetector(String managerAddress, ProxyFactory proxyFactory) {
        if (managerAddress.contains(",")) {
            managerAddress = managerAddress.split(",")[0];
        }
        LOGGER.debug("Checking ManagerAddress:" + managerAddress);
        managerLookup = proxyFactory.getRemoteService(LookupSpace.NAME, LookupSpace.class, managerAddress);
    }

    @Override
    public void run() {
        try {
            if (TestModeSetter.isTestMode()) return;
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Ping:" + managerLookup);
            String ping = managerLookup.ping(VSOProperties.getResourceType(), System.currentTimeMillis());
            if (startPing == null) {
                startPing = ping;
            } else if (!startPing.equals(ping)) {
                throw new RuntimeException("Manager Was bounced, we are bouncing too...:" + startPing + " != " + ping);
            }
        } catch (Throwable t) {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("PingFailed:" + managerLookup + " ex:" + t.toString());
            t.printStackTrace();
            auditLogger.emit("PingFailed", t.getMessage());
            throw new RuntimeException(t);
        }
    }
}
