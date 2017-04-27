package com.liquidlabs.vso.agent.outtage;

import com.liquidlabs.common.Logging;
import com.liquidlabs.common.NetworkUtils;

/**
 * Created by neil on 29/01/16.
 */
public class NicChangedDetector  implements Runnable {
    static Logging auditLogger = Logging.getAuditLogger("VAuditLogger", NicChangedDetector.class);

    private String usedNic = NetworkUtils.getNic();
    @Override
    public void run() {
        boolean nicDown = NetworkUtils.isNicDown(usedNic);
        if (nicDown) {
            auditLogger.emit("NicFailed", usedNic);
            throw new RuntimeException("NicIsDown:" + usedNic);
        }

    }
}
