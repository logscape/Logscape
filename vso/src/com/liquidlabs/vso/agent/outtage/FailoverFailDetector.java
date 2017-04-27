package com.liquidlabs.vso.agent.outtage;

import com.liquidlabs.common.Logging;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.vso.lookup.LookupSpace;

/**
 * Created by neil on 29/01/16.
 */
public class FailoverFailDetector extends ManagerFailDetector {



    // failover address is item [1]
    public FailoverFailDetector(String lookupAddress, ProxyFactory proxyFactory) {
        super(lookupAddress, proxyFactory);
        auditLogger = Logging.getAuditLogger("VAuditLogger", FailoverFailDetector.class);
        if (lookupAddress.contains(",")) {

            lookupAddress = lookupAddress.split(",")[1];
        }
        managerLookup = proxyFactory.getRemoteService(LookupSpace.NAME, LookupSpace.class, lookupAddress);
    }
}
