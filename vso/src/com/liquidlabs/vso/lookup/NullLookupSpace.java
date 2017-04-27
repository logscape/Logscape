package com.liquidlabs.vso.lookup;

import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.proxy.AddressUpdater;

import java.util.List;

public class NullLookupSpace implements LookupSpace {
    @Override
    public String ping(String agentRole, long agentTime) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<ServiceInfo> findService(String template) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String registerService(ServiceInfo serviceInfo, long timeout) {
        return "lease";

    }

    @Override
    public boolean unregisterService(ServiceInfo serviceToRemove) {
        return true;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void registerUpdateListener(String addressListenerId, AddressUpdater addressListener, String template, String resourceId, String contextInfo, String location, boolean isStrictLocationMatch) throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String[] getServiceAddresses(String serviceName, String location, boolean strictMatch) {
        return new String[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public URI getEndPoint() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public long time() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void addLookupPeer(URI uri) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void removePeer(URI uri) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void renewLease(String leaseKey, int expires) throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void cancelLease(String leaseKey) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void start() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void stop() {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
