package com.liquidlabs.vso.lookup;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.space.lease.Leasor;
import com.liquidlabs.transport.proxy.AddressUpdater;
import com.liquidlabs.transport.proxy.clientHandlers.FailFastOnce;
import com.liquidlabs.transport.proxy.RemoteOnly;
import com.liquidlabs.transport.proxy.ReplayOnAddressChange;

import java.util.List;


public interface LookupSpace extends LifeCycle, Leasor {
	
	public static final String NAME = LookupSpace.class.getSimpleName();
	
	/**
	 * Return the LUSpace startAddress and startTime
     * @param agentRole
     * @param agentTime
     */
	@FailFastOnce @RemoteOnly
	public String ping(String agentRole, long agentTime);
	
	public List<ServiceInfo> findService(String template);
	
	
	@ReplayOnAddressChange
	String registerService(ServiceInfo serviceInfo, long timeout);
	boolean unregisterService(ServiceInfo serviceToRemove);
	
	@ReplayOnAddressChange
	void registerUpdateListener(String addressListenerId, final AddressUpdater addressListener, String template, final String resourceId, String contextInfo, String location, boolean isStrictLocationMatch) throws Exception;
	
	String[] getServiceAddresses(String serviceName, String location, boolean strictMatch);


	URI getEndPoint();

	long time();

    void addLookupPeer(URI uri);

    void removePeer(URI uri);

}
