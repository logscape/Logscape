package com.liquidlabs.transport.proxy;

import com.liquidlabs.transport.proxy.clientHandlers.FailFastOnce;

public interface AddressUpdater extends Remotable {
	void setProxyClient(ProxyClient<?> client);
	void updateEndpoint(String address, String replicationAddress) throws Exception;

    @FailFastOnce
	void removeEndPoint(String address, String replicationAddress);

    @FailFastOnce
	void syncEndPoints(String[] addresses, String[] replicationLocations) throws Exception;
	
	String getId();
	
	void setId(String clientId);

}
