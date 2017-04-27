package com.liquidlabs.transport.proxy;

import com.liquidlabs.transport.proxy.clientHandlers.Broadcast;
import com.liquidlabs.transport.proxy.clientHandlers.FailFastAndDisable;
import com.liquidlabs.transport.proxy.clientHandlers.HashableAddressByParam;
import com.liquidlabs.transport.proxy.clientHandlers.RoundRobin;

public interface PeerFancyDummyService {

	@Broadcast
	void broadcast(String string) throws Exception;
	
	@Broadcast
	@FailFastAndDisable
	void broadCastAndDisable(String string) throws Exception;

	@RoundRobin(factor=2)
	void shouldRoundRobinWithFactor(String string) throws Exception;

    @HashableAddressByParam
    void sendHashableParamMessage(String firstParamForHash) throws Exception;

}
