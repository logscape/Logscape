package com.liquidlabs.logserver;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.transport.proxy.clientHandlers.Broadcast;
import com.liquidlabs.transport.proxy.clientHandlers.HashableAddressByParam;
import com.liquidlabs.transport.proxy.Remotable;

public interface LogServer extends Remotable, LifeCycle {

	String NAME = LogServer.class.getSimpleName();

    @HashableAddressByParam
    boolean isAvailable(String hostFileNameForHash);

	@HashableAddressByParam
	int handle(String hostFileNameForHash, LogMessage msg);

	@HashableAddressByParam
	void roll(String hostFileNameForHash, String host, String fromFile, String toFile) throws RuntimeException;

	@HashableAddressByParam
	void deleted(String hostFileNameForHash, String host, String filename);

    // DEPR TOO slow - and can overload server AND because the endpoint count could change...it should be PUSH only
	@HashableAddressByParam
    @Deprecated
	int getStartLine(String hostFileNameForHash, String hostname, String filename);

	// DEPR cause the endpoint count cannot change...it should be PUSH only
	@HashableAddressByParam
	@Deprecated
	long getStartPos(String hostFileNameForHash, LogMessage msg);

    @Broadcast
    void deleteAccount(String id);

    @Broadcast
    void deleteAccountFiles(String userId, long maxAge);
}
