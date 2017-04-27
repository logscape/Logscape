package com.liquidlabs.transport;

import com.liquidlabs.common.net.URI;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.transport.protocol.Type;
import com.liquidlabs.transport.proxy.RetryInvocationException;

public interface Sender extends LifeCycle {

	byte[] send(String protocol, URI endPoint, byte[] bytes, Type type, boolean isReplyExpected, long timeoutSeconds, String info, boolean allowlocalRoute) throws InterruptedException, RetryInvocationException;

	URI getAddress();

	void dumpStats();

}
