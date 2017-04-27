package com.liquidlabs.transport;

import com.liquidlabs.common.net.URI;
import java.util.Set;

import com.liquidlabs.common.LifeCycle;

public interface TransportFactory extends LifeCycle {

	enum TRANSPORT { NETTY, RABBIT}

	EndPoint getEndPoint(URI uri, Receiver receiver, boolean reuseEndpoint);

	Set<String> supportedProtocols();

}
