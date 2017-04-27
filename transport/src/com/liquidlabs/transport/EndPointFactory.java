package com.liquidlabs.transport;

import com.liquidlabs.common.net.URI;

import com.liquidlabs.common.LifeCycle;

public interface EndPointFactory extends LifeCycle {
	
	public EndPoint getEndPoint(URI uri, Receiver receiver);

}
