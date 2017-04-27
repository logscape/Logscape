package com.liquidlabs.transport.proxy;

public interface RemoteClientCallback extends Remotable {
	
	public void callback(String payload);

}
