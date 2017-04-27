package com.liquidlabs.transport;

import com.liquidlabs.common.LifeCycle;

public interface Receiver extends LifeCycle {
    public byte[] receive(byte[] payload, String remoteAddress, String remoteHostname) throws InterruptedException;

	public boolean isForMe(Object payload);

	public byte[] receive(Object payload, String remoteAddress, String remoteHostname);

}
