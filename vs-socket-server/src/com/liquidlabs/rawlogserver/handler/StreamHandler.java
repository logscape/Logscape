package com.liquidlabs.rawlogserver.handler;

import com.liquidlabs.common.LifeCycle;

public interface StreamHandler extends LifeCycle {
	
	void handled(byte[] payload, String address, String host, String rootDir);

	void setTimeStampingEnabled(boolean b);

	StreamHandler copy();

}
