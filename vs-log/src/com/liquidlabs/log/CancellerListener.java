package com.liquidlabs.log;

import com.liquidlabs.transport.proxy.Remotable;

public interface CancellerListener extends Remotable {
	
	String getId();

	void cancel(String subscriberId);

}
