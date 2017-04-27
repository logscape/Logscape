package com.liquidlabs.transport.proxy;

import com.liquidlabs.transport.proxy.clientHandlers.FailFastAndDisable;

public interface NotifyInterface extends Remotable {
	
	@FailFastAndDisable
	public void notify(String payload) throws Exception;
	
	public String getId();

}
