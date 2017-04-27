/**
 * 
 */
package com.liquidlabs.log.space;

import com.liquidlabs.transport.proxy.clientHandlers.FailFastAndDisable;
import com.liquidlabs.transport.proxy.Remotable;


public interface LogEventListener extends Remotable {
	
	String getId();
	
	@FailFastAndDisable
	void handle(LogEvent event);
}