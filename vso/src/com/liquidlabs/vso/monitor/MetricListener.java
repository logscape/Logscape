package com.liquidlabs.vso.monitor;

import com.liquidlabs.transport.proxy.clientHandlers.FailFastAndDisable;
import com.liquidlabs.transport.proxy.Remotable;

public interface MetricListener extends Remotable {

	String getId();

	@FailFastAndDisable
	void handle(Metrics metrics);
}
