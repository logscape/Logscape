package com.liquidlabs.replicator.service;

import com.liquidlabs.replicator.data.Meta;
import com.liquidlabs.transport.proxy.clientHandlers.FailFastAndDisable;
import com.liquidlabs.transport.proxy.Remotable;

public interface MetaUpdateListener extends Remotable {

	@FailFastAndDisable
	void newHost(Meta metaInfo);
	
	String getId();
}
