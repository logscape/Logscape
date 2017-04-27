package com.liquidlabs.vso.resource;

import com.liquidlabs.transport.proxy.Remotable;

public interface RequestEventListener extends Remotable {
	
	void requested(String requestId, String owner, String ownerLabel, String template, int priority, int count);

	String getId();

}
