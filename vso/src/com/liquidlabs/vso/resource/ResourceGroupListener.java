package com.liquidlabs.vso.resource;

import com.liquidlabs.transport.proxy.Remotable;
import com.liquidlabs.transport.proxy.events.Event.Type;

public interface ResourceGroupListener extends Remotable {

	void resourceGroupUpdated(Type event, ResourceGroup result);
	String getId();

}
