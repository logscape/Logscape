package com.liquidlabs.orm;

import com.liquidlabs.transport.proxy.Remotable;
import com.liquidlabs.transport.proxy.events.Event.Type;

public interface ORMEventListener extends Remotable {
	String getId();
	void notify(String key, String payload, Type event, String source);
}
