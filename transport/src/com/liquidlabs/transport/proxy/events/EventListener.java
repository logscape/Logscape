package com.liquidlabs.transport.proxy.events;

import com.liquidlabs.transport.proxy.Remotable;


public interface EventListener extends Remotable {
	public String getId();
	public void notify(Event event);

}
