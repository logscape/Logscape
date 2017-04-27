package com.liquidlabs.space.notify;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.transport.proxy.clientHandlers.FailFastOnce;
import com.liquidlabs.transport.proxy.events.Event;
import com.liquidlabs.transport.proxy.events.EventListener;
import com.liquidlabs.transport.proxy.events.Event.Type;

public interface EventHandler extends LifeCycle {

	@FailFastOnce(ttl=5)
	void handleEvent(Event event);
	
	@FailFastOnce(ttl=5)
	void handleEvent(String eventId, Event event);

	@FailFastOnce(ttl=5)
	void notify(String[] keys, String[] templates, EventListener listener, Type[] eventMask, long expires);
	
	@FailFastOnce(ttl=5)
	boolean removeListener(String listenerKey);
}
