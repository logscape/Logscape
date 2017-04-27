package com.liquidlabs.vso;

import com.liquidlabs.transport.proxy.events.Event.Type;

public interface Notifier<T> {
	
	public void notify(Type event, T result);

}
