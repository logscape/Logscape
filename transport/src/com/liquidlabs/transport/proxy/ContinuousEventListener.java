package com.liquidlabs.transport.proxy;


/**
 * Default implementation that wraps up a user event listener and delegates to it
 * upon receiving an invocation.
 * Bespoke implementations register using the {@link ProxyFactoryImpl} 
 * registerEventListener(id, eventListener) method
 */
public class ContinuousEventListener  {

	private final String id;
	private final Object target;
	
	public ContinuousEventListener(String id, Object target) {
		this.id = id;
		this.target = target;
	}
	public String getId() {
		return id;
	}
	public Object getTarget() {
		return target;
	}
}
