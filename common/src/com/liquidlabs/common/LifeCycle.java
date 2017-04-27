package com.liquidlabs.common;


public interface LifeCycle {
	
	// NOTE: ordering is important here as we rely on ordinals
	enum State {
		UNASSIGNED, STARTED, RUNNING, PENDING, SUSPENDED, ASSIGNED, STOPPING, STOPPED, ERROR, WARNING
	}
	public void start();
	public void stop();

}
