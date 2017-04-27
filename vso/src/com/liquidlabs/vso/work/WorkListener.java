package com.liquidlabs.vso.work;

import com.liquidlabs.transport.proxy.clientHandlers.FailFastOnce;
import com.liquidlabs.transport.proxy.Remotable;
import com.liquidlabs.vso.agent.ResourceAgent;

/**
 * Notification interface used by {@link ResourceAgent} to 
 * be aware work is being sent to them.
 */
public interface WorkListener extends Remotable {

	String getId();
	
	/**
	 * Called from the resourceContainer when an SLA request is being full-filled.
	 * @param resourceAddress
	 */
	void start(WorkAssignment workAssignment);
	
	@FailFastOnce
	void update(WorkAssignment workAssignment);

	/**
	 * Called by the SLA or client application when a resource is no longer needed.
	 * @param workAssignment
	 */
	@FailFastOnce
	void stop(WorkAssignment workAssignment);

}
