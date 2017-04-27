package com.liquidlabs.vso.resource;

import com.liquidlabs.transport.proxy.clientHandlers.FailFastAndDisable;
import com.liquidlabs.transport.proxy.Remotable;
import com.liquidlabs.vso.agent.ResourceProfile;

/**
 * * Implementers must provide a toString as follows in order for a remote proxy event to be returned.
 * <code>
 * 	public String toString(){<br>
 * 		return "proxy-" + ResourceAgent.NAME + "-" + resourceProfile.getEndPoint();
 * }
 * </code>
 *
 */
public interface ResourceRegisterListener extends Remotable {
	
	String getId();

	/**
	 * Called from the resourceContainer when an SLA request is being full-filled.
	 * @param resourceProfile 
	 * @param requestId 
	 * @param resourceAddress
	 */
	@FailFastAndDisable
	void register(String resourceId, ResourceProfile resourceProfile);

	/**
	 * Called by the SLA or client application when a resource is no longer needed.
	 * @param resourceProfile 
	 * @param requestId 
	 * @param resourceName
	 */
	@FailFastAndDisable
	void unregister(String resourceId, ResourceProfile resourceProfile);

}
