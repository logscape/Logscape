package com.liquidlabs.vso.resource;

import java.util.List;

import com.liquidlabs.transport.proxy.Remotable;

public interface AllocListener extends Remotable {
	
	/**
	 * Called (By resourceSpace) when resource Added to this owner
	 * @param requestId TODO
	 * @param resourceIds
	 * @param owner TODO
	 * @param priority TODO
	 */
	public void add(String requestId, List<String> resourceIds, String owner, int priority);
	
	/**
	 * Called by self to remove resources when no longer required 
	 * @param requestId TODO
	 * @param owner TODO
	 * @param resourceIds
	 */
	public void take(String requestId, String owner, List<String> resourceIds);
	
	/**
	 * Called (By Resourcespace) when being asked to release resource - NEVER release items before returning the list of items 
	 * that will be released
	 * @param requestId TODO
	 * @param resourceIds
	 * @param releaseCount TODO
	 */
	public List<String> release(String requestId, List<String> resourceIds, int releaseCount);

	public void pending(String requestId, List<String> resourceIds, String owner, int priority);

	public void satisfied(String requestId, String owner, List<String> resourceIds);

}
