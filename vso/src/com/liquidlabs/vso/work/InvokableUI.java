package com.liquidlabs.vso.work;

public interface InvokableUI {
	
	/**
	 * Remove Invocation UID to identify this instance
	 * @return
	 */
	public String getUID();
	
	/**
	 *  XML Descriptor to allow for remote rendering and GUI invocations - use when registering with ServiceInfo against LUService
	 * @return
	 */
	public String getUI();

}
