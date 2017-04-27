/**
 * 
 */
package com.liquidlabs.space.lease;

public interface Registrator {
	String register() throws Exception;
	String info();
	void registrationFailed(int failedCount);
}