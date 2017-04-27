package com.liquidlabs.space.lease;

import org.apache.log4j.Logger;

import com.liquidlabs.space.VSpaceProperties;

public class Renewer {

	private final Leasor leasor;
	private final Registrator registrator;
	private String lease;
    private int expiry;
    private final String name;
    
    public static boolean makeMeBreak = false;
    private int failedCount = 0;
    
    
    boolean trouble = false;
	private final Logger LOGGER;
    
	public Renewer(Leasor leasor, Registrator registrator, String lease, int expiry, String name, Logger LOGGER) {
		this.leasor = leasor;
		this.registrator = registrator;
		this.lease = lease;
		this.expiry = expiry;
		this.name = name;
		this.LOGGER = LOGGER;
	}
	
	public void renew() {
		try {
			if (makeMeBreak) throw new RuntimeException("PreventLEASE-BOOM!");
			leasor.renewLease(lease, expiry);
		} catch (Throwable t) {
			trouble = true;
			LOGGER.warn(String.format("[%s] Failed to renew lease %s for %s. Trying to re-register", name, lease, name));
			register();
		}
	}

	public void register() {
		try {
			if (makeMeBreak) throw new RuntimeException("PreventREGISTERLEASE-BOOM!");
			
			if (trouble) LOGGER.info("Trying to Register new instance of:" + lease);
			lease = registrator.register();
			if (trouble == true) {
				trouble = false;
				failedCount = 0;
				LOGGER.info("Successfully Re-Registered:" + lease);
			}
		} catch (Throwable t) {
			trouble = true;
			LOGGER.warn(String.format("[%s] Failed to re-register item against[%s] for %s", name, registrator.info(), t.toString()), t);
			try {
				failedCount++;
				Thread.sleep(VSpaceProperties.getLeaseRegisterFailureWaitSeconds() * 1000);
				if (failedCount > 20) {
					registrator.registrationFailed(failedCount);
				}
			} catch (InterruptedException e) {
			}
		}
	}
	public int getFailedCount() {
		return failedCount;
	}
}
