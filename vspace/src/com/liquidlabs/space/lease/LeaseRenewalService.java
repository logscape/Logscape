package com.liquidlabs.space.lease;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;


public class LeaseRenewalService implements LeaseRenewer {

	private final static Logger LOGGER = Logger.getLogger(LeaseRenewalService.class);
	private Map<String, ScheduledFuture<?>> leases = new ConcurrentHashMap<String, ScheduledFuture<?>>();
	private Leasor leaseManger;
	private final ScheduledExecutorService scheduler;
	private final CopyOnWriteArraySet<String> cancel = new CopyOnWriteArraySet<String>();
	boolean isActive = true;
	
	public LeaseRenewalService(Leasor leaseManger, ScheduledExecutorService scheduler) {
		this.leaseManger = leaseManger;
		this.scheduler = scheduler;
		
		Runtime.getRuntime().addShutdownHook(new Thread(){
			public void run() {
				isActive = false;
			}
		});

	}

	
	private void cleanUp(final String leaseKey) {
		cancel.add(leaseKey);
		leases.remove(leaseKey);
	}
	
	public void cancelLeaseRenewal(String leaseKey) {
		if (leaseKey == null) return;
		leaseManger.cancelLease(leaseKey);
		ScheduledFuture<?> scheduledFuture = leases.get(leaseKey);
		if (scheduledFuture != null) {
			scheduledFuture.cancel(false);
		}
		cleanUp(leaseKey);
	}

	public void add(final Renewer leaseThing, int renewal, final String leaseKey) {
		leases.put(leaseKey, scheduler.scheduleAtFixedRate(new Runnable() {
            public void run() {
                if (isActive && !cancel.contains(leaseKey)) {
                    leaseThing.renew();
                }
            }
        }, renewal, renewal, TimeUnit.SECONDS));
		
	}
    public void stop() {
        LOGGER.info("Stopping");
        for (ScheduledFuture<?> scheduledFuture : this.leases.values()) {
            try {
                scheduledFuture.cancel(true);
            } catch (RuntimeException ex) {
                LOGGER.warn(ex);
            }
        }
    }

}
