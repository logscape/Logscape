package com.liquidlabs.space;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.liquidlabs.common.LifeCycle.State;
import com.liquidlabs.space.lease.Renewer;

public class ClientLeaseManager {

	private ScheduledExecutorService scheduler;
	State state = State.RUNNING;

	public ClientLeaseManager(){}
	public ClientLeaseManager(ScheduledExecutorService scheduler) {
		this.scheduler = scheduler;
	}
	
	public void setScheduler(ScheduledExecutorService scheduler) {
		this.scheduler = scheduler;
		
	}

	/**
	 * Use something like 
	 * <code>
	 * manage(new Runnable() {
	 *    public void run() { 
	 *        space.renewLease(leaseKey, period 
	 *    }
	 * <code>
	 * @param leaseRunner
	 * @param renewInterval
	 */
	public void manage(final Runnable leaseRunner, final int renewInterval) {
		scheduler.scheduleAtFixedRate(leaseRunner, 10, renewInterval, TimeUnit.SECONDS);
	}
	
	public void manage(final Renewer leaseThing,  int renewal) {
		scheduler.scheduleAtFixedRate(new Runnable() {
			public void run() {
				if (state != State.RUNNING) return;
				leaseThing.renew();
			}}, renewal, renewal, TimeUnit.SECONDS);
	}
	public void stop() {
		this.state = State.STOPPED;
		
	}

}
