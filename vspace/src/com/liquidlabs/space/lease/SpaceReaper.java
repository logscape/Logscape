package com.liquidlabs.space.lease;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.joda.time.DateTimeUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.space.Space;
import com.liquidlabs.space.lease.Leases.LeaseSpaceWriteLease;
import com.liquidlabs.space.lease.Leases.NotifyLease;
import com.liquidlabs.space.lease.Leases.TakeLease;
import com.liquidlabs.space.lease.Leases.UpdateLease;
import com.liquidlabs.space.lease.Leases.WriteLease;
import com.liquidlabs.transport.serialization.ObjectTranslator;

/**
 * Looks after Leasing and Notification handling
 *
 */
public class SpaceReaper implements Runnable, LifeCycle {
	
	private static final Logger LOGGER = Logger.getLogger(SpaceReaper.class);
	

	private final Space dataSpace;
	private final Space leasedSpace;
	Leases leases = new Leases();
	ObjectTranslator query = new ObjectTranslator();
	DateTimeFormatter timeFormat = DateTimeFormat.mediumTime();
	
	boolean flipflop;
	
	boolean started = false;
	private final String partition;

	private final ExecutorService executor;
	
	public SpaceReaper(String partition, Space dataSpace, Space leasedSpace) {
		this.partition = partition;
		this.dataSpace = dataSpace;
		this.leasedSpace = leasedSpace;
		this.executor = com.liquidlabs.common.concurrent.ExecutorService.newDynamicThreadPool("manager", partition + " reaper");
	}
	public void start() {
		started = true;
	}
	public void stop() {
		started = false;
	}
	public void run() {
		run(0);
	}
	LinkedBlockingQueue<Lease> quickLease = new LinkedBlockingQueue<Lease>();
	public void addImmediateLease(Lease lease) {
		quickLease.add(lease);
	}


	public void run(int depth ) {
		flipflop = !flipflop;
		
		handleQuickLeases();
		
		// go double time if there are lots of items in the leaseSpace
		if (flipflop && leasedSpace.size() < 100) return;
		
		if (!started) return;
		
		try {
			long now = getNowWithClockDrift();
			long nowSeconds = now / 1000;
			
			List<String> templates = new ArrayList<String>();
			String[] writeTemplate = query.getQueryStringTemplate(WriteLease.class, " timeoutSeconds <= " + nowSeconds);
			templates.add(writeTemplate[0]);
			String[] takenTemplate = query.getQueryStringTemplate(TakeLease.class, " timeoutSeconds <= " + nowSeconds);
			templates.add(takenTemplate[0]);
			String[] updateTemplate = query.getQueryStringTemplate(UpdateLease.class, " timeoutSeconds <= " + nowSeconds);
			templates.add(updateTemplate[0]);
			String[] notifyTemplate = query.getQueryStringTemplate(NotifyLease.class, " timeoutSeconds <= " + nowSeconds);
			templates.add(notifyTemplate[0]);
			String[] leaseWLease = query.getQueryStringTemplate(LeaseSpaceWriteLease.class, " timeoutSeconds <= " + nowSeconds);
			templates.add(leaseWLease[0]);
			
			String [] leaseItems = leasedSpace.takeMultiple(Arrays.toStringArray(templates), -1, -1, -1);
			
			for (String leaseString : leaseItems) {
				try {
					if (leaseString != null) {
						final Lease leaseItem = leases.getLeaseForValues(leaseString);
						if (leaseItem != null){
							executeLease(leaseItem, leaseItems.length > 100);
						}
					}
				} catch (Throwable t){
					LOGGER.warn("Failed to execute lease:"  + t.getMessage() + "\nLeaseString:" + leaseString, t);
				}
			}
		} catch (Throwable t){
			LOGGER.error("Reaping Error", t);
		}
	}
	private long getNowWithClockDrift() {
		long now = DateTimeUtils.currentTimeMillis();
		now = now + LeaseManagerImpl.CLOCK_DRIFT;
		return now;
	}
	private void handleQuickLeases() {
		while (this.quickLease.size() > 0) {
			executeLease(quickLease.poll(), this.quickLease.size() > 100);
		}
	}
	private void executeLease(final Lease leaseItem, boolean dispatch) {
		if (leaseItem == null) return;
		if (!leaseItem.getItemValue().startsWith("_EVENT_")){
			
//						if (LOGGER.isDebugEnabled()) LOGGER.debug(String.format("%s Now:%s ExecuteLease:%s Timeout:%s LeaseKey:%s ItemKey:%s DSpaceSize:%d LSpaceSize:%d", 
//								SLoggerConfig.VS_LEASE, timeFormat.print(nowSeconds*1000), leaseItem.leaseType, timeFormat.print(leaseItem.getTimeoutSeconds()*1000), 
//									leaseItem.getLeaseKey(), leaseItem.getItemKey(), dataSpace.size(), leasedSpace.size()));
		}
		if (leaseItem.getDuration() > 2 * 60) {
//						if (LOGGER.isInfoEnabled()) LOGGER.info(String.format("%s ExecuteLease:%s DSpaceSize:%d LSpaceSize:%d", 
//								if (LOGGER.isDebugEnabled()) 
								LOGGER.warn(String.format("%s ExecuteLease:%s DSpaceSize:%d LSpaceSize:%d", 
								"LOGGER", leaseItem, dataSpace.size(), leasedSpace.size()));
//								
//								Set<String> keySet = leasedSpace.keySet();
//								int i = 0;
//								for (String leaseKeyString : keySet) {
//									if (leaseKeyString.contains("_lease_")) {
//										final Lease aLeaseItem = leases.getLeaseForValues(leasedSpace.read(leaseKeyString));
//										LOGGER.info(i++ + ") " + leaseKeyString + " Exp:" + aLeaseItem.timeoutSecondsString);
//									}
//								}
		}
		
		if (dispatch) {
			executor.execute(new Runnable() { 
				public void run() {
					leaseItem.execute(dataSpace, leasedSpace);
					// release mem
					leaseItem.itemKey = null;
					leaseItem.itemValue = null;
				}
			});
		} else {
			leaseItem.execute(dataSpace, leasedSpace);
			// release mem
			leaseItem.itemKey = null;
			leaseItem.itemValue = null;

		}
	}	
}
