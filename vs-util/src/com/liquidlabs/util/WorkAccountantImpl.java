package com.liquidlabs.util;


import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.joda.time.DateTimeUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.UID;
import com.liquidlabs.common.plot.XYPoint;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.vso.SpaceService;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.LookupSpaceImpl;
import com.liquidlabs.vso.work.WorkAllocator;
import com.liquidlabs.vso.work.WorkAllocatorImpl;
import com.liquidlabs.vso.work.WorkAssignment;
import com.liquidlabs.vso.work.WorkListener;

public class WorkAccountantImpl implements WorkAccountant {
	
	public static String TAG = "UTIL";
	private final static Logger LOGGER = Logger.getLogger(WorkAccountant.class);
	
	DateTimeFormatter mediumDate = DateTimeFormat.mediumDate();
	DateTimeFormatter dateHourFormat = DateTimeFormat.forPattern("yy-MM-dd HH:00");
	DateTimeFormatter dateHourMinFormat = DateTimeFormat.forPattern("yy-MM-dd HH:mm");
	
	
	private final WorkAllocator workAllocator;

	private final EventStore eventStore;
	private final SpaceService spaceService;
	private final ProxyFactory proxyFactory;
	private String id;
	
	
	public WorkAccountantImpl(SpaceService spaceService, WorkAllocator workAllocator, EventStore eventStore, ProxyFactory proxyFactory) {
		this.spaceService = spaceService;
		this.workAllocator = workAllocator;
		this.eventStore = eventStore;
		this.proxyFactory = proxyFactory;
		this.id = NAME + UID.getSimpleUID("");;
	}
	
	public String getId() {
		return id;
	}
	public void start() {
		LOGGER.info("Starting");
		try {
			spaceService.start(this, "vs-util-1.0");
			
			loadExistingAllocations(workAllocator);
			// TODO: Potential race state here
			setupWorkListener(this, getId(), workAllocator, proxyFactory.getScheduler());

		} catch (Throwable t){
			LOGGER.error("Failed to start", t);
		}
	}


	public void stop() {
		try {
			LOGGER.info("Stopping");
			spaceService.stop();
		} catch (Throwable t) {
			LOGGER.error("Failed to stop", t);
		}
	}
	
	public void update(WorkAssignment workAssignment) {
		start(workAssignment);
	}
	
	synchronized public void start(WorkAssignment workAssignment) {
		if (workAssignment.getCostPerUnit() == 0.0) return;
		if (!workAssignment.getStatus().equals(LifeCycle.State.RUNNING)) return;
		
		AccountEvent accountEvent = new AccountEvent(workAssignment.getBundleId(), workAssignment.getPriority(), workAssignment.getServiceName(), workAssignment.getResourceId(), DateTimeUtils.currentTimeMillis(), workAssignment.getCostPerUnit());		
		startAccountEvent(accountEvent);
	}

	synchronized public void stop(WorkAssignment workAssignment) {
		if (workAssignment.getCostPerUnit() == 0.0) return;
		AccountEvent event = new AccountEvent(workAssignment.getBundleId(), workAssignment.getPriority(), workAssignment.getServiceName(), workAssignment.getResourceId(), DateTimeUtils.currentTimeMillis(), workAssignment.getCostPerUnit());
		stopAccountEvent(event);
	}
	
	/**
	 * Start tracking a resource
	 */
	public void startAccountEvent(AccountEvent event) {
		if (this.spaceService.containsKey(AccountEvent.class, event.UID)) return;
		if (LOGGER.isDebugEnabled()) LOGGER.debug(String.format("%s Start tracking:%s app:%s resource:%s", TAG, event.getUID(), event.getBundleId(), event.getResourceId()));
		this.spaceService.store(event, -1);
	}

	/**
	 * Stop tracking and move to the eventStore
	 */
	public void stopAccountEvent(AccountEvent templateEvent) {
		stopAccountEvent(templateEvent, DateTimeUtils.currentTimeMillis());
	}
	public void stopAccountEvent(AccountEvent templateEvent, long endTime) {
		if (LOGGER.isDebugEnabled()) LOGGER.debug(String.format("%s Stop tracking:%s app:%s resource:%s", TAG, templateEvent.getUID(), templateEvent.getBundleId(),templateEvent.getResourceId()));
		
		// find all outstanding items and add the cost to the account holder
//		AccountEvent event = spaceService.findById(AccountEvent.class, templateEvent.getUID());
		String format = String.format("bundleId equals %s AND serviceName equals %s AND resourceId equals %s", templateEvent.getBundleId(), templateEvent.getServiceName(), templateEvent.getResourceId());
		List<AccountEvent> findObjects2 = spaceService.findObjects(AccountEvent.class, format, false, 10);
		if (findObjects2.size() == 0) {
			LOGGER.warn("Ignoring - Failed to find AccountEvent:" + templateEvent.getUID());
			return;
		}
//		if (findObjects2.size() > 1) {
//			LOGGER.warn("Ignoring - Failed multiple events:" + findObjects2);
//			return;
//		}
		for (AccountEvent event : findObjects2) {
			event.stop(endTime);
			eventStore.store(event);
			spaceService.remove(AccountEvent.class, event.getId());
			LOGGER.debug("Removed completed event:UID:" + templateEvent.getId());
		}
	}
	
	public List<XYPoint> getServiceAllocationHistory(String appNameId, String serviceName, long fromTimeMs, long toTimeMs, int intervalSeconds) {
		LOGGER.debug(TAG + " getServiceAllocationHistory:" + appNameId + " service:" + serviceName);
		List<XYPoint> completedEvents = eventStore.retrieveEventsCountFrom(appNameId, serviceName, fromTimeMs, toTimeMs, intervalSeconds * 1000);
		
		TreeMap<Long, XYPoint> workingMap = new TreeMap<Long, XYPoint>();
		for (XYPoint accountEvent : completedEvents) {
			workingMap.put(accountEvent.getTimeMs(), accountEvent);
		}
		
		// now accumulate live items (no endTime)
		String startOverlap = "bundleId equals %s AND serviceName equals %s AND startTime <= %d AND endTime == 0";
		List<AccountEvent> liveEvents = spaceService.findObjects(AccountEvent.class, String.format(startOverlap, appNameId, serviceName, toTimeMs), false, -1);
		List<XYPoint> countedEvents = eventStore.count(fromTimeMs, toTimeMs, intervalSeconds * 1000, liveEvents, workingMap);
		LOGGER.debug(TAG + " getServiceAllocationHistory:" + " count:" + countedEvents.size());
		return countedEvents;
	}
	
	public List<AccountStatement> getCostsForTime(String appNameId, int intervalSeconds, long fromTimeMs, long toTimeMs, List<String> serviceIds, List<Double> costPerUnit) {
		
		List<AccountStatement> result = new ArrayList<AccountStatement>();
		boolean isCreating = true;
		int i = 0;
		for (String serviceId : serviceIds) {
			List<XYPoint> costAtTime = getServiceUtilHistory(appNameId, serviceId, fromTimeMs, toTimeMs, intervalSeconds, costPerUnit.get(i), false);
			List<XYPoint> countAtTime = getServiceAllocationHistory(appNameId, serviceId, fromTimeMs, toTimeMs, intervalSeconds);
			int j = 0;
			for (XYPoint point : costAtTime) {
				if (isCreating) {
					AccountStatement accountStatement = new AccountStatement(appNameId, point.getTimeMs(), (int)point.getY(), 0, 0);
					accountStatement.setCost(serviceId, point.getY());
					result.add(accountStatement);
				} else { 
					AccountStatement accountStatement = result.get(j);
					accountStatement.setCompletedUnitCosts((int) (accountStatement.getCompletedUnitCosts() + point.getY()));
					accountStatement.setCost(serviceId, point.getY());
				}
				result.get(j).setCount(serviceId, countAtTime.get(j).getY());
				j++;
			}
			isCreating = false;
		}
		
		
		return result;
	}

	
	
	public List<XYPoint> getServiceUtilHistory(String appNameId, String serviceName, long fromTimeMs, long toTimeMs, int intervalSeconds, double costPerUnit, boolean returnMsInsteadOfCost) {
		
		LOGGER.debug(TAG + " getServiceUtilHistory:" + appNameId  + " service:" + serviceName);
		
		List<XYPoint> completedEvents = eventStore.retrieveAccumulatedEventsFrom(appNameId, serviceName, fromTimeMs, toTimeMs, intervalSeconds * 1000);
		TreeMap<Long, XYPoint> workingMap = new TreeMap<Long, XYPoint>();
		for (XYPoint accountEvent : completedEvents) {
			workingMap.put(accountEvent.getTimeMs(), accountEvent);
		}
		// now accumulate live items (no endTime)
		String startOverlap = "bundleId equals %s AND serviceName equals %s AND startTime <= %d";
		if (serviceName == null || serviceName.length() == 0) startOverlap = "bundleId equals %s AND  startTime <= %d";
		List<AccountEvent> liveEvents = spaceService.findObjects(AccountEvent.class, String.format(startOverlap, appNameId, serviceName, toTimeMs), false, -1);
		List<XYPoint> accumlatedEvents = eventStore.accumulate(fromTimeMs, toTimeMs, intervalSeconds * 1000, liveEvents, workingMap);
		for (XYPoint accountEvent : accumlatedEvents) {
			if (!returnMsInsteadOfCost) { 
				accountEvent.setY(Double.valueOf(calcCost(accountEvent.getY(), costPerUnit)).longValue());
			}
		}
		LOGGER.debug(TAG + " getServiceUtilHistory count:" + accumlatedEvents.size());
		return accumlatedEvents;
	}

	/**
	 * Needs to be pluggable and include other params, for now use CPU-Time - if it were time aware cost would be accumulated
	 * in the previous steps
	 */
	double calcCost(long eventTimeMs, double unitCostPerHour) {
		double oneMinute = 60 * 1000;
		double oneHour = 60 * oneMinute;
		return ((eventTimeMs/oneHour) * unitCostPerHour);
	}

	/**
	 * Occurs when a resource was returned to the pool, so capture the info and warehouse it
	 */
	public void updateAccount(String bundleId, int priority, long startTime, long endTime) {
		LOGGER.info("UpdateAccount:" + bundleId);
	}
	
	/**
	 * Return costs associated with this bundle
	 * TODO: roll account events to daily events
	 */
	public AccountStatement getCurrentUnitCost(String bundleId){
		String query = "bundleId equals " + bundleId;
		if (bundleId == null || bundleId.length() == 0) {
			query = "";
		}
		List<DailyAccount> completedAccounts = spaceService.findObjects(DailyAccount.class, query, false, 1);
		int completedUnitCosts = 0;
		for (DailyAccount dailyAccount : completedAccounts) {
			completedUnitCosts += dailyAccount.getUnits();
		}
		
		AccountStatement accountStatement = getAsOfNowCosts(bundleId, completedUnitCosts);
		
		return accountStatement;
	}


	private AccountStatement getAsOfNowCosts(String bundleId, int completedUnitCosts) {
		double runningUnitCosts = 0;
		String query = "bundleId equals " + bundleId;
		if (bundleId == null || bundleId.length() == 0) query = "";
		List<AccountEvent> runningAccounts = spaceService.findObjects(AccountEvent.class, query, false, -1);
		for (AccountEvent accountEvent : runningAccounts) {
			long startTime = accountEvent.getStartTime();
			long endTime = accountEvent.getEndTime();
			if (endTime == 0) endTime = DateTimeUtils.currentTimeMillis();
			runningUnitCosts += calcCost(endTime - startTime, accountEvent.getCostUnit());
		}
		
		return new AccountStatement(bundleId, DateTimeUtils.currentTimeMillis(), completedUnitCosts, runningAccounts.size(), (int) Double.valueOf(runningUnitCosts).longValue());
	}
	public static void boot(String lookupAddress) throws URISyntaxException{
		LOGGER.info("Starting");
		
		ORMapperFactory mapperFactory = LookupSpaceImpl.getSharedMapperFactory();
		
		LookupSpace lookupSpace = LookupSpaceImpl.getRemoteService(lookupAddress, mapperFactory.getProxyFactory(),"WorkAccBoot");
		WorkAllocator workAllocator = WorkAllocatorImpl.getRemoteService("workAccBoot", lookupSpace,  mapperFactory.getProxyFactory());
		
		EventStoreImpl eventStore = new EventStoreImpl(mapperFactory, true, true);
		
		SpaceServiceImpl spaceService = new SpaceServiceImpl(lookupSpace, mapperFactory, NAME, mapperFactory.getScheduler(), true, false, true);
		WorkAccountant workAccount = new WorkAccountantImpl(spaceService, workAllocator, eventStore, mapperFactory.getProxyFactory());
		mapperFactory.getProxyFactory().registerMethodReceiver(WorkAccountant.NAME, workAccount);
		workAccount.start();
	}
	public static WorkAccountant getRemoteService(String whoAmI, LookupSpace lookup, ProxyFactory proxyFactory) {
		return SpaceServiceImpl.getRemoteService(whoAmI, WorkAccountant.class, lookup, proxyFactory, WorkAccountant.NAME, false, false);
	}
	
	private void loadExistingAllocations(final WorkAllocator workAllocator) {
		String[] workIdsForQuery = workAllocator.getWorkIdsForQuery("costPerUnit > 0 AND status equals " + LifeCycle.State.RUNNING);
		for (String workId : workIdsForQuery) {
			List<WorkAssignment> work = workAllocator.getWorkAssignmentsForQuery("id equals "+ workId);
			for (WorkAssignment workAssignment : work) {
				start(workAssignment);
			}			
		}
	}
	private void setupWorkListener(final WorkListener workListener, String listenerId, final WorkAllocator workAllocator, ScheduledExecutorService scheduler) {
		final String workListenerId = listenerId + "_WRK_LSTNR";
        
        scheduler.scheduleAtFixedRate(new Runnable(){
        	public void run() {
        		try {
					workAllocator.registerWorkListener(workListener, "", workListenerId, false);
				} catch (Exception e) {
					LOGGER.fatal(e);
				}
        	}
        }, 1, VSOProperties.getLUSpaceServiceRenewInterval(), TimeUnit.SECONDS);
        
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run() {
                try {
                    workAllocator.unregisterWorkListener(workListenerId);
                    Thread.sleep(20);
                } catch (Throwable t){}
            }
        });
	}

}
