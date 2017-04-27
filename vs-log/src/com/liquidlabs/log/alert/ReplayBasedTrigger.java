package com.liquidlabs.log.alert;

import com.liquidlabs.admin.AdminSpace;
import com.liquidlabs.common.UID;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.ReplayEvent;
import com.liquidlabs.log.space.AggSpace;
import com.liquidlabs.log.space.LogSpace;
import com.liquidlabs.vso.SpaceService;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeUtils;
import org.joda.time.format.DateTimeFormat;

import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ReplayBasedTrigger implements Trigger {
	
	final static Logger LOGGER = Logger.getLogger(ReplayBasedTrigger.class);
	
	public String name;
	public String reportName;
	transient volatile int triggerCount = 0;
	
	public String lastEvents = "";
	public String lastTrigger = "";

	private String thisId;
	
	volatile public boolean firedOnceToPreventSpam;
	int trigger = 10;
	
	transient AdminSpace adminSpace;
	transient SpaceService spaceService;
	transient LogSpace logSpace;
	transient AggSpace aggSpace;
	transient String errorMsg;
	boolean verbose;

	// timeseries map of events leading to the trigger
	transient Map<Long, ReplayEvent> logEvents = new LinkedHashMap<Long, ReplayEvent>();

	transient private TriggerFiredCallBack triggerFiredCallBack;

	transient private final List<ScheduleHandler> scheduleHandlers;

	private final ScheduledExecutorService scheduler;

	public int getMaxReplays() {
		return Math.max(LogProperties.getMinAlertTrigger(),trigger * 2);
	}
	
	public ReplayBasedTrigger(boolean verbose, String name, String reportName, Schedule schedule, final int trigger, List<ScheduleHandler> scheduleHandlers, TriggerFiredCallBack firedCallBack, ScheduledExecutorService scheduler){
		this.verbose = verbose;
		this.name = name;
		this.scheduler = scheduler;
		thisId =  getClass().getSimpleName() + "_" + name + "_" + UID.getUUIDWithHostNameAndTime();
		this.reportName = reportName;
		this.trigger = trigger;
		this.scheduleHandlers = scheduleHandlers;
		triggerFiredCallBack = firedCallBack;
		if (verbose) LOGGER.info("Creating ReplayTrigger: count:" + trigger);

		if(trigger == 0){
			scheduler.schedule(new Runnable() {
				@Override
				public void run() {
					if(triggerCount == 0){
						ReplayEvent event = new ReplayEvent("zero-event-trigger", 0, 0, 0, "zero-event-trigger", 0L, "zero-event-trigger");
						fireTrigger(event);
					}
				}
			}, Integer.getInteger("alert.event.TimeThreshold",30), TimeUnit.SECONDS);
		}
	}
	public void attach(LogSpace logSpace, AggSpace aggSpace, SpaceService spaceService, AdminSpace adminSpace) {
		this.logSpace = logSpace;
		this.aggSpace = aggSpace;
		this.spaceService = spaceService;
		this.adminSpace = adminSpace;
	}
	
	public String getId() {
		return thisId;
	}
	public String toString() {
		return getId();
	}
	public boolean isReplayOnly() {
		return true;
	}

	public void handle(final ReplayEvent event) {
		try {
			if (triggerCount == 0) {
				LOGGER.debug("Received REPLAY:" + event.subscriber() + " Trigger:" + triggerCount);
			}

			if (firedOnceToPreventSpam) return;
			
			putReplayEvent(event);
			synchronized (this) {
					triggerCount++;
					// even if we hit the trigger we might wait for trailing event
					if (firedOnceToPreventSpam) return;
					if (triggerCount >= trigger) {
						if (LogProperties.getReplayAlertPauseSecs() > 0) {
							scheduler.schedule(new Runnable() {
								@Override
								public void run() {
									fireTrigger(event);
								}
							}, LogProperties.getReplayAlertPauseSecs(), TimeUnit.SECONDS);
						} else {
							fireTrigger(event);
						}
						
					}
			}
			lastEvents = getNowString();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	synchronized ReplayEvent putReplayEvent(ReplayEvent event) {
		if (firedOnceToPreventSpam) return event;
		if (this.verbose) LOGGER.info("RECEIVED: Replay:" + event.subscriber() + " Count:" + triggerCount + " Trigger:" + trigger);

		if (logEvents == null) {
			logEvents = Collections.synchronizedMap(new LinkedHashMap<Long, ReplayEvent>() {
				private static final long serialVersionUID = 1L;
				protected boolean removeEldestEntry(java.util.Map.Entry<Long, ReplayEvent> eldest) {
					return (this.size() > LogProperties.getMaxTriggerHeldItems());
				};
			});
		}
        // dont let is go boom - OOM
        if (logEvents.size() > 500) {
            if (trigger > 100 && logEvents.size() > 100) return event;
            if (logEvents.size() > trigger * 10) return event;
        }



		long time = event.getTime();
		while (logEvents.containsKey(time)) { time++; }
		return logEvents.put(time, event);
	}

	public void handle(Bucket event) {
	}

	public int handle(String providerId, String subscriber, int size,
                      Map<String, Object> histo) {
        return 1;
	}
	private String getNowString() {
		return DateTimeFormat.shortDateTime().print(DateTimeUtils.currentTimeMillis());
	}

	public int handle(List<ReplayEvent> events) {
		for (ReplayEvent event : events) {
			handle(event);
		}
		return 100;
	}

	public void handleSummary(Bucket bucketToSend) {
	}

	public int status(String provider, String subscriber, String msg) {
		LOGGER.info(">>>>>>>>>>>> STATUS:" + subscriber + " MSG:" + msg);
        return 1;
	}
	public void fireTrigger(ReplayEvent event) {
		if (firedOnceToPreventSpam) return;
		if (verbose) LOGGER.info("FIRING TRIGGER:" + thisId + " count:" + triggerCount);
		firedOnceToPreventSpam = true;
		LOGGER.info(String.format(" %s Schedule:%s TRIGGERED:%d ThresholdPassed:%d", Schedule.TAG, name, triggerCount, trigger));
		lastTrigger= getNowString();
		
		List<ReplayEvent> replays = new ArrayList<ReplayEvent>(this.logEvents.values());
		// sort the leading events
		Collections.sort(replays, new Comparator<ReplayEvent>(){
			public int compare(ReplayEvent o1, ReplayEvent o2) {
				return Long.valueOf(o1.getTime()).compareTo(o2.getTime());
			}
			
		});
		List<String> leadingEvents = new ArrayList<String>();
		for (ReplayEvent replayEvent : replays) {
			leadingEvents.add(replayEvent.getRawData());
		}
		

        LOGGER.info(String.format(" %s Schedule:%s Handlers:%d", Schedule.TAG, name, scheduleHandlers.size()));
		for (ScheduleHandler handler : this.scheduleHandlers) {
			handler.handle(event, logEvents, trigger, triggerCount);
		}
        if (triggerFiredCallBack != null) triggerFiredCallBack.fired(leadingEvents);
		
		this.logEvents.clear();
		triggerCount = 0;
	}
	
	public void stop() {
		if (logEvents != null) logEvents.clear();
	}
}
