package com.liquidlabs.log.alert.correlate;

import com.liquidlabs.admin.AdminSpace;
import com.liquidlabs.log.alert.Trigger;
import com.liquidlabs.log.alert.TriggerFiredCallBack;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.ReplayEvent;
import com.liquidlabs.log.space.AggSpace;
import com.liquidlabs.log.space.LogSpace;
import com.liquidlabs.vso.SpaceService;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ScheduledFuture;

public class CorrEventFeed implements Trigger {
	private final static Logger LOGGER = Logger.getLogger(CorrEventFeed.class);
	
	private Window window;
	private List<ReplayEvent> currentBucket = Collections.synchronizedList(new ArrayList<ReplayEvent>());
	private final Map<String, FieldSet> fieldSets;
	private String fieldName;
	// note: id == subsriber id
	private final String id;
	private final String keyField;
	private ScheduledFuture<?> task;
	private TriggerFiredCallBack triggerFiredCallBack;

	public CorrEventFeed(String id, Window window, List<FieldSet> fieldSet, String fieldName, String keyField) {
		this.id = id;
		this.window = window;
		fieldSets = new HashMap<String,FieldSet>();
		for (FieldSet fieldSet2 : fieldSet) {
			fieldSets.put(fieldSet2.id, fieldSet2);
		}
		this.fieldName = fieldName;
		this.keyField = keyField;
	}
	public int getMaxReplays() {
		return 1024;
	}

	public void attach(LogSpace logSpace, AggSpace aggSpace, SpaceService spaceService, AdminSpace adminSpace) {
	}

	public void stop() {
		if (task != null) task.cancel(false);
	}

	public void fireTrigger(ReplayEvent replayEvent) {
	}

	public boolean isReplayOnly() {
		return true;
	}

	public String getId() {
		return id;
	}

	public void handle(ReplayEvent event) {
		if (!event.subscriber().equals(this.id)){
			LOGGER.warn("CORR Received Invalid Replay: sub:" + this.id + " recv:"+ event.subscriber());
			return;
		}

        FieldSet fieldSet = this.fieldSets.get(event.fieldSetId());
        if (fieldSet != null) {
            // prime the discovered fields
            fieldSet.getFields(event.getRawData(), -1, -1, event.getTime());
        } else {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Missing FieldSet:" + event.fieldSetId());
        }

        currentBucket.add(event);
		if (currentBucket.size() > 10 * 1024) {
			fireNext();
		}
	}

	public void handle(Bucket event) {
	}

	public int handle(String providerId, String subscriber, int size, Map<String, Object> histo) {
		  return 1;
	}

	public int handle(List<ReplayEvent> events) {
		for (ReplayEvent replayEvent : events) {
			handle(replayEvent);
		}
		return 100;
	}

	public void handleSummary(Bucket bucketToSend) {
	}

	public int status(String provider, String subscriber, String msg) {
		return 1;
	}

	public void fireNext() {
		ArrayList<ReplayEvent> copyBucket = new ArrayList<ReplayEvent>(currentBucket);
		currentBucket.clear();
		Collections.sort(copyBucket, new Comparator<ReplayEvent>() {
			@Override
			public int compare(ReplayEvent o1, ReplayEvent o2) {
				return (int) (o1.getTime() - o2.getTime());
			}
		});
        if (LOGGER.isDebugEnabled()) LOGGER.debug("fireNext >> eventCount:" + copyBucket.size());

		for (ReplayEvent event : copyBucket) {
			try {
				FieldSet fieldSet = this.fieldSets.get(event.fieldSetId());
				if (fieldSet != null) {
                    Event event1 = new Event(event, fieldSet, fieldName, keyField);
                    if (LOGGER.isDebugEnabled()) LOGGER.debug("EVAL:" + event1);
                    window = window.eventReceived(event1);
                } else {
                    if (LOGGER.isDebugEnabled()) LOGGER.debug("MissingFieldSet for:" + event.fieldSetId());
                }
			} catch (Throwable t) {
				LOGGER.warn("Ignoring Event:" + event + " ex:" + t.toString(), t);
			}
		}
        if (LOGGER.isDebugEnabled()) LOGGER.debug("fireNext <<");
    }

	public void setTask(ScheduledFuture<?> task) {
		this.task = task;
	}
	public void addFiredCallBack(TriggerFiredCallBack triggerFiredCallBack) {
		this.triggerFiredCallBack = triggerFiredCallBack;
	}
	public String fieldName() {
		return this.fieldName;
	}
	public String keyField() {
		return this.keyField;
	}

}
