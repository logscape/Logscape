package com.liquidlabs.log.alert.correlate;

import com.liquidlabs.log.alert.ScheduleHandler;
import com.liquidlabs.log.alert.TriggerFiredCallBack;
import com.liquidlabs.log.fields.FieldSet;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class CorrEventFeedAssembler {
	private final static Logger LOGGER = Logger.getLogger(CorrEventFeedAssembler.class);

	private final ScheduledExecutorService scheduler;

	private final List<ScheduleHandler> handlers;

	private final TriggerFiredCallBack firedCallBack;

	public CorrEventFeedAssembler(ScheduledExecutorService scheduler, List<ScheduleHandler> handlers, TriggerFiredCallBack firedCallBack) {
		this.scheduler = scheduler;
		this.handlers = handlers;
		this.firedCallBack = firedCallBack;
	}
	
	class AllWindows implements Window {
		Map<String, Window> windows = new HashMap<String, Window>();
		private Window defaultWindow;

		public AllWindows(Window window) {
			this.defaultWindow = window;
		}
		
		@Override
		public Window eventReceived(Event event) {
			String key = event.getKey();
			if (key == null) {
				defaultWindow = defaultWindow.eventReceived(event);
			} else {
				if (!windows.containsKey(key)) {
					windows.put(key, defaultWindow.copy());
				}
				Window window = windows.get(key);
				windows.put(key, window.eventReceived(event));
			}
			return this;
		}
		
		@Override
		public Window copy() {
			throw new UnsupportedOperationException("Not Implemented Here!");
		}
		
	
	}

	public CorrEventFeed createTrigger(int fireNextInterval, Window window, final String id, Map<String, String> ruleMap, List<FieldSet> fieldSet) {
		String field = ruleMap.get("field");
		String keyField = ruleMap.get("key");
		final CorrEventFeed eventFeed = new CorrEventFeed(id, window, fieldSet, field, keyField);
		ScheduledFuture<?> task = scheduler.scheduleAtFixedRate(new Runnable() {
			public void run() {
				try {
					eventFeed.fireNext();
				} catch (Throwable t) {
					LOGGER.warn("FireNextFailed", t);
				}
			}
		}, fireNextInterval, fireNextInterval, TimeUnit.SECONDS);
		eventFeed.setTask(task);
		return eventFeed;
	}

	public CorrEventFeed createTrigger(String alertName, int intervalSeconds, String rule, String subscriber, List<FieldSet> fieldSets) {
		Map<String, String> ruleMap = convertCorrelationTriggerToMap(rule);
		Window window = createWindow(alertName, rule, ruleMap);
		return createTrigger(intervalSeconds, window, subscriber, ruleMap, fieldSets);

	}

	private Window createWindow(String alertName, String rule, Map<String, String> ruleMap) {
		long windowSize = Long.valueOf(ruleMap.get("time"));
        String type = ruleMap.get("type");
        String[] corrFieldValues = ruleMap.get("sequence").split(",");

        if (type == null || type.equals("sequence")) {
        	 LOGGER.info("Creating Window For SequenceRule");
		    return new AllWindows(new SlidingWindow(new SequenceRule(corrFieldValues), System.currentTimeMillis(), windowSize, new HellRaiser(alertName, handlers, firedCallBack)));
        }

        LOGGER.info("Creating Window For AveragingRule");
        return new AllWindows(new CopyOfSlidingWindow(new AveragingRule(Integer.valueOf(corrFieldValues[0])), System.currentTimeMillis(),
                windowSize, new HellRaiser(alertName, handlers, firedCallBack)));
	}

	public static Map<String, String> convertCorrelationTriggerToMap(String rule) {
		String[] split = rule.split(" ");
		HashMap<String, String> result = new HashMap<String, String>();
		for (String arg : split) {
			String[] kv = arg.split(":");
			if (kv.length != 2) continue;
			result.put(kv[0], kv[1]);
		}
		return result;
	}

}
