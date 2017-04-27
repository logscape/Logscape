package com.liquidlabs.log.alert;

import com.liquidlabs.admin.AdminSpace;
import com.liquidlabs.common.UID;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.log.LogRequestBuilder;
import com.liquidlabs.log.search.*;
import com.liquidlabs.log.search.filters.Filter;
import com.liquidlabs.log.space.*;
import com.liquidlabs.log.space.agg.ClientHistoItem;
import com.liquidlabs.log.space.agg.ClientHistoItem.SeriesValue;
import com.liquidlabs.log.space.agg.HistogramHandler;
import com.liquidlabs.transport.serialization.Convertor;
import com.liquidlabs.vso.SpaceService;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.*;

public class ExpressionTrigger implements Trigger {
	
	private final static Logger LOGGER = Logger.getLogger(ExpressionTrigger.class);
    private final ScheduledFuture<?> scheduledEvent;

    public String name;
	public String reportName;
	public transient int triggerCount = 0;
	
	transient String thisId;
	
	public boolean firedOnceToPreventSpam;
	String trigger = "";
	
	private LogRequest request;

	transient AdminSpace adminSpace;
	transient SpaceService spaceService;
	transient LogSpace logSpace;
	transient AggSpace aggSpace;
	transient String errorMsg;

	// timeseries map of events leading to the trigger
	public transient Map<Long, ReplayEvent> logEvents = Collections.synchronizedMap(new LinkedHashMap<Long, ReplayEvent>() {
            private static final long serialVersionUID = 1L;
            protected boolean removeEldestEntry(Map.Entry<Long, ReplayEvent> eldest) {
                return (this.size() > 1000);
            };
        });;
	transient Map<String, LogRequest> requests = new ConcurrentHashMap<String, LogRequest>();

	transient private HistogramHandler histogramHandler;

	public int bucketsReceived;

	private ExpressionEvaluator triggerEvaluator;
	
	private boolean verbose;

	transient private TriggerFiredCallBack triggerFiredCallBack;

	transient private final List<ScheduleHandler> scheduleHandlers;
	boolean fired = false;

    public ExpressionTrigger(LogRequest logRequest, String name, String reportName, Schedule schedule, String trigger, List<ScheduleHandler> scheduleHandlers, TriggerFiredCallBack firedCallBack, ScheduledExecutorService scheduler){
		this.request = logRequest;
		this.scheduleHandlers = scheduleHandlers;
		triggerFiredCallBack = firedCallBack;
		this.verbose = logRequest.isVerbose();
		this.name = name;
		this.reportName = reportName;
		this.trigger = trigger;
		this.triggerEvaluator = new ExpressionEvaluator(trigger);

		
		if (logRequest.isVerbose()) LOGGER.info("Creating HistoTrigger: trigger:" + trigger + " Subscriber:" + logRequest.subscriber());
		requests.put(logRequest.subscriber(), logRequest);
		
		if (logRequest.getBucketCount() == 1) {
			logRequest.setBucketCount(60);
		}


        scheduledEvent = scheduler.schedule(runLiveScrolling(scheduler, logRequest.subscriber(), logRequest), 2, TimeUnit.SECONDS);
    }
	
	private Runnable runLiveScrolling(final ScheduledExecutorService scheduler, final String subscriber, final LogRequest logRequest) {
		final Runnable task = new Runnable() {

			public void run() {
				if (request.isExpired() || request.isCancelled() || fired) return;
				try {
					List<Map<String, Bucket>> aggHist = histogramHandler.getHisto();
                    if (aggHist == null) {
                        LOGGER.warn("Cannot scroll Histo:" + subscriber);
                        return;
                    }
					
					List<Map<String, Bucket>> createdHisto = new ArrayList<Map<String,Bucket>>();
					Map<String, Bucket> lastItem = aggHist.get(aggHist.size()-1);
					String key = lastItem.keySet().iterator().next();
					Bucket bucket = lastItem.get(key);
					try {
						Bucket cloned = (Bucket) Convertor.deserialize(Convertor.serialize(bucket));
						long time = System.currentTimeMillis();
						cloned.setTimes(time-10, time);
						cloned.resetAll();
						if (cloned.hits() == 0) cloned.increment();
						Map<String, Bucket> aggEntry = new HashMap<String, Bucket>();
						aggEntry.put(key, cloned);
						createdHisto.add(aggEntry);
					} catch (Exception e) {
						e.printStackTrace();
					}
					
					histogramHandler.handle("providerId-sched", subscriber, 1, createdHisto);
					scheduler.schedule(runLiveScrolling(scheduler, subscriber, logRequest), 2, TimeUnit.SECONDS);
				} catch (Throwable t) {
					LOGGER.warn("Failed to pump:" + subscriber + " Ex:" + t,t);
				}
			}
		};
		return task;		
	}

	public int status(String provider, String subscriber, String msg) {
		if (verbose) {
			LOGGER.info("LOGGER " + request.subscriber() + " Status:" + msg);
		}
		return 1;
	}

	private void evaluateTrigger(String subscriber, List<ClientHistoItem> histo) {
		if (verbose) {
			LOGGER.info("CHECKING TRIGGER:" + histo + " Series:"+ histo.get(0).meta.allSeriesNames);
			LOGGER.info("Evaluator:" + this.triggerEvaluator);
		}
		// check the client histo to see if we can fire
		List<ReplayEvent> allItems = new ArrayList<ReplayEvent>();

		for (ClientHistoItem clientHistoItem : histo) {
			Collection<SeriesValue> values = clientHistoItem.series.values();
			if (values.size() > 0) {
				String item = clientHistoItem.time + " "  ;
				for (SeriesValue seriesValue : values) {
					item +=  seriesValue.label + ":" + seriesValue.value + "";
				}
                ReplayEvent event = new ReplayEvent("ExpressionTrigger", 1, 1, 1, subscriber,  clientHistoItem.timeMs, item);
                event.setDefaultFieldValues("", "","","","", "","","");
                allItems.add(event);
			}
		}
		
        boolean triggered = triggerEvaluator.isTriggered(histo);
        if (triggered && !firedOnceToPreventSpam) {
            firedOnceToPreventSpam = true;

            LOGGER.info("TRIGGER is FIRING KeyMatch:" + trigger + " TriggerCount:" + this.triggerCount);

            this.logEvents.clear();
            long tt = System.currentTimeMillis();
			for (ReplayEvent replayEvent1 : allItems) {
				this.logEvents.put(tt++, replayEvent1);
			}


            ReplayEvent event = new ReplayEvent("ExpressionTrigger", 1, 1, 1, subscriber,  tt++, "SOURCE: [" + this.request.query(0).sourceQuery() + "] Filter: " + triggerEvaluator.filters.toString());
            event.setDefaultFieldValues("", "","","","", "","","");
            fireTrigger(event);

                if (this.requests != null) this.requests.clear();
                if (this.logEvents != null) this.logEvents.clear();
		}
		if (verbose) LOGGER.info("DONE");
	}
	
    public void attach(LogSpace logSpace, AggSpace aggSpace, SpaceService spaceService, AdminSpace adminSpace) {
		this.logSpace = logSpace;
		this.aggSpace = aggSpace;
		this.spaceService = spaceService;
		this.adminSpace = adminSpace;
		this.histogramHandler = new HistogramHandler(Executors.newScheduledThreadPool(1), requests.values().iterator().next(), logSpace);
	}
	
	public boolean isReplayOnly() {
		return false;
	}
	
	public String getId() {
		if (this.thisId == null) this.thisId = getClass().getSimpleName() + "_" + name + reportName + "_" + UID.getUUIDWithHostNameAndTime();
		return thisId;
	}

	public void handle(ReplayEvent event) {
		if (firedOnceToPreventSpam) return;
		
		synchronized (this) {
			if (firedOnceToPreventSpam) return;
			putReplayEvent(event);
		}
	}
	synchronized ReplayEvent putReplayEvent(ReplayEvent event) {
		long time = event.getTime();
		while (logEvents.containsKey(time)) { time++;}
		return logEvents.put(time, event);
	}

	public void handle(Bucket event) {
	}

	@SuppressWarnings("unchecked")
	public int handle(String providerId, String subscriber, int size, Map<String, Object> histoWrapper) {
		if (fired && firedOnceToPreventSpam) return 1;
		List<Map<String, Bucket>> histo = (List<Map<String, Bucket>>) histoWrapper.get("histo");
		if (verbose) LOGGER.info("ReceivedHist:" + subscriber);
		bucketsReceived++;
		histogramHandler.handle(providerId, subscriber, size, histo);

        List<ClientHistoItem> clientHisto = histogramHandler.getHistogramForClient(histogramHandler.getHisto(), request);
        evaluateTrigger(subscriber, clientHisto);
		return 1;

    }

	public int handle(List<ReplayEvent> events) {
		for (ReplayEvent event : events) {
			handle(event);
		}
		return 100;
	}

	public void handleSummary(Bucket bucketToSend) {
	}


	public void fireTrigger(ReplayEvent event) {
		if (verbose) {
			LOGGER.info("LOGGER " + request.subscriber() + "- Incrementing Trigger, count:" + triggerCount);
		}
		LOGGER.info(String.format(" %s Schedule[%s] TRIGGERED[%d] ThresholdPassed[%s]", Schedule.TAG, name, triggerCount, trigger));
		

		// make sure we wait until replays have arrived - they can come after the histogram has been triggered
		int wait = 0;
		while (logEvents == null && wait++ < 10) {
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
		if (logEvents == null) {
			LOGGER.error("Failed to Receive any LogEvent Data sub:" + this.request.subscriber());
			logEvents = new HashMap<Long, ReplayEvent>();
		}
		triggerCount++;


		LOGGER.info(String.format(" %s Schedule:%s Handlers:%d", Schedule.TAG, name, scheduleHandlers.size()));
		// TODO:::: NEED to pass through the histogram/xml etc here for processing....maybe?
		for (ScheduleHandler handler : this.scheduleHandlers) {
			handler.handle(event, logEvents, triggerCount, triggerCount);
		}

        if (triggerFiredCallBack != null) {
            triggerFiredCallBack.fired(Arrays.asList(event.getRawData()));
        }


        this.logEvents.clear();
		
	}
	
	public void stop() {
		if (logEvents != null) logEvents.clear();
        histogramHandler.stop();
		requests.clear();
        scheduledEvent.cancel(true);
	}

	public static class ExpressionEvaluator {

        private final List<Filter> filters;

        // should support filters - same as the search expression
        // i.e.
        // AND - CPU.gt(10) CPU.lt(80)
        // OR  - level.contains(WARN,ERROR)
		public ExpressionEvaluator(String trigger) {
            LogRequest request = new LogRequestBuilder().getLogRequest("SUB", Arrays.asList("* | " + trigger), "", 0, 0);
            filters = request.queries().get(0).filters();
		}

		/**
		 * Return TRUE when it passed the filter
		 * for example
		 * Returns SVs key=level_DEBUG value=999
		 * Trigger expression would be level_WARN > 90
         * Expression incoming CPU.countDeltaPerecent(, DELTA)
         * -> We filter with DELTA.gt(90) DELTA.lt(-10)
		 * To perform on an agnostic group basis use prt of the fieldname on the filter i.e. CPU_.gt(90)
         * @return
		 */
		public boolean isTriggered(List<ClientHistoItem> histoItems) {
            List<Filter> passedFilters = new ArrayList<Filter>();
            for (Filter filter : filters) {
                if (passedFilters.contains(filter)) continue;
                for (ClientHistoItem histoItem : histoItems) {
                    for (SeriesValue sv : histoItem.series.values()) {
                        if (sv.label.contains(filter.group())) {
                            if (filter.execute(sv.getValue()+"")) {
                                if (!passedFilters.contains(filter)) passedFilters.add(filter);
                            }
                        }
                    }
                }
            }
            return passedFilters.size() == filters.size();

		}
		public String toString() {
			return "TEval:[" + this.filters;
		}
	}
	public int getMaxReplays() {
		return 1024;
	}

}
