package com.liquidlabs.log.alert;

import com.google.gson.Gson;
import com.liquidlabs.admin.AdminSpace;
import com.liquidlabs.admin.DataGroup;
import com.liquidlabs.admin.User;
import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.log.LogRequestBuilder;
import com.liquidlabs.log.alert.correlate.CorrEventFeedAssembler;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.ReplayEvent;
import com.liquidlabs.log.space.*;
import com.liquidlabs.orm.Id;
import com.liquidlabs.vso.SpaceService;
import com.liquidlabs.vso.resource.BloomMatcher;
import com.liquidlabs.vso.resource.ResourceSpace;
import it.sauronsoftware.cron4j.Predictor;
import it.sauronsoftware.cron4j.SchedulingPattern;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeUtils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class Schedule  {
    // need logs for audit trail
	private final static Logger LOGGER = Logger.getLogger(Schedule.class);
	public final static String TAG = "REPORT_SCHEDULE";
    public static final String WEB_SOCKET_PORT = "webSocketPort";
    public static final String FEED_SCRIPT = "feedScript";
	transient static private AtomicLong SUB_ID = new AtomicLong();


	// V2 - added isGeneratingReport and isEmailing Report
	enum SCHEMA { copyAction, emailFrom, emailMessage, emailSubject, emailTo, 
					end, firedOnceToPreventSpam, interval, isLiveAlert, lastEvents, 
					lastRequest, lastRun, lastTrigger, logAction, msgOnly, 
					name, reportName, scriptAction, start,
					trigger, isGeneratingReport, isEmailingReport, generateReportName, userId, subcriber, cron, lastRunMs, deptScope, variables, enabled, liveFeedScript };
	
	@Id
	public String name;
	
	// search to trigger events
	public String reportName ="";
	
	public String cron;
	@Deprecated
	public String start;
	@Deprecated
	public String end;
	@Deprecated
	public int interval = 10;
	
	public String copyAction;
	public String scriptAction;
	public String logAction;
	public String msgOnly;
	
	public String lastRequest;
	
	public String lastRun = "";
	public long lastRunMs;
	public String lastEvents = "";
	public String lastTrigger = "";
	public String trigger;
	public String triggerExp;
	public String userId = "admin";
	
	
	
	public String emailFrom;
	public String emailTo;
	public String emailSubject;
	public String emailMessage;
	
	public boolean isLiveAlert;
	
	public boolean isEmailingReport;
	@Deprecated
	public boolean isGeneratingReport;
	
	public String generateReportName;
	
	boolean firedOnceToPreventSpam = false;
	
	public String subcriber = "";
	
	public String deptScope = "all";

	public String variables = "";

    private boolean enabled = true;
	
	transient AdminSpace adminSpace;
	transient LogRequest request;
	transient LogSpace logSpace;
	transient AggSpace aggSpace;
	transient String errorMsg;
	public transient Trigger triggerListener;
	transient public String cronId = "";

    /**
     * A Stringified hashmap that contains webSocketPort
     * OR bespokeScript = "XXXXX"
     */
    public String liveFeedScript = "";
	
	public Schedule(){
	}
	
	public Schedule(String userId, String name, String triggerSearch, String generateReportName, String scriptAction, String copyAction, String logAction, String lastRun, String trigger, String emailFrom, String emailTo, String emailSubject, String emailMessage, boolean isLiveAlert, boolean isEmailingReport, String cron, String deptScope, String variables, boolean enabled, String webSocketPort, String bespokeFeedScript) {
		this.userId = userId;
		this.name = name;
		this.reportName = triggerSearch;
		this.scriptAction = scriptAction;
		this.copyAction = copyAction;
		this.logAction = logAction;
		this.trigger = trigger;
		this.emailFrom = emailFrom;
		this.emailTo = emailTo;
		this.emailSubject = emailSubject;
		this.emailMessage = emailMessage;
		this.isLiveAlert = isLiveAlert;
		this.isEmailingReport = isEmailingReport;
		this.generateReportName = generateReportName;
		this.cron = cron;
		this.deptScope = deptScope;
		this.variables = variables;
        this.enabled = enabled;
        this.liveFeedScript = new Config(webSocketPort, bespokeFeedScript).toJson();
	}

	public void run(final LogSpace logSpace, AggSpace aggSpace, final SpaceService spaceService, AdminSpace adminSpace, ScheduledExecutorService scheduler, ResourceSpace resourceSpace) {
		try {
			if (!enabled) return;
			
			this.logSpace = logSpace;
			this.aggSpace = aggSpace;
			this.adminSpace = adminSpace;
			this.errorMsg = null;
			this.firedOnceToPreventSpam = false;

			ScheduleExecution execution = findExecution(spaceService);
			execution.updateLastRun();
			saveExecution(spaceService, execution);
			final String searchName = reportName;
			
			if (!this.getClass().getName().contains("Test") && logSpace.getSchedule(this.name) == null) {
				LOGGER.warn("Failed to find Schedule:" + name + " - cancelling execution");
				return;
			}

            logSomeShit(searchName);
			
			if (!isTriggerBased()  && (generateReportName != null && generateReportName.length() > 0)) {
                runScheduledReportGeneration(logSpace, aggSpace, spaceService, adminSpace, searchName, scheduler);
			    return;
			}

            Search search = logSpace.getSearch(searchName, null);
			if (search == null) {
				LOGGER.warn("TriggerSearch:" + searchName + " doesnt exist - aborting Schedule:" + this.name + " Trigger was[" + this.trigger+"]" );
				return;
			}


			subcriber = "ALERT_" + deptScope + "_" + searchName + "_" + SUB_ID.incrementAndGet();
			subcriber = subcriber.replaceAll(" ", "_");
			
            User user = null;
            BloomMatcher bloomFilter = null;
			if (deptScope != null && deptScope.length() > 0) {
                DataGroup dataGroup = adminSpace.getDataGroup(deptScope, true);
                if (dataGroup != null) {
                    user = new User();
                    user.setUsername("scheduler-service-" + deptScope);
                    user.setDataGroup(dataGroup);
                    bloomFilter = resourceSpace.expandGroupIntoBloomFilter(dataGroup.getResourceGroup());
                } else {
                    LOGGER.error("Failed to Load DataGroup for Alert:" + name + " Group:" + deptScope);
                    return;
                }
			}  else {
                LOGGER.error("DataGroup not specified for Alert:" + name);
                return;
            }

			
			request = new LogRequestBuilder().getLogRequest(subcriber, search, variables);
            request.setUser(user);
            if (bloomFilter != null) request.setHosts(bloomFilter);

			lastRequest = request.subscriber();
			
			// nothing to trigger - just run it
			if (isTriggerNow()) {
                fireImmediately(logSpace, aggSpace, spaceService, adminSpace, scheduler, searchName);
				return;
			}

            this.lastRequest = request.subscriber();
            execute(logSpace, aggSpace, spaceService, adminSpace, scheduler);
			
		} catch (Throwable e) {
			String errorMsg = String.format("%s Error executing Schedule[%s] Report[%s]", TAG, name, reportName);
			LOGGER.warn(errorMsg, e);
			this.errorMsg = errorMsg;
		}
	}

    private void execute(LogSpace logSpace, AggSpace aggSpace, SpaceService spaceService, AdminSpace adminSpace, ScheduledExecutorService scheduler) throws Exception {
        triggerListener = getTriggerListener(trigger, scheduler, spaceService, subcriber);

        Trigger proxy = triggerListener;

        if (getBespokeFeedScript().length() > 0) {
            proxy = new LiveFeed(name, getBespokeFeedScript(), triggerListener, logSpace).proxy();
            triggerListener = proxy;
        } else if (getWebSocketPort() != null && getWebSocketPort().length() > 0) {
            String dir = System.getProperty("feed.script.dir", "scripts/feed");
            proxy = new LiveFeed(name, dir + "/webSocketServer.groovy " + getWebSocketPort(), triggerListener, logSpace).proxy();
            triggerListener = proxy;
        }


        configureRequest(new Replay(ReplayType.START, triggerListener.getMaxReplays()));

        LOGGER.info("LOGGER Executing:" + this.request + " Trigger:" + triggerListener);
        scheduleUnregister(proxy, spaceService, request, scheduler);


        proxy.attach(logSpace, aggSpace, spaceService, adminSpace);

        if (triggerListener.isReplayOnly()) {
            aggSpace.replay(request, triggerListener.getId(), triggerListener);
        } else {
            request.setSearch(true);
            aggSpace.search(request, triggerListener.getId(), triggerListener);
        }
        logSpace.executeRequest(request);
    }

    private void configureRequest(Replay replay) {
        if (this.isLiveAlert) {
            // REALTIME - ALERT (LIVE)
            // setup the streaming to start from NOW - and autocancel after in interval minutes
            request.setStreaming(true);
            request.setTimeToLive(getTTL());
            request.setStartTimeMs(DateTimeUtils.currentTimeMillis());
            request.setEndTimeMs(request.getStartTimeMs() + getTTL() * DateUtil.MINUTE);
        } else {
            request.setTimePeriodMins(getTTL());
        }
        request.setReplay(replay);
    }

    private void fireImmediately(LogSpace logSpace, AggSpace aggSpace, final SpaceService spaceService, AdminSpace adminSpace, ScheduledExecutorService scheduler, String searchName) {
        TriggerFiredCallBack triggerFiredCallBack = new TriggerFiredCallBack() {
            public void fired(List<String> leadingEvents) {
                Schedule.this.updateFiredTime(spaceService, leadingEvents);
            }
        };
        triggerListener = new ReplayBasedTrigger(request.isVerbose(), name, searchName, this, 0, getHandlers(spaceService, logSpace.fieldSets(), scheduler), triggerFiredCallBack, scheduler);
        triggerListener.attach(logSpace, aggSpace, spaceService, adminSpace);
        triggerListener.fireTrigger(new ReplayEvent());
        scheduleUnregister(triggerListener, spaceService, request, scheduler);
    }

    private void runScheduledReportGeneration(LogSpace logSpace, AggSpace aggSpace, SpaceService spaceService, AdminSpace adminSpace, String searchName, ScheduledExecutorService scheduler) {
        LOGGER.info("Running Scheduled generation:" + searchName);
        new PrintReportHandler(logSpace, aggSpace, spaceService.proxyFactory(), adminSpace, this, getTTL(), variables, true, scheduler).handle(null, null, 0, 0);
        updateFiredTime(spaceService, Arrays.asList(""));
    }

    private void logSomeShit(String searchName) {
        //String logMsg = String.format("%s %s Schedule:%s Trigger:%s Action:Active ", DateUtil.shortDateTimeFormat7.print(System.currentTimeMillis()), TAG, removeCrap(name), removeCrap(searchName));
		String logMsg = String.format("{ \"time:\"%s\",  \"Schedule\":\"%s\", \"Trigger\":\"%s\", \"Action\":\"Active\" }",
				DateUtil.shortDateTimeFormat7.print(System.currentTimeMillis()), removeCrap(name), removeCrap(searchName));
        LOGGER.info(logMsg);
        LOGGER.info("LS_EVENT:ALERT " + name);
        logAgainstGroup(Arrays.asList(logMsg), false);
    }

    private String removeCrap(String name) {
        return name.replaceAll("\\s+", "_").replace("(","_").replace(")","");
    }

    private boolean isTriggerBased() {
		return ! (trigger == null || trigger.length() == 0 || trigger.equals("0"));
	}

	public static Object processLock = new Object();
	public void logAgainstGroup(List<String> eventInfos, boolean timestamp) {
		synchronized(processLock) {
			try {
				String now = DateUtil.shortDateFormat.print(System.currentTimeMillis());
				new File("work/schedule").mkdirs();
				OutputStream fos = new BufferedOutputStream(new FileOutputStream("work/schedule/" + now + "-schedule-" + deptScope + ".log", true));
				for (String string : eventInfos) {
					if (timestamp) {
						if (string.length() >0) {
                            String logMsg = String.format("{ \"time\":\"%s\",  %s }", DateUtil.shortDateTimeFormat7.print(System.currentTimeMillis()), string);
                            fos.write((logMsg + "\n").getBytes());
                        }
					} else {
						fos.write((string + "\n").getBytes());
					}
				}
				fos.close();
			} catch (Exception e) {
				LOGGER.error("Failed to write user msg:" + e);
			}
		}
	}

	private void saveExecution(final SpaceService spaceService, ScheduleExecution execution) {
		spaceService.store(execution, (int)( (DateUtil.DAY * 14) / 1000 ));
	}
	protected void updateFiredTime(SpaceService spaceService, List<String> leadingEvents) {
		LOGGER.info("Updating:" + this.name + " LastFired Time");
		LOGGER.info("LS_EVENT:ALERT_FIRED " + name);
//		String msg = String.format("%s %s Schedule:%s Action:Triggered Events:%d ThresholdPassed[%s]\n", DateUtil.shortDateTimeFormat7.print(System.currentTimeMillis()), Schedule.TAG, name.replaceAll("\\s+","_"), 2, trigger);
		String msg = String.format("{ \"time\":\"%s\", \"Schedule\":\"%s\", \"Action\":\"Triggered\", \"Events\":\"%d\", \"ThresholdPassed\":\"%s\" }",
				DateUtil.shortDateTimeFormat7.print(System.currentTimeMillis()),  name.replaceAll("\\s+","_"), 2, trigger);

        // indent the events
        ArrayList<String> mmm = new ArrayList<String>();
        for (String event : leadingEvents) {
            mmm.add(" { \"event\": \"" + org.apache.commons.lang3.StringEscapeUtils.escapeJson(event) + " \" }\n");
        }
        mmm.add(0, msg);

		logAgainstGroup(mmm, false);

		// reload ourselves and update the last trigger time and save us back down again
		
		try {
			ScheduleExecution execution = findExecution(spaceService);
			execution.updateLastTrigger();
			saveExecution(spaceService, execution);
		} catch (Exception e) {
			LOGGER.warn(e);
		}
	}

	private ScheduleExecution findExecution(SpaceService logDataService) {
		ScheduleExecution execution = logDataService.findById(ScheduleExecution.class, name);
		if (execution == null) {
			execution = new ScheduleExecution(name);
		}
		return execution;
	}

	private void scheduleUnregister(final Trigger trigger, final SpaceService logSpaceDataspaceService, final LogRequest request, ScheduledExecutorService scheduler) {
		int expireSeconds = getTTL() * 60;
		scheduler.schedule(new Runnable() {
			public void run() {
				if (LOGGER.isDebugEnabled()) LOGGER.debug("Closing:" + request.subscriber());
				try {
					logSpaceDataspaceService.proxyFactory().stopProxy(trigger);
				} catch (Exception e) {
					LOGGER.warn("CloseFail:" + e.toString(), e);
				}
				try {
					trigger.stop();
				} catch (Exception e) {
					LOGGER.error(e);
				}
                // want to stop the previous before the next one goes
			}
		}, expireSeconds-1, TimeUnit.SECONDS);
		

		// cleanup the logrequest msg at the end
		scheduler.schedule(new Runnable() {
			@Override
			public void run() {
				logSpace.cancel(request.subscriber());
				
			}
		}, expireSeconds, TimeUnit.SECONDS);
	}

	private Trigger getTriggerListener(String triggerExpression, ScheduledExecutorService scheduler, final SpaceService spaceService, String subscriber) {
		triggerExpression = triggerExpression.trim();
		
		TriggerFiredCallBack triggerFiredCallBack = new TriggerFiredCallBack() {
			public void fired(List<String> leadingEvents) {
				Schedule.this.updateFiredTime(spaceService, leadingEvents);
                triggerListener.stop();
			}
		};
		List<FieldSet> fieldSets = logSpace.fieldSets();

		if (StringUtil.isInteger(triggerExpression) != null) return new ReplayBasedTrigger(request.isVerbose(), name, reportName, this, Integer.parseInt(triggerExpression), getHandlers(spaceService, fieldSets, scheduler), triggerFiredCallBack, scheduler);
		if (triggerExpression.contains("corr:")) {
			CorrEventFeedAssembler eventFeedAssembler = new CorrEventFeedAssembler(scheduler, getHandlers(spaceService, fieldSets, scheduler), triggerFiredCallBack);
			return eventFeedAssembler.createTrigger(this.name, Integer.getInteger("alert.corr.eval.sec", 3), triggerExpression, subscriber, fieldSets);
		}
		return new ExpressionTrigger(request, name, reportName, this, trigger, getHandlers(spaceService, fieldSets, scheduler), triggerFiredCallBack, scheduler);
	}

    public boolean isNumericTrigger() {
        return StringUtil.isInteger(trigger) != null;
    }

    public boolean isCorrelationTrigger() {
        return trigger.contains("corr:");
    }

    public boolean isExpressionTrigger() {
        return !isNumericTrigger() && !isCorrelationTrigger();
    }

	private List<ScheduleHandler> getHandlers(SpaceService logSpaceDataService, List<FieldSet> fieldSets, ScheduledExecutorService scheduler) {
		ArrayList<ScheduleHandler> handlers = new ArrayList<ScheduleHandler>();
		if (hasLogAction()) {
			handlers.add(new LogHandler(TAG, LOGGER, name, reportName,0, logAction));
		}
		if (hasCopyAction()) {
			handlers.add(new FileWriteHandler(Schedule.TAG, LOGGER, name, reportName, copyAction));
		}
		if (hasScriptAction()) {
			handlers.add(new GroovyHandler(Schedule.TAG, LOGGER, name, reportName, scriptAction, fieldSets));
		}
        boolean isGeneratingReport = generateReportName != null && generateReportName.length() > 0;
		if (!isGeneratingReport && hasEmailAction()) {
			handlers.add(new EmailHandler(Schedule.TAG, LOGGER, name, reportName, adminSpace,emailFrom, emailTo, emailSubject, emailMessage, fieldSets));
		}

        if (isGeneratingReport) {
			handlers.add(new PrintReportHandler(logSpace, aggSpace, logSpaceDataService.proxyFactory(), adminSpace, this, getTTL(), variables, isPrintInBackground(),scheduler));
		}
		return handlers;
	}

    public boolean hasLogAction() {
        return logAction != null && !logAction.isEmpty();
    }

    public boolean hasCopyAction() {
        return copyAction != null && !copyAction.isEmpty();
    }

    public boolean hasReportAction() {
        return generateReportName !=null && !generateReportName.trim().isEmpty();
    }
    public boolean hasScriptAction() {
        return scriptAction !=null && !scriptAction.trim().isEmpty();
    }


    public boolean hasEmailAction() {
        return emailTo != null && !emailTo.trim().isEmpty() && emailFrom !=null && !emailFrom.trim().isEmpty();
    }

	private boolean isPrintInBackground() {
		return !isTriggerBased() && !isLiveAlert;
	}

	private boolean isTriggerNow() {
		return trigger == null || trigger.trim().length() == 0 || trigger.equals("0");
	}

	/**
	 * Read from the Cron schedule when the next time is - and assume it was called at a current interval - so assume next == last interval
	 * @return
	 */
	public short getTTL() {
		String cron = this.cron == null ? "* * * * *" : this.cron;
		Predictor predictor = new Predictor(new SchedulingPattern(cron));
		
		long nextMillis = predictor.nextMatchingTime();
		long nextMillis2 = predictor.nextMatchingTime();
		
		return (short) ((nextMillis2 - nextMillis)/ (60 * 1000));
	}

	public void setAdminSpace(AdminSpace adminSpace) {
		this.adminSpace = adminSpace;
	}
	
	public boolean isJustFiredInLastSecond() {
		return DateTimeUtils.currentTimeMillis() - lastRunMs < 1000;
	}

	public String toString() {
		if (cron == null || cron.equals("0")) cron = "* * * * *";
		return String.format("Schedule name:%s cron:%s triggerSource:%s lastRun:%s lastTrigger:%s ", name, cron, reportName, lastRun, lastTrigger);
	}
	private String getTimeString(long time) {
		if (time == 0) return "";
		return DateUtil.shortDateTimeFormat3.print(time);
	}

	public void cancel(LogSpace logSpace) {
		if (request != null) {
			LOGGER.info("Cancelling : " + request.subscriber());
			request.cancel();
			logSpace.cancel(request.subscriber());
			aggSpace.cancel(request.subscriber());
            triggerListener.stop();
		}

	}

	public void updateExecution(SpaceService logDataService) {
		ScheduleExecution execution = findExecution(logDataService);
		this.lastRun = getTimeString(execution.getLastRunMs());
		this.lastTrigger = getTimeString(execution.getLastTriggerMs());
	}
    public boolean isEnabled(){
        return this.enabled;
    }
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
    public static class Config {
        String webSocketPort = "";
        String bespokeScript = "";
        public Config(String webSocketPort, String bespokeScript) {
            if (webSocketPort == null) webSocketPort = "";
            if (bespokeScript == null) bespokeScript = "";
            this.webSocketPort = webSocketPort;
            this.bespokeScript = bespokeScript;
        }
        String toJson() {
            Gson gson = new Gson();
            return gson.toJson(this);
        }
    }
    public String getWebSocketPort() {
        try {
            Gson gson = new Gson();
            Config config = gson.fromJson(this.liveFeedScript, Config.class);
            return config.webSocketPort;
        } catch (Throwable t) {
            return "";
        }

    }
    public String getBespokeFeedScript() {
        try {
            Gson gson = new Gson();
            Config config = gson.fromJson(this.liveFeedScript, Config.class);
            return config.bespokeScript;
        } catch (Throwable t) {
            return "";
        }

}


}
