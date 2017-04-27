package com.liquidlabs.log.alert;

import com.liquidlabs.admin.AdminSpace;
import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.common.concurrent.NamingThreadFactory;
import com.liquidlabs.log.space.AggSpace;
import com.liquidlabs.log.space.LogSpace;
import com.liquidlabs.transport.proxy.events.Event;
import com.liquidlabs.transport.proxy.events.Event.Type;
import com.liquidlabs.vso.Notifier;
import com.liquidlabs.vso.SpaceService;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.ServiceInfo;
import com.liquidlabs.vso.resource.ResourceSpace;
import it.sauronsoftware.cron4j.Scheduler;
import it.sauronsoftware.cron4j.Task;
import it.sauronsoftware.cron4j.TaskExecutionContext;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

public class AlertScheduler implements LifeCycle  {

	private final static Logger LOGGER = Logger.getLogger(AlertScheduler.class);

	private final LogSpace logSpace;

	private final AdminSpace adminSpace;
    private final ResourceSpace resourceSpace;

	private final AggSpace aggSpace;

	private final SpaceService logDataSpaceService;

	private ScheduledExecutorService scheduleExecutor;
	Scheduler cronScheduler = new Scheduler();
	String lastScheduleError = null;

	private final LookupSpace lookupSpace;

	private final ServiceInfo logSpaceServiceInfo;
	private State state = State.RUNNING;

	public AlertScheduler(LogSpace logSpace, AggSpace aggSpace, AdminSpace adminSpace, SpaceService logDataSpaceService, LookupSpace lookupSpace, ServiceInfo serviceInfo, ResourceSpace resourceSpace) {
		this.logSpace = logSpace;
		this.aggSpace = aggSpace;
		this.adminSpace = adminSpace;
		this.logDataSpaceService = logDataSpaceService;
		this.lookupSpace = lookupSpace;
		this.logSpaceServiceInfo = serviceInfo;

		scheduleExecutor = ExecutorService.newScheduledThreadPool(50, new NamingThreadFactory("AlertPool", true, Thread.NORM_PRIORITY));

        this.resourceSpace = resourceSpace;

	}

	public Map<String, Schedule> scheduledTasks = new ConcurrentHashMap<String, Schedule>();

	public void start() {

		String agentRole = VSOProperties.getResourceType();

		if (agentRole.endsWith("Management") ) {
			LOGGER.info("Scheduling as Management");
			cronScheduler.start();


			scheduleTasks();
			LOGGER.info("Scheduled:" + scheduledTasks.keySet());
		} else {
			LOGGER.info("Deferred Scheduling until Failover occurs");
			// we are the failover node
			// only start Alerts if we are on the Management Resource OR
			// if we are on the Failover Resource
			Runnable failoverDetector = new Runnable() {
				boolean isRunning = false;
				boolean isFailover = false;
				public void run() {
					try {
						String[] serviceAddresses = lookupSpace.getServiceAddresses(LogSpace.NAME, VSOProperties.getZone(), true);
						LOGGER.info("LS_EVENT:Failover - Checking Addresses:" + serviceAddresses.length);
						// if FAILOVER ON
						if (serviceAddresses.length == 1) {
							if (!isFailover) {

								LOGGER.info("LS_EVENT:Failover - Starting Scheduled Tasks on FAILOVER");
								try {
									// need to force 1 way msgs to failover
									aggSpace.size();
								} catch (Throwable t) {
									LOGGER.error("AggSpace reference didnt refresh properly" , t);
								}
								isFailover = true;
								isRunning = true;
								// then we are in failover
								cronScheduler.start();
								scheduleTasks();

							}
						} else {
							if (isRunning) {
								isRunning = false;
								LOGGER.info("LS_EVENT:Failover - Disabling Scheduled Tasks to allow Manager Node to take over");
								isFailover = false;
								// FAILOVER=>FAILBACK
								//
								for (Schedule task : scheduledTasks.values()) {
									cancelATask(task);
								}

								cronScheduler.stop();
							}
						}
					} catch (Throwable t) {
						LOGGER.warn("Failover Task error:",t);
					}


				}
			};
			scheduleExecutor.scheduleAtFixedRate(failoverDetector, 1, 1, TimeUnit.MINUTES);
		}
	}

	private void scheduleTasks() {
		List<String> allScheduleNames = logSpace.getScheduleNames("");
		for (String scheduleName : allScheduleNames) {
			final Schedule schedule = logSpace.getSchedule(scheduleName);
			scheduleATask(schedule);
		}
	}

	String scheduleATask(final Schedule schedule) {
		LOGGER.info("Creating:" + schedule);
				try {

				final String scheduledId = cronScheduler.schedule(schedule.cron, new Task() {
					public void execute(final TaskExecutionContext context) throws RuntimeException {
						Future<?> future = scheduleExecutor.submit(new Runnable() {
								public void run() {
									try {
										if (state != State.RUNNING) return;
                                        if (logSpace.getSchedule(schedule.name) == null) {
                                            LOGGER.warn("Schedule not found:" + schedule.name);
                                            return;
                                        }
										context.setStatusMessage("Running");
										LOGGER.info("Running:" + schedule + " sch" + schedule.cronId);
										schedule.run(logSpace, aggSpace, AlertScheduler.this.logDataSpaceService, adminSpace, scheduleExecutor, resourceSpace);
										context.setStatusMessage("Complete");
									} catch (Throwable t) {
										context.setStatusMessage("Error:" + t.toString());
										LOGGER.warn("Schedule:" + schedule + " failed:" + t, t);
									}
								}
							});
						try {
							// get the future so we get a correct status when listing running tasks
							future.get();
						} catch (InterruptedException e) {
							e.printStackTrace();
						} catch (ExecutionException e) {
							e.printStackTrace();
						}
					}

				});
				schedule.cronId = scheduledId;
				scheduledTasks.put(scheduledId, schedule);
				return scheduledId;
		} catch (Throwable t) {
            AlertScheduler.this.exception = t;
			LOGGER.error("Failed to schedule task:" + schedule.name, t);
		}
		return "";
	}

	public String manuallyRun(String name) {
		LOGGER.info("Manually Running Schedule:" + name);
		final Schedule schedule = logSpace.getSchedule(name);
		if (schedule == null) return "Schedule not found:" + name;
		Future<?> task = scheduleExecutor.submit(new Runnable(){
			public void run() {
					try {
						// turn off live when running manually - we prob want to see some data
						schedule.isLiveAlert = false;
						schedule.trigger = "";
						schedule.run(logSpace, aggSpace, AlertScheduler.this.logDataSpaceService, adminSpace, scheduleExecutor, resourceSpace);
					} catch (Exception e) {
						LOGGER.warn("Manual Run Failed", e);
					}
			}
		});


		String result = "Executed:" + name + "@" + new DateTime();
		try {
			task.get(30, TimeUnit.SECONDS);
		} catch (Exception e) {
			LOGGER.error("Manual Run Failed", e);
			result += ":" + e.toString();
		}
		if (schedule.errorMsg != null) result += " " + schedule.errorMsg;
		return result;
	}
	private void cancelATask(final Schedule schedule) {
		LOGGER.info("Cancelling:" + schedule);

		Set<String> taskIds = this.scheduledTasks.keySet();
		Set<String> removed = new HashSet<String>();
		for (String taskId : taskIds) {
			Schedule schedule2 = this.scheduledTasks.get(taskId);
			if (schedule.name.equals(schedule2.name)) {
				LOGGER.info("Descheduling:" + taskId + " /" + schedule.name);
				schedule.cancel(logSpace);

				cronScheduler.deschedule(taskId);
				removed.add(taskId);
			}
		}
		taskIds.removeAll(removed);
	}
	public void stop() {
		this.state = State.STOPPED;
		LOGGER.info("LS_EVENT:Stop LogScheduler");
        if(cronScheduler.isStarted()) {
		    cronScheduler.stop();
        }
	}


	public void registerEventListener(SpaceService logDataService) {

		logDataService.registerListener(Schedule.class, "", new Notifier<Schedule>() {

			public void notify(Type event, Schedule schedule) {
				try {
					if (event == Type.WRITE) {
						scheduleATask(schedule);
					}
					if (event == Type.UPDATE) {
						cancelATask(schedule);
						scheduleATask(schedule);
					}
					if (event == Type.TAKE) {
						cancelATask(schedule);
					}
				} catch (Throwable t){
					lastScheduleError = new DateTime() + " ex:" + t + " schedule:" + schedule;
					LOGGER.error("Failed to schedule:" + schedule, t);
				}
			}

		}, "ReportScheduler", -1, new Event.Type[]{Type.WRITE, Type.UPDATE, Type.TAKE});

	}

    protected Throwable exception = null;
    public Throwable lastError() {
        Throwable last = this.exception;
        this.exception = null;
        return last;


    }
}
