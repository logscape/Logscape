package com.liquidlabs.log.alert;

import it.sauronsoftware.cron4j.Task;
import it.sauronsoftware.cron4j.TaskExecutor;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.management.MBeanServer;
import javax.management.ObjectName;

public class AlertSchedulerJmxAdmin implements AlertSchedulerJmxAdminMBean {
	
	private final AlertScheduler scheduler;

	public AlertSchedulerJmxAdmin(AlertScheduler scheduler) {
		this.scheduler = scheduler;
		
		ObjectName objectName;
		try {
			MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
			objectName = new ObjectName("com.liquidlabs.log.alert.AlertScheduler:type=Admin");
			if (mbeanServer.isRegistered(objectName)) return;
			mbeanServer.registerMBean(this, objectName);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public String listCronScheduledTasks() {
		StringBuilder results = new StringBuilder();
		List<String> scheduledTasks = new ArrayList<String>(scheduler.scheduledTasks.keySet());
		Collections.sort(scheduledTasks);
		
		int i = 0;
		for (String string : scheduledTasks) {
			results.append(i++).append(") ").append(string).append(" = ").append(scheduler.scheduledTasks.get(string)).append("<BR/>\n");
		}
		
		return results.toString();
	}
	public String cancelTask(String taskId) {
		return "done";
	}
	public String listExecutingTasks() {
		StringBuilder results = new StringBuilder();
		TaskExecutor[] tasks = scheduler.cronScheduler.getExecutingTasks();
		int i = 0;
		for (TaskExecutor taskExecutor : tasks) {
			String guid = taskExecutor.getGuid();
			String status = taskExecutor.getStatusMessage();
			Task task = taskExecutor.getTask();
			results.append(i++).append(") ").append(guid).append(" = ").append(status).append(" task:").append(task).append("<BR/>\n");
		}
		return results.toString();
	}

}
