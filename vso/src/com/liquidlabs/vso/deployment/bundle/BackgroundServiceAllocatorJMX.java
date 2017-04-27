package com.liquidlabs.vso.deployment.bundle;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

import com.liquidlabs.vso.work.WorkAssignment;

public class BackgroundServiceAllocatorJMX implements BackgroundServiceAllocatorJMXMBean {

	final static Logger LOGGER = Logger.getLogger(BackgroundServiceAllocatorJMX.class);
	private final BackgroundServiceAllocator bg;
	public BackgroundServiceAllocatorJMX(BackgroundServiceAllocator bg) {
		this.bg = bg;
		try {
			MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
			ObjectName objectName = new ObjectName("com.liquidlabs.vso:Component=BackgroundServiceAllocatorJMX");
			if (mbeanServer.isRegistered(objectName)) return;
			mbeanServer.registerMBean(this, objectName);
		} catch (Exception e) {
			LOGGER.error(e);
		}

	}
	public String listPendingWork(){
		List<String> keys = new ArrayList<String>(bg.workToAllocate.keySet(WorkAssignment.class));
		Collections.sort(keys);
		int pos = 1;
		StringBuilder result = new StringBuilder();
		for (String key : keys) {
			WorkAssignment work = bg.workToAllocate.findById(WorkAssignment.class, key);
			result.append(pos++).append(") ").append(key).append(" =  Outstanding:").append(work.getAllocationsOutstanding()).append("/n<BR/>");
		}
		
		return result.toString();		

	}
	public String listAllocatedTo(){
		List<String> keySet = new ArrayList<String>(bg.allocatedTo.keySet());
		Collections.sort(keySet);
		int pos = 1;
		StringBuilder result = new StringBuilder();
		for (String key : keySet) {
			result.append(pos++).append(") ").append(key).append(" = ").append(bg.allocatedTo.get(key)).append("/n<BR/>");
		}
		
		return result.toString();		
	}
	public String listJustAssigned() {
		List<String> keySet = new ArrayList<String>(bg.justAssignedTasks.keySet());
		Collections.sort(keySet);
		int pos = 1;
		StringBuilder result = new StringBuilder();
		for (String key : keySet) {
			result.append(pos++).append(") ").append(key).append(" = ").append(bg.justAssignedTasks.get(key)).append("/n<BR/>");
		}
		
		return result.toString();
	}

}
