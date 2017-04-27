package com.liquidlabs.dashboard.server.vscape.dto;

import java.util.ArrayList;
import java.util.Map;

public class Usage {

	public String time;
	public long timeMs;
	public String appName;
	public int completed;
	public int running;
	public Map<String, Long> serviceCosts;
	public Map<String, Long> serviceCount;
	public ArrayList<String> serviceNames;

	public Usage(String time, long timeMs,String appName, int completed, int running, Map<String, Long> serviceCosts, Map<String, Long> serviceCount) {
		this.time = time;
		this.timeMs = timeMs;
		this.appName = appName;
		this.completed = completed;
		this.running = running;
		this.serviceCosts = serviceCosts;
		this.serviceCount = serviceCount;
		if (serviceCosts != null) serviceNames = new ArrayList<String>(serviceCosts.keySet());
	}

	public void update(int completedUnitCosts, int runningUnitCosts, Map<String, Long> serviceCosts2, Map<String, Long> serviceCount2) {
		this.completed += completedUnitCosts;
		this.running += runningUnitCosts;
		if (serviceCosts != null) serviceNames.addAll(serviceCosts2.keySet());
		this.serviceCosts.putAll(serviceCosts2);
		this.serviceCount.putAll(serviceCount2);
		
	}

}
