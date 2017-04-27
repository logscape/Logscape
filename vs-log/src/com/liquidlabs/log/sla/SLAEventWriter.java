package com.liquidlabs.log.sla;

import com.liquidlabs.log.space.LogEvent;
import com.liquidlabs.log.space.LogEventListener;
import com.liquidlabs.vso.container.SLAContainerAdminMBean;

public class SLAEventWriter implements LogEventListener{

	private final String endPoint;
	private final SLAContainerAdminMBean slaAdmin;
	private final String slaId;

	public SLAEventWriter(String endPoint, SLAContainerAdminMBean slaAdmin, String slaId) {
		this.endPoint = endPoint;
		this.slaAdmin = slaAdmin;
		this.slaId = slaId;
	}
	
	public String getId() {
		return "SLAEventWriter" + slaId;
	}

	public void handle(LogEvent event) {
		slaAdmin.handleLogEvent(event.getMessage());
	}
	
}
