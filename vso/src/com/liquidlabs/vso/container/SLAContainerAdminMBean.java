package com.liquidlabs.vso.container;

public interface SLAContainerAdminMBean {

	String displaySLAContent();

	String updateSLA(String sla);
	
	String validateSLA(String sla);

	String triggerSLAAction(String script, int priority);

	String getNotifyFilter();

	void handleLogEvent(String message);

	String getServiceToRun();

	String resetSLAStatus();

	String status();

	String getServiceCriteria();


}
