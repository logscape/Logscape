package com.liquidlabs.vso.agent;

public interface ResourceAgentAdminImplMBean {

    /**
     * Attributes
     * @return
     */
    String getStatus();

    /**
     * Operations
     * @param argument
     * @return
     */

	String dumpThreadsWithOptionalCommaDelimFilter(String argument);

	String displayProfileInformation(String argument);

	String listProcesses(String arguments);

	String displaySystemProperties(String argument);

	String listEmbeddedServices(String args);
	
	String preventSystemLeaseRenewals();

	String setClassLogLevel(String clazz, String level);

	String setSystemProperty(String key, String value);

	String killProcessWithWorkId(String workId);

    String dumpHeap(String filename);
}
