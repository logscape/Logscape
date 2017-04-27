package com.liquidlabs.vso.agent;

import com.liquidlabs.common.ExceptionUtil;
import com.liquidlabs.common.ThreadUtil;
import com.liquidlabs.common.jmx.JmxHtmlServerImpl;
import com.liquidlabs.common.util.HeapDumper;
import com.liquidlabs.space.lease.Renewer;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.transport.proxy.Remotable;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.ServiceInfo;
import com.liquidlabs.vso.work.InvokableImpl;
import com.liquidlabs.vso.work.InvokableUI;
import com.thoughtworks.xstream.XStream;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

public class ResourceAgentAdminImpl implements ResourceAgentAdminImplMBean, Remotable, InvokableUI  {
	final static Logger LOGGER = Logger.getLogger(ResourceAgentAdminImpl.class);
	
	private final ResourceAgentImpl agent;
	String id = "";

	public ResourceAgentAdminImpl(ResourceAgentImpl agent, ResourceProfile resourceProfile) {
		this.agent = agent;
		try {
			MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
			ObjectName objectName = new ObjectName("com.liquidlabs.logscape:Service=AGENT");
			if (!mbeanServer.isRegistered(objectName)) {
				mbeanServer.registerMBean(this, objectName);
				this.id = "JMX-" + agent.getId();
			}
			
			ObjectName objectNameP = new ObjectName("com.liquidlabs.logscape:Profile=" + resourceProfile.getId());
			if (!mbeanServer.isRegistered(objectNameP)) {
				mbeanServer.registerMBean(resourceProfile, objectNameP);
			}

		} catch (Exception e) {
			LOGGER.error(e);
		}
	}
	public String getId() {
		return id;
	}
	public String preventSystemLeaseRenewals() {
		Renewer.makeMeBreak = true;
		return "The system is going to go BOOOM!";
	}
	
	public String setClassLogLevel(String name, String level) {
		try {
			LogManager.getLogger(name).setLevel(Level.toLevel(level));
			return "Log Level for:" + name + " is now:" + level;
		} catch (Throwable t) {
			String stringFromStack = ExceptionUtil.stringFromStack(t, Integer.MAX_VALUE);
			return stringFromStack.replaceAll("\n", "<BR/>\n");
		}
	}
	public String setSystemProperty(String key, String value) {
		System.setProperty(key, value);
		return "DONE > System.setProperty(" + key + "," + value + ");";
	}
	
	public String displayProfileInformation(String argument) {
		Resource[] res  = agent.resources;
		XStream stream = new XStream();
		if (res == null) return "null";
		StringBuilder builder = new StringBuilder();
		for (Resource resource : res) {
			ResourceProfile profile = resource.profile();
			String xml = stream.toXML(profile);
			String escapeHtml = StringEscapeUtils.escapeHtml4(xml);
			escapeHtml = escapeHtml.replaceAll("\n", "\n<br>");
			builder.append("\n\n<br><br>*)").append(escapeHtml);
		}
		return builder.toString();
	}

    @Override
    public String getStatus() {
        return agent.status.name();
    }

    public String dumpThreadsWithOptionalCommaDelimFilter(String filter) {
		return ThreadUtil.threadDump(null, filter).replaceAll("\n", "<br>\n");
	}
	
	public String dumpHeap(String arg) {
		System.out.println(new Date() +  " Dumping HEAP");
		return "Created Heap:" + HeapDumper.dumpHeapWithPid(true);
	}
	public String listProcesses(String arguments) {
		String listProcesses = agent.processHandler.listProcesses();
		listProcesses = listProcesses.replaceAll("\n", "\n<br>");
		return listProcesses;
	}
	public String killProcessWithWorkId(String workId) {
		return agent.processHandler.stop(workId);
	}
	public String listEmbeddedServices(String args) {
		StringBuilder builder = new StringBuilder();
		
		Map<String, EmbeddedServiceManager> embeddedServices = agent.embeddedServices;
		for (String key : embeddedServices.keySet()) {
			EmbeddedServiceManager esm = embeddedServices.get(key);
			builder.append(key).append("=").append(esm).append("<br><br>\n\n");
		}
		return builder.toString();
	}
	
	public String displaySystemProperties(String argument) {
		StringBuilder builder = new StringBuilder();
		Properties properties = System.getProperties();
		for (Object o : properties.keySet()){
			builder.append(o).append("=").append(properties.get(o)).append("<br><br>\n\n");
		}
		return builder.toString();
	}
	private void makeRemotable(ProxyFactory proxyFactory) {
		if (proxyFactory != null) {
			InvokableImpl invokable = new InvokableImpl(this);
			proxyFactory.registerMethodReceiver(getId(), invokable);
		}
	}
	public String getUI() {
		return "<root>" +
		"<panel>" +
		"	<title label='" + "ResourceAgent" + "'/>"+
		"	<label label='space\t:Management'         padding='10'/>"+
		"	<label label=' --------------------' padding='30'/>"+
		"  <row2 spaceOneWidth='10' label='List Processes' labelWidth='200' buttonLabel='>>' buttonWidth='200' method='listProcesses' inputLabel='' taText='             ' outputHeight='100' />\n" +
		"  <row2 spaceOneWidth='10' label='Dump Threads' labelWidth='200' buttonLabel='>>' buttonWidth='200' method='dumpThreads' inputLabel='' taText='             ' outputHeight='200' />\n" +
		"  <row2 spaceOneWidth='10' label='Dump Heap' labelWidth='200' buttonLabel='>>' buttonWidth='200' method='dumpHeap' inputLabel='' taText='             ' outputHeight='200' />\n" +
		"  <row2 spaceOneWidth='10' label='DisplaySystemProperties' labelWidth='200' buttonLabel='>>' buttonWidth='200' method='displaySystemProperties' inputLabel='' taText='             ' outputHeight='200' />\n" +
		" </panel>" + 
		"</root>";

	}
	public String getUID() {
		return id;
	}
	public void registerServiceWithLookup(LookupSpace lookupSpace, ProxyFactory proxyFactory) {
		makeRemotable(proxyFactory);
		final ServiceInfo serviceInfo = new ServiceInfo(getId(), proxyFactory.getAddress().toString(), ResourceAgentAdminImpl.class.getName(), JmxHtmlServerImpl.locateHttpUrL(), "Agent:" + VSOProperties.getResourceType(), getId(), VSOProperties.getZone(), VSOProperties.getResourceType());
		lookupSpace.registerService(serviceInfo, -1);
	}
}
