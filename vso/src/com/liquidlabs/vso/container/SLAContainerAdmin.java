package com.liquidlabs.vso.container;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

import com.liquidlabs.vso.container.sla.Rule;
import com.liquidlabs.vso.container.sla.SLA;
import com.liquidlabs.vso.container.sla.SLASerializer;
import com.liquidlabs.vso.resource.ResourceSpace;

public class SLAContainerAdmin implements SLAContainerAdminMBean {

	private static final Logger LOGGER = Logger.getLogger(SLAContainerAdmin.class);
	private SLAContainer container;
	private final ResourceSpace resourceSpace;
	private final String bundleName;
	private final String serviceToRun;
	private final String serviceCriteria;
	private final Consumer consumer;
	private final String notifyFilterForSLAEventWire;

	public SLAContainerAdmin(SLAContainer container, Consumer consumer, ResourceSpace resourceSpace, String bundleName, String serviceToRun, String notifyFilter, String serviceCriteria) {

		this.container = container;
		this.consumer = consumer;
		this.resourceSpace = resourceSpace;
		this.bundleName = bundleName;
		this.serviceToRun = serviceToRun;
		this.serviceCriteria = serviceCriteria;
		this.notifyFilterForSLAEventWire = notifyFilter;
		try {
			MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
			ObjectName objectName = new ObjectName("com.liquidlabs.vso.container:type=Admin");
			if (mbeanServer.isRegistered(objectName)) return;
			mbeanServer.registerMBean(this, objectName);
		} catch (Exception e) {
			LOGGER.error(e);
		}
	}
	public void setContainer(SLAContainer container) {
		this.container = container;
	}
	

	public String displaySLAContent() {
		try {
			return new SLASerializer().serialize(container.getSla());
		} catch (Throwable t) {
			LOGGER.error("Failed to get SLA:" + t, t);
			return "Failed to extract SLA due to error:" + t;
		}
	}
	public String updateSLA(String sla) {
		
		try {
			SLASerializer serializer = new SLASerializer();
			SLA newSLA = serializer.deSerialize(null, sla);
			return this.container.setSLA(newSLA, bundleName);
		} catch (Throwable t){
			LOGGER.error("Cannot update SLA", t);
			LOGGER.error(sla);
			throw new RuntimeException(t);
		}
	}
	public String validateSLA(String sla) {
		
		try {
			return this.container.validateSLA(sla);
		} catch (Throwable t){
			LOGGER.error("Cannot update SLA", t);
			LOGGER.error(sla);
			throw new RuntimeException(t);
		}
	}
	public String resetSLAStatus() {
		this.container.resetStatusToRunning();
		return "done";
	}
	
	public String triggerSLAAction(String script, int priority){
		try {
			Metric[] metrics = this.consumer.collectMetrics();
			
			Binding binding = bind(metrics);
			
			StringBuilder stringBuilder = new StringBuilder();
			stringBuilder.append(Rule.IMPORT_COM_VSCAPE_CONTAINER_ALL).append("\n");
			stringBuilder.append(script).append("\n");
			
			GroovyShell shell = new GroovyShell(binding);
			Action action = (Action)shell.evaluate(stringBuilder.toString());
			if (action != null) {
				action.setPriority(priority);
				LOGGER.info(container.getOwnerId() + " Execute Action:" + action);
				action.perform(resourceSpace, container.getOwnerId(), consumer, this.getServiceToRun(), bundleName, this.container);
			}
		} catch (Throwable t) {
			LOGGER.error(String.format("Failed to execute script\n\n%s\n",script), t);
			return "Operation failed:" + t.getMessage();
		}
		
		
		return "Operation was performed";
	}
	private Binding bind(Metric[] metrics) {
		Binding binding  = new Binding();
		for (Metric metric : metrics) {
			binding.setVariable(metric.name(), metric.value());
		}
		return binding;
	}

	public String getNotifyFilter() {
		return notifyFilterForSLAEventWire;
	}

	public void handleLogEvent(String message) {
		container.handleMessage(message);
	}

	public String status() {
		return container.getServiceStatus().toLongString();
		
	}

	public String getServiceToRun() {
		return serviceToRun;
	}
	public String getServiceCriteria() {
		return serviceCriteria;
	}

}
