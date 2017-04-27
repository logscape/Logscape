package com.liquidlabs.vso.container.sla;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;

import com.liquidlabs.vso.container.Action;
import com.liquidlabs.vso.container.Consumer;
import com.liquidlabs.vso.container.Metric;
import com.liquidlabs.vso.container.NullAction;

public class Rule {
	public static final String IMPORT_COM_VSCAPE_CONTAINER_ALL = "import com.liquidlabs.vso.container.*;\n";
	private static final String IMPORT_ORG_APACHE_LOG4J_LOGGER_NULL = "import org.apache.log4j.Logger;\n";
	
	transient private final static Logger LOGGER = Logger.getLogger(Rule.class);
	transient private Logger scriptLogger;
	
	private String script;
	int maxResources = 10;
	int priority = 10;
	private List<String> resourceGroups = new ArrayList<String>();
	
	public Rule() {
	}
	public Rule(String script, int maxResources, int priority) {
		this.script = script;
		this.maxResources = maxResources;
		this.priority = priority;
	}
	
	String update(String script) {
		String imports = "";
		if (!script.contains(IMPORT_COM_VSCAPE_CONTAINER_ALL)) {
			imports = IMPORT_COM_VSCAPE_CONTAINER_ALL;
		}
		if (!script.contains(IMPORT_ORG_APACHE_LOG4J_LOGGER_NULL)) {
			imports = IMPORT_ORG_APACHE_LOG4J_LOGGER_NULL + imports;
		}
		if (!script.contains("org.joda.time")) {
			String importJoda = "import org.joda.time.*\n";
			imports = importJoda + imports;  
		}
		
		if (!script.contains("def getTime(hhmm)")) {

			String getTime =  
			" def getTime(hhmm) {\n"+
			"	DateTime time = new DateTime()\n" +
			" 	time = time.withHourOfDay(Integer.valueOf(hhmm.split(\":\")[0]))\n"+
			"	time = time.withMinuteOfHour(Integer.valueOf(hhmm.split(\":\")[1]))\n"+
			"	return time.getMillis()\n" +
			"}\n"; 		
			script = getTime + script;
		}
		
		return imports + script;
	}

	public Action evaluate(Consumer consumer, Metric [] metrics) {
		
		String msg = "";
		Binding binding = bind(consumer, metrics);
		
		GroovyShell shell = new GroovyShell(binding);
		try {
			String script = update(this.script);

			Action action = (Action)shell.evaluate(script);
			if (action != null) {
				action.setPriority(priority);
				if (resourceGroups == null) resourceGroups = new ArrayList<String>();
				action.setResourceGroups(new ArrayList<String>(resourceGroups));
				return action;
			}
			return new NullAction("No Rules fired");
		} catch (Throwable t) {
			if (this.scriptLogger != null) scriptLogger.error(String.format("Failed to execute script\n\n%s\n",script), t);
			LOGGER.error(String.format("Failed to execute script\n\n%s\n",script), t);
//			msg = t.getMessage();
			throw new RuntimeException(t);
		}
		
	}

	private Binding bind(Consumer consumer, Metric[] metrics) {
		Binding binding  = new Binding();
		for (Metric metric : metrics) {
			binding.setVariable(metric.name(), metric.value());
		}
		if (this.scriptLogger != null) {
			binding.setVariable("LOGGER", this.scriptLogger);
			binding.setVariable("Logger", this.scriptLogger);
			binding.setVariable("logger", this.scriptLogger);
			binding.setVariable("LOG", this.scriptLogger);
			binding.setVariable("Log", this.scriptLogger);
			binding.setVariable("log", this.scriptLogger);
			binding.setVariable("sysout", this.scriptLogger);

		}
		DateTime currentTime = new DateTime(DateTimeUtils.currentTimeMillis());
		binding.setVariable("currentTime", currentTime.getMillis());
		binding.setVariable("now", currentTime.getMillis());
		binding.setVariable("currentDateTime", Calendar.getInstance().getTime());
		binding.setVariable("nowDateTime", Calendar.getInstance().getTime());
		binding.setVariable("consumer", consumer);
		return binding;
	}

	public void setMaxResources(int maxResources) {
		this.maxResources = maxResources;
		
	}
	public int getMaxResources() {
		return this.maxResources;
	}

	public int getPriority() {
		return priority;
	}

	public void setPriority(int priority) {
		this.priority = priority;
	}

	public void setScriptLogger(Logger scriptLogger) {
		this.scriptLogger = scriptLogger;
	}

	public List<String> getResourceGroups() {
		return resourceGroups;
	}

	public String getScript() {
		return script;
	}

}
