package com.liquidlabs.log.alert;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;

import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.ReplayEvent;

public class GroovyHandler implements ScheduleHandler {

	private static final String IMPORT_ORG_APACHE_LOG4J_LOGGER_NULL = "import org.apache.log4j.Logger;\n";
	private final String TAG;
	private final Logger LOGGER;
	private final String name;
	private final String reportName;
	private final String scriptAction;
	private final List<FieldSet> fieldSets;

	public GroovyHandler(String tag, Logger logger, String name, String reportName, String scriptAction, List<FieldSet> fieldSets) {
		this.TAG = tag;
		this.LOGGER = logger;
		this.name = name;
		this.reportName = reportName;
		this.scriptAction = scriptAction;
		this.fieldSets = fieldSets;
	}

	public void handle(ReplayEvent event, Map<Long, ReplayEvent> logEvents, int trigger, int triggerCount) {
		if (scriptAction != null && scriptAction.trim().length() > 0) {
			try {
				LOGGER.info(TAG + " GroovyHandler Executing:" + name);
				Binding binding = bind(event, logEvents, name, reportName, trigger, triggerCount);
				GroovyShell shell = new GroovyShell(binding);
				String script = update(scriptAction);
				shell.evaluate(script);
			} catch (Throwable t) {
				LOGGER.warn(TAG + " GroovyHandler failed schedule:" + name,t);
			}
		}
	}

	String update(String script) {
		String imports = "";
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

	private Binding bind(ReplayEvent lastEvent, Map<Long,ReplayEvent> logEvents, String name, String reportName, int trigger, int triggerCount) {
		
		lastEvent.populateFieldValues(new HashSet<String>(), fieldSets);
		
		Binding binding  = new Binding();
		List<ReplayEvent> sortedEventsRE = new ArrayList<ReplayEvent>(logEvents.values());
		Collections.sort(sortedEventsRE, new Comparator<ReplayEvent>(){
			public int compare(ReplayEvent o1, ReplayEvent o2) {
				return Long.valueOf(o1.getTime()).compareTo(o2.getTime());
			}
		});
		
		List<Map<String,String>> sortedMap = new ArrayList<Map<String, String>>();
		
		
		List<String> textEvents = new ArrayList<String>();
		for (ReplayEvent replayEvent : sortedEventsRE) {
			replayEvent.populateFieldValues(new HashSet<String>(), fieldSets);
			textEvents.add(replayEvent.toString());
			sortedMap.add(replayEvent.keyValueMap);
		}
		binding.setVariable("event", lastEvent);
		binding.setVariable("textEvents", textEvents);
		binding.setVariable("allEvents", logEvents);
		binding.setVariable("sortedEvents", sortedEventsRE);
		binding.setVariable("sortedMap", sortedMap);
		binding.setVariable("name", name);
		binding.setVariable("triggerSearch", reportName);
		binding.setVariable("trigger", trigger);
		binding.setVariable("triggerCount", triggerCount);
		
		binding.setVariable("LOGGER", LOGGER);
		binding.setVariable("Logger", LOGGER);
		binding.setVariable("logger", LOGGER);
		binding.setVariable("LOG", LOGGER);
		binding.setVariable("Log", LOGGER);
		binding.setVariable("log", LOGGER);
		binding.setVariable("sysout", LOGGER);
		DateTime currentTime = new DateTime(DateTimeUtils.currentTimeMillis());
		binding.setVariable("currentTime", currentTime.getMillis());
		binding.setVariable("now", currentTime.getMillis());
		binding.setVariable("currentDateTime", Calendar.getInstance().getTime());
		binding.setVariable("nowDateTime", Calendar.getInstance().getTime());
		return binding;
	}

}
