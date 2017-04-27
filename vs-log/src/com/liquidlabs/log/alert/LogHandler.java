package com.liquidlabs.log.alert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;

import org.apache.log4j.Logger;

import com.liquidlabs.log.search.ReplayEvent;

public class LogHandler implements ScheduleHandler {
	
	private final String TAG;
	private final Logger LOGGER;
	private final String name;
	private final String reportName;
	private final String instruction;

	public LogHandler(String tag, Logger logger, String name, String reportName, int trigger, String instruction) {
		this.TAG = tag;
		this.LOGGER = logger;
		this.name = name;
		this.reportName = reportName;
		this.instruction = instruction;
	}

	public void handle(ReplayEvent event, Map<Long, ReplayEvent> logEvents, int trigger, int triggerCount) {
		try {
			String myInstruction = instruction;
			if (myInstruction != null && myInstruction.length() > 0) {
				if (myInstruction.contains("{hostname}")) {
					myInstruction = myInstruction.replace("{hostname}", event.getHostname());
				}
				if (myInstruction.contains("{host}")) {
					myInstruction = myInstruction.replace("{host}", event.getHostname());
				}
				if (myInstruction.contains("{line}")) {
					myInstruction = myInstruction.replace("{line}", event.getLineNumber().toString());
				}
				if (myInstruction.contains("{file}")) {
					myInstruction = myInstruction.replace("{file}", event.getFilePath());
				}
				if (myInstruction.contains("{filename}")) {
					myInstruction = myInstruction.replace("{filename}", event.getFilePath());
				}
				if (myInstruction.contains("{error}")) {
					LOGGER.error(myInstruction.replace("{error}", ""));
				} else if (myInstruction.contains("{warn}")) {
					LOGGER.warn(myInstruction.replace("{warn}", ""));
				} else if (myInstruction.contains("{warning}")) {
					LOGGER.warn(myInstruction.replace("{warning}", ""));
				} else if (myInstruction.contains("{fatal}")) {
					LOGGER.fatal(myInstruction.replace("{fatal}", ""));
				} else {
					LOGGER.info(myInstruction);
				}
				
				if (myInstruction.contains("{all}")) {
					ArrayList<ReplayEvent> eventsToWrite = sortReplayEvents(logEvents);
					for (ReplayEvent replayEvent : eventsToWrite) {
						LOGGER.info(">LogEvent:" + replayEvent.getHostname() + ", " + replayEvent.getFilePath() + ", " + replayEvent.getRawData());
					}
				}
			}
		} catch (Throwable t) {
			LOGGER.error("Failed to executed LogHandler:" + t.toString());
		}
	}

	private ArrayList<ReplayEvent> sortReplayEvents(Map<Long, ReplayEvent> logEvents) {
		ArrayList<ReplayEvent> eventsToWrite = new ArrayList<ReplayEvent>(logEvents.values());
		Collections.sort(eventsToWrite, new Comparator<ReplayEvent>() {
			public int compare(ReplayEvent o1, ReplayEvent o2) {
				return Long.valueOf(o1.getTime()).compareTo(o2.getTime());
			}
		});
		return eventsToWrite;
	}
}
