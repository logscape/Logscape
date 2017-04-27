package com.liquidlabs.log.alert;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.liquidlabs.log.search.ReplayEvent;

public class FileWriteHandler implements ScheduleHandler {
	
	private final static DateTimeFormatter dateF = DateTimeFormat.forPattern("yyyyMMdd");
	private final static DateTimeFormatter timeF = DateTimeFormat.forPattern("HHmm");
	private final String name;
	private final String reportName;
	private final Logger LOGGER;
	private final String TAG;
	boolean error;
	private final String copyAction;

	public FileWriteHandler(String tag, Logger logger, String name, String reportName, String copyAction){
		this.TAG = tag;
		this.LOGGER = logger;
		this.name = name;
		this.reportName = reportName;
		this.copyAction = copyAction;
		
	}

	public void handle(ReplayEvent event, Map<Long, ReplayEvent> logEvents, int trigger, int triggerCount) {
		if (copyAction != null && copyAction.length() > 0) {
			String copyName = getFilename(copyAction);
			try {
				// mkdir!
				String msg = String.format("%s Schedule[%s] Writing Report[%s] to output file[%s]", TAG, name, reportName, new File(copyName).getAbsolutePath());
				LOGGER.info(msg);
				File parent = new File(new File(copyName).getAbsolutePath()).getParentFile();
				if (!parent.exists()) parent.mkdirs();
				FileWriter fileWriter = new FileWriter(copyName, true);
				BufferedWriter writer = new BufferedWriter(fileWriter);
				
				writer.write(msg);
				writer.write("\r\n==============================================\r\n");
				writer.write(String.format("%s, %s, %d, %s\r\n", event.getHostname(),event.getFilePath(),event.getLineNumber(),event.getRawData()));
				
				ArrayList<ReplayEvent> sortedReplayEvents = sortReplayEvents(logEvents);
				
				writer.write(String.format("\r\n(%d) Events leading to trigger\r\n==============================================\r\n", sortedReplayEvents.size()));
				for (ReplayEvent replayEvent : sortedReplayEvents) {
					writer.write(String.format("%s, %s, %d, %s\r\n", replayEvent.getHostname(),replayEvent.getFilePath(),replayEvent.getLineNumber(),replayEvent.getRawData()));
				}
				
				writer.close();
			} catch (Exception e) {
				error = true;
				LOGGER.warn(String.format("%s Schedule[%s] Error writing Report[%s] to output file[%s]", TAG, name, new File(reportName).getAbsolutePath(), copyName), e);
			}
		}
	}
	public boolean isError() {
		return error;
	}
	String getFilename(String filename) {
		String time = timeF.print(new DateTime());
		String date = dateF.print(new DateTime());
		filename = filename.replaceAll("\\{time\\}",time);
		filename = filename.replaceAll("\\{date\\}",date);
		filename = filename.replaceAll("\\{search\\}",this.reportName);
		filename = filename.replaceAll("\\{schedule\\}",this.name);
		if (!filename.contains(".")) {
			filename = filename + ".csv";
		}
		return filename;
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
