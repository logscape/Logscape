package com.liquidlabs.log;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.log.alert.LogHandler;
import com.liquidlabs.log.alert.Schedule;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.search.ReplayEvent;
import junit.framework.TestCase;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeUtils;

import java.util.HashMap;
import java.util.Map;

public class LogHandlerTest extends TestCase {
	
	public void testShouldWriteStuff() throws Exception {
		
		Logger LOGGER = Logger.getLogger(Schedule.class);
		String fName = "";
		LogHandler logHandler = new LogHandler(fName, LOGGER, fName, fName, 0, "SCHEDULED Report was TRIGGER - detected from event on logHost:{hostname}{warn}");
		
		
		ReplayEvent event = new ReplayEvent("src", 100 , 1, 0, "sub", 100, "");
		
		FieldSet fieldSet = FieldSets.get();
		event.getCsv(fieldSet);
		event.getCsvHeader(fieldSet);
		event.toXMLReportString(DateUtil.shortDateTimeFormat6.print(DateTimeUtils.currentTimeMillis()), fieldSet);
		
		fieldSet = FieldSets.getBasicFieldSet();
		event.getCsv(fieldSet);
		event.getCsvHeader(fieldSet);
		event.toXMLReportString(DateUtil.shortDateTimeFormat6.print(DateTimeUtils.currentTimeMillis()), fieldSet);

		Map<Long, ReplayEvent> logEvents = new HashMap<Long, ReplayEvent>();
		int trigger = 10;
		int triggerCount = 10;
		logHandler.handle(event, logEvents, trigger, triggerCount);
		
		// test is to see if anything blows up on replacement - a bit crap yes
	}

}
