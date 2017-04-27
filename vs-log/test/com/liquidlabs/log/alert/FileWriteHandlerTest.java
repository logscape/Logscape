package com.liquidlabs.log.alert;

import com.liquidlabs.log.search.ReplayEvent;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class FileWriteHandlerTest {
	private final static Logger LOGGER = Logger.getLogger(FileWriteHandlerTest.class);

	@After
	public void after() {
		new File("build/FileWriterTestDIR/testReportName.csv").delete();
		new File("build/FileWriterTestDIR").delete();
		new File("testReportName.csv").delete();
	}
	@Test
	public void shouldWriteFileGoodWhenDirsDontExist() throws Exception {
		
		FileWriteHandler handler = new FileWriteHandler("TAG", LOGGER, "testAlert", "testReportName", "build/build/FileWriterTestDIR/testReportName");
		Map<Long, ReplayEvent> logEvents = new HashMap<Long, ReplayEvent>();
		ReplayEvent event = new ReplayEvent();
		event.setDefaultFieldValues("type", "host", "filename", "path", "tag", "agentType", "", "0");
		handler.handle(event, logEvents, 10, 10);
		Assert.assertFalse(handler.isError());
	}
	@Test
	public void shouldWriteFileGoodWhenDirsExist() throws Exception {
		
		FileWriteHandler handler = new FileWriteHandler("TAG", LOGGER, "testAlert", "testReportName",  "testReportName");
		Map<Long, ReplayEvent> logEvents = new HashMap<Long, ReplayEvent>();
		ReplayEvent event = new ReplayEvent();
		event.setDefaultFieldValues("type", "host", "filename", "path", "tag", "agentType", "", "0");
		handler.handle(event, logEvents, 10, 10);
		Assert.assertFalse(handler.isError());
	}

}
