package com.liquidlabs.rawlogserver;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.file.FileUtil;
import org.joda.time.DateTimeUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertTrue;

public class RawLogServerTest {
	
	
	private RawLogServer rawLogServer;
	private File destinationFile;

	@Before
	public void setup() {
		System.setProperty("raw.server.fq.delay.secs", "1");
		rawLogServer = new RawLogServer("build/RawTest");
		FileUtil.deleteDir(new File("build/RawTest/"));
		rawLogServer.start();
	}
	
	@Test
	public void shouldDumpRawData() throws Exception {
		rawLogServer.receive("line 1\nline 1.1.1.1\n".getBytes(), "remoteAddress1", "remoteHost1");
		// check the file was created
		Thread.sleep(1500);
		assertTrue(new File("build/RawTest/remoteHost1/remoteAddress1/raw-" +  DateUtil.shortDateFormat.print(DateTimeUtils.currentTimeMillis()) + ".log").exists());
	}
	
	@Test
	public void testShouldStoreFileData() throws Exception {
		
		destinationFile = new File("build/RawTest/remoteHost2/remoteAddress2/unknown-" + DateUtil.shortDateFormat.print(DateTimeUtils.currentTimeMillis()) + ".log");
		destinationFile.delete();
		rawLogServer.receive("line 1\nline 1.1.1.1\n".getBytes(), "remoteAddress2", "remoteHost2");
		rawLogServer.receive("line 2\n".getBytes(), "remoteAddress2", "remoteHost2");
		rawLogServer.receive("line 3\n".getBytes(), "remoteAddress2", "remoteHost2");
		// wait so it flushed properly
		Thread.sleep(4000);
		
		Assert.assertTrue(destinationFile.exists());
		String readAsString = FileUtil.readAsString(destinationFile.getAbsolutePath());
		System.out.println(readAsString);
		
		Assert.assertEquals(4, FileUtil.countLines(destinationFile)[1]);
	
	}

}
