package com.liquidlabs.log.streamer;

import com.liquidlabs.common.file.raf.RAF;
import com.liquidlabs.common.file.raf.RafFactory;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class LogLineTest {
	
	
	@Test
	public void shouldReadFileGood() throws Exception {
		//RAF rafSingleLine = RafFactory.getRafSingleLine("C:\\work\\logs\\RawSplunkLogs\\11Feb10.log");
		RAF rafSingleLine = RafFactory.getRafSingleLine("test-data/agent.log");
		String line = "";
		int lines = 0;
		while ((line = rafSingleLine.readLine()) != null) {
			LogLine logLine = new LogLine(100, "TIME", 100, line);
//			System.out.println(logLine.text);
			lines++;
			assertNotNull(logLine.text);
			assertTrue(logLine.text.length() > 0);
		}
		
	}

}
