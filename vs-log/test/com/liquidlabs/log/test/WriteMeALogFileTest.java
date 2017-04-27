package com.liquidlabs.log.test;

import org.junit.Test;

public class WriteMeALogFileTest {
	
	String filename = "test-data/logSources/rainbow-src.log";
	String dir = "build";//"/Volumes/Media/weblogs";
	String format = "yyyy-MM-dd HH:mm:ss";
	
	
	@Test
	public void shouldWriteIt() throws Exception {
		
		LogLoadTester loadTester = new LogLoadTester(filename, dir + "/rainbow3.log", format, true);
		int multiplier = 1;//24;
		int twoHoursAsSeconds = multiplier * 2 * 60 * 60;
		int totalLines = multiplier * 250 * 1000;
		int ratePerSecond =  totalLines / twoHoursAsSeconds;
		loadTester.writeBurst(ratePerSecond, totalLines);

		
	}

}
