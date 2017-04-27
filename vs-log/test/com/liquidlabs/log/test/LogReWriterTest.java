package com.liquidlabs.log.test;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.file.raf.BreakRule;
import com.liquidlabs.common.file.raf.RAF;
import com.liquidlabs.common.file.raf.RafFactory;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

public class LogReWriterTest {
	
	
	private LogReWriter logReWriter;
	String filename = "stuff.log";
		
	String[] filenames = new String[] { 
					"C:\\work\\logs\\AccuWeather\\ax26_osuwww_access_2010-11-21.log",
					"C:\\work\\logs\\AccuWeather\\www_100709.esw3c_U.201011210000-2400-28",
					"C:\\work\\logs\\AccuWeather\\w3svc1_web-03_ex101121.log",
					"C:\\work\\logs\\AccuWeather\\w3svc1_widget-01_ex101121.log" };
	
	String dir = "C:\\work\\logs\\AccuWeather\\" + new DateTime().getMonthOfYear() + "-" + new DateTime().getDayOfMonth();
	//2011-01-20 10:33:35,989 INFO main (vso.VSOMain)	Thu Jan 20 10:33:35 GMT 2011
	String format = "yyyy-MM-dd HH:mm:ss,SSS";

	@Before
	public void setUp() throws Exception {
		logReWriter = new LogReWriter(format);
	}
	public String getName() {
		return this.getClass().getSimpleName();
	}

	@Test
    public void testFixThis() {}
	
//	@Test DodgyTest? Not really sure what the point is
	public void xtestShouldRewriteDirectory() throws Exception {
		
		FileOutputStream fos = new FileOutputStream("c:\\work\\logs\\daiwa\\sim-big.log");
		
		DateTime yesterday = new DateTime().minusDays(1);
		int total = 10 * 100000;
		int incremment = (1 * 24 * 60 * 60 * 1000)/total;
		long lastTime = -1;
		boolean isStarting = true;;
		for (int i = 0; i < 10 * 100000; i++) {
			long time = yesterday.getMillis() + (i * incremment);
			String Starting = "Starting";
			if (!isStarting) Starting = "Stopping";
			
			String timestamp = DateUtil.log4jFormat.print(time);
			String output = String.format("%s GMT Standard Time INFO [%d:%d] root - Daemon::initApp(): %s SIM\n", timestamp, lastTime, lastTime, Starting);
//			System.out.println(output);
			fos.write(output.getBytes());
			if (!isStarting) {
				lastTime = time;
			}
			isStarting = !isStarting;
			
		}
		fos.close();
		
	}
	
//	@Test DodgyTest? Too Pointless?
	public void testShouldReWriteToDestfile() throws Exception {
		
		if (true) return;
		
		for (String filename : filenames) {
				String outfile = dir + "\\" + new File(filename).getName();
				
				new File(dir).mkdir();
				//int linesWritten = logReWriter.rewrite(outfile, filename, 0);
				
				// simple version....
				OutputStream fos = new BufferedOutputStream(new FileOutputStream(outfile));
				RAF raf = RafFactory.getRaf(filename, BreakRule.Rule.SingleLine.name());
				String line = "";
				while ((line = raf.readLine()) != null) {
					//21/Nov
					String newLine = line;
					newLine = newLine.replaceFirst("22\\/Nov", "03\\/Feb");
					newLine = newLine.replaceFirst("21\\/Nov", "02\\/Feb");
					newLine = newLine.replaceFirst("11-21", "02-03");
					newLine = newLine.replaceFirst("11-20", "02-02");
					newLine = newLine.replaceAll("2010", "2011");
					newLine += "\n";
					fos.write(newLine.getBytes());
				}
				
				raf.close();
				fos.close();
		}
		
//		assertTrue(linesWritten > 1);
//		assertTrue(new File(outfile).exists());
		
	}
	
	public void xtestShouldGetElapsedTimeMins() throws Exception {
		int mins = logReWriter.getElapsedMins(filename);
		Assert.assertTrue(mins / (24.0 * 60.0) > 4);
		System.out.println("ElapsedMins:" + mins);
		System.out.println("ElapsedDays:" + mins / (24.0 * 60.0));
		
	}
	public void xtestShouldGetLogEndTime() throws Exception {
		DateTime time  = logReWriter.getLogEndTime(filename);
		
		Assert.assertTrue(time.getDayOfYear() != new DateTime().getDayOfYear());
		System.out.println("End Time:" + time);
	}
	public void xtestShouldGetLogStartTime() throws Exception {
		DateTime time  = logReWriter.getLogStartTime(filename);
		
		Assert.assertTrue(time.getDayOfYear() != new DateTime().getDayOfYear());
		System.out.println("Start Time:" + time);
	}

}
