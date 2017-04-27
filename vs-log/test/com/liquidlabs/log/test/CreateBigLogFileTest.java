package com.liquidlabs.log.test;

import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.space.LogSpace;
import com.liquidlabs.log.space.WatchDirectory;
import org.jmock.Mockery;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.text.SimpleDateFormat;

import static junit.framework.Assert.assertTrue;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class CreateBigLogFileTest  {
	
	
	private Mockery	context = new Mockery();
	private LogSpace logSpace;
	private Indexer indexer;
	private WatchDirectory watch;
//	private DefaultPatternIndexer patternIndexer;
	private LogLoadTester loadTester;

    @Test
    public void doSweetFuckAll() {}

    @Before
	public  void setUp() throws Exception {
		FileUtil.mkdir("build/bigLog");
//		loadTester = new LogLoadTester("test-data/trade-load-template.log", "build/bigLog/test1.log", "yy-MM-dd HH:mm:ss,SSS",false);
		loadTester = new LogLoadTester("test-data/trade-load-template.log", "../master/build/vscape/test.log", "yyyy-MM-dd HH:mm:ss,SSS", false);
//		loadTester = new LogLoadTester("test-data/trade-load-template.log", "/Users/neil/Downloads/apps/IRDvar/intarch/environments/gdscapacity/cachemgr/coherence/test.log", "yyyy-MM-dd HH:mm:ss,SSS", false);
	}
	
	@Test
	public void testShouldCalculateStartTimeForLPS() throws Exception {
		DateTime dateTime = new DateTime();
		DateTime time = loadTester.getStartTimeFor(10, 60 * 10);
		int seconds = dateTime.getSecondOfDay() - time.getSecondOfDay();
		assertThat(seconds, is(60));
	}

	public void testShouldWriteBurst() throws Exception {
// 1meelion lines ish (966k)	
		// 276M 966K lines
		int Size276M = 10 * 4 ;//* (5 * 600 * 10);
		for (int i = 0; i < 10; i++) {
			System.out.println("i:" + i);
			String file = String.format("build/bigLog/test%d.log", i);
			loadTester.setDest(file);
			int  written = loadTester.writeBurst(10,  Size276M * 4);
		}
		
// 300k lines (ish)		
//		int  written = loadTester.writeBurst(10,  2 * 4 * (5 * 600 * 10));
//		assertTrue(written > 0);
	}
	
	public void testShouldWriteTailForTimePeriod() throws Exception {
		System.err.println("Starting");
		int written = loadTester.writeTail(10,   2);//999999 * 1000);
		System.err.println("Done");
		assertTrue(written > 0);
		
	}
	
	public void testBurstFileForTime() throws Exception {
		
		
		
		File file = new File("test-data/biglog.log");
		if (file.exists()) file.delete();
		FileOutputStream fos = new FileOutputStream(file);
		
		SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS,sss");
		
		long start = DateTimeUtils.currentTimeMillis();
		

		DateTime dateTime = new DateTime();
		DateTime minusHours = dateTime.minusHours(4);
		
		
		
		int total = 0;
//		for (int mins = 10; mins < 41; mins++){
//			for (int secs = 10; secs < 60; secs++){
//				for (int ms = 100; ms < 1000; ms++){
//					
////					fos.write(String.format("2009-01-01 %d:%d:%d,%d this is a log line of stuff\n", 10,mins,secs,ms).getBytes());
////					String stuff = String.format("2009-01-01 %d:%d:%d,%d this is a log line of stuff\n", 10,mins,secs,ms);
//					String stuff = String.format("2009-01-01 %d:%d:%d,%d", 10,mins,secs,ms);
////					Date parse = formatter.parse(stuff);
////					DateTime parse = parser.parseDateTime(stuff);
//					total++;
//					
//				}
//			}
//		}
		long end = DateTimeUtils.currentTimeMillis();
		long elapsed = end - start;
//		System.out.println("Elapsed:" + elapsed + " perSec" + (total/elapsed)  );
//		fos.close();
		
	}
}
