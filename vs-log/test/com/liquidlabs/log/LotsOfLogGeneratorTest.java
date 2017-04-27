package com.liquidlabs.log;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.log.test.LogLoadTester;
import org.joda.time.DateTime;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LotsOfLogGeneratorTest {
	
	
	@Test
	public void shouldWriteLOTSOfDataToFile() throws Exception {
	if (true) return;
		FileOutputStream fos = new FileOutputStream("//Volumes/Media/LOGSCAPE/LOGSCAPE_1.3/master/build/logscape/big.log");
		int num = 0;
		while (num < 100 * 100 * 1000) {
			for (String line : this.lines) {
				fos.write((DateUtil.log4jFormat.print(System.currentTimeMillis()) + line + "\n").getBytes());
				Thread.sleep(10);
				if (num++ % 100 == 0) {
					System.out.println(new DateTime() + " Count:" + num);
				}
			}
		}
	}
	String[] lines = 
			(" INFO orm-SHARED-5-1 (netty.NettyEndPoint)	 Stats:SHARED/stcp://192.168.70.180:11022 Send[0msg 0kb] Recv[0msg 0kb]\n" + 
			" INFO PF-SHARED-9-5 (resource.ResourceSpace)	RenewAllocLeasesFor:BundleSvcAlloc11000-alteredcarbon.local count:3\n" + 
			" WARN PF-SHARED-9-5 (admin.AdminSpaceImpl)	Invalid License:29-Nov-2012 10:20:32zkbtrial.lic\n" + 
			" INFO PF-SHARED-9-5 (admin.AdminSpaceImpl)	Licenses found:8 processed:7\n" + 
			" INFO PF-SHARED-9-5 (admin.AdminSpaceImpl)	Apply Limit:3165\n" + 
			" INFO transport-128-1 (netty.NettyEndPoint)	 Stats:AgentLogService/stcp://192.168.70.180:11052 Send[34msg 17kb] Recv[0msg 0kb]\n" + 
			" INFO orm-SHARED-5-1 (netty.NettyEndPoint)	 Stats:SHARED/stcp://192.168.70.180:11003 Send[20msg 10kb] Recv[48msg 16kb]\n" + 
			" INFO orm-LookupSpace-20-1 (netty.NettyEndPoint)	 Stats:LookupSpace/stcp://192.168.70.180:11000 Send[55msg 20kb] Recv[19msg 6kb]\n" + 
			" INFO orm-LookupSpace-20-1 (netty.NettyEndPoint)	 Stats:LookupSpace/stcp://192.168.70.180:15000 Send[0msg 0kb] Recv[0msg 0kb]\n" + 
			" INFO PF-LookupSpace-24-4 (lookup.LookupSpace)	LookupSpace alteredcarbon.local CPU:14 MemFree:546 MemUsePC:2.96 DiskFree:781016 DiskUsePC:18.09 SwapFree:352 SwapUsePC:31.25\n" + 
			" INFO PF-LookupSpace-24-4 (lookup.LookupSpace)	LookupSpace alteredcarbon.local MEM MB MAX:1011 COMMITED:253 USED:67 AVAIL:943\n" + 
			" INFO PF-SHARED-9-3 (resource.ResourceSpace)	ResourceSpace alteredcarbon.local CPU:14 MemFree:545 MemUsePC:2.96 DiskFree:781016 DiskUsePC:18.09 SwapFree:352 SwapUsePC:31.25\n").split("\n");

	@Test
	public void shouldWriteAFileEveryTwoMins() throws Exception {
		
		ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
		int initialDelay = 60 - new DateTime().getSecondOfMinute();
		System.out.println("initial delay:" + initialDelay);
		scheduler.scheduleAtFixedRate(new Runnable() {
			public void run() {
				try {
					int minuteOfHour = new DateTime().getMinuteOfHour();
					int minPart =  10 * (int)( minuteOfHour/10);
					int offset = minuteOfHour - minPart;
					String outFile = "d:/work/logs/ROLLING/2testRoll" + offset + ".log";
					System.out.println(new DateTime() + " OUTFIle:" + outFile);
					FileOutputStream fos = new FileOutputStream(outFile);
					for (int i = 0; i <119; i++) {
//						2012-08-16 13:04:33,163
						fos.write((DateUtil.log4jFormat.print(System.currentTimeMillis()) + " INFO main (vso.VSOMain) VSOMain ENTRY:" + i + "\n").getBytes());
						Thread.sleep(1000);
					}
					fos.close();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
		}, initialDelay, 120, TimeUnit.SECONDS);
		//Thread.sleep(Long.MAX_VALUE);
		
	}

    @Test
    public void shouldPretendToBeATest() {}
	@Test
	public void testShouldWrite1WeeksWorthOfLogs() throws Exception {
		String destBase = "../master/bigLog-1.0/logs/";
//		String sourceFile = "../master/build/logscape/work/agent.log";
		String sourceFile = "../vs-log/test-data/agent.log";
		
		FileUtil.deleteDir(new File(destBase));
		int ratePerSecond = 1;//5;
		
		int devices = 1;//0;
		int rolls = 1;
		
		for (int i = 0; i < rolls; i++) {
			DateTime endTime = new DateTime().minusDays(rolls - i);
			// roll to the end of the day
			endTime = endTime.plusHours(24 - endTime.getHourOfDay());
			for (int j = 0; j < devices; j++) {
				FileUtil.mkdir(destBase + i);
				String dest = String.format("%s%d/log-%d-%d.log",destBase,i,endTime.getDayOfMonth(),j);
				LogLoadTester loadTester = new LogLoadTester(sourceFile, dest, "yyyy-MM-dd HH:mm:ss,SSS", true, endTime);
				System.err.println(i + " Writing:" + j + " " + dest);
				int written = loadTester.writeBurst(ratePerSecond, devices * (60 * 60 * 24));
				System.out.println("wrote:" + written);
			}
		}
		
	}

}
