package com.liquidlabs.rawlogserver.handler;

import com.liquidlabs.common.file.raf.BreakRule;
import com.liquidlabs.common.file.raf.RAF;
import com.liquidlabs.common.file.raf.RafFactory;
import com.liquidlabs.rawlogserver.handler.SplunkPacketizer.SplunkPacket;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class SplunkPacketTest {
	@Test
	public void shouldScanAFewLines() throws Exception {
		
//		RAF raf = RafFactory.getRaf("test-data/SplunkCookedASASession-raw-11May09.log");
		RAF raf = RafFactory.getRaf("test-data/SpunkCookedOSSEC-raw-11May09.log", BreakRule.Rule.SingleLine.name());
		raf.setBreakRule("Explicit:_time");
		String line = raf.readLine();
		int count = 0;
		while ((line = raf.readLine()) != null && count < 10) {
			SplunkPacket packet = new SplunkPacket(line.getBytes());
			if (packet.raw.length() == 0) continue;
			count++;
//			System.out.println("------------------------\n" + packet);
		}
	}

	@Test
	public void shouldCreatePacket() throws Exception {
		RAF raf = RafFactory.getRaf("test-data/SplunkCookedASASession-raw-11May09.log", BreakRule.Rule.SingleLine.name());
		String line = raf.readLine();
		line = raf.readLine();
		line = raf.readLine();
		SplunkPacket packet = new SplunkPacket(line.getBytes());
		String string = packet.toString();
		assertTrue(string.contains("host:192.168.3.1"));
	}


	@Test
	public void shouldExtractSourceType() throws Exception {
		RAF raf = RafFactory.getRaf("test-data/SplunkCookedASASession-raw-11May09.log", BreakRule.Rule.SingleLine.name());
		String line = raf.readLine();
		line = raf.readLine();
		line = raf.readLine();
		System.out.println(line);
		String extractWord = new SplunkPacket(new byte[0]).extractWord("sourcetype".getBytes(),"\0\0\0".getBytes(), line.getBytes());
		System.out.println("word:" + extractWord);
		assertTrue(extractWord.startsWith(":udp:514"));
	}
//	@Test DodgyTest? the line doesn't have May 9 15:01:41 on it, so i'm not sure how we can have that assertion
	public void shouldExractWord() throws Exception {
		RAF raf = RafFactory.getRaf("test-data/SplunkCookedASASession-raw-11May09.log", BreakRule.Rule.SingleLine.name());
		raf.readLine();
		raf.readLine();
		String line = raf.readLine();
		System.out.println(line);
		System.out.println("RAW:" + line.indexOf("_raw"));
		String extractWord = new SplunkPacket(new byte[0]).extractWord("_raw\0\0\0\0".getBytes(), "\0\0\0".getBytes(), line.getBytes());
		System.out.println("word:" + extractWord);
		assertTrue(extractWord.contains("May  9 15:01:41 192.168.3.1"));
	}

}
