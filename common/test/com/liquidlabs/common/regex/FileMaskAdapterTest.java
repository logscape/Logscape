package com.liquidlabs.common.regex;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class FileMaskAdapterTest {
	
	@Test
	public void shouldNotFailWithTrailingDot() throws Exception {
		evaluate(".*\\.", "*.", false);
		evaluate(".*", ".*", false);
		
	}
	@Test
	public void shouldDoWildCardsOnFilenames() throws Exception {
		
		evaluate(".*\\.log", "*.log", false);
		evaluate(".*\\.log", "*.log", true);
		
		
		evaluate("C:\\.*\\.log", "C:\\*.log", true);
		
		evaluate(".*agent\\.log", "*agent.log", false);
		
		evaluate("/Volumes/Media/weblogs.*", "/Volumes/Media/weblogs*", false);
		
		evaluate(".*ODCLog\\.log,.*coherence.*\\.log", ".*ODCLog.log,.*coherence.*.log", false);
		
		
		evaluate(".*\\.log,not(.*stuff.*)", "*.log,not(*stuff*)", false);
	}
	@Test
	public void shouldExpandWildCards() throws Exception {
		
		assertEquals(".*stuff.*log", FileMaskAdapter.handleWildcard("*stuff*log"));
		assertEquals("stuff\\.log", FileMaskAdapter.handleDot("stuff.log"));
		assertEquals("stuff.*log", FileMaskAdapter.handleDot("stuff.*log"));
		assertEquals("stuff\\.log", FileMaskAdapter.handleDot("stuff\\.log"));
		
		// escapse the first dot!
		assertEquals(".*/stuff", FileMaskAdapter.handleDot("./stuff"));
		
	}
	
	@Test
	public void shouldRegExpMatch() throws Exception {
		
		evaluate("E:\\\\Files-stuff\\\\logs.*log","E:\\Files-stuff\\logs.*log", true);
		
		evaluate(".*work.*evt", "*work*evt", true);
		
		evaluate(".*\\\\work.*\\.evt", "*/work.*\\.evt", true);
		
		evaluate(".*work.*", "*work*",false);
		
		
		evaluate(".*\\\\work", "./work", true);
		evaluate(".*/work", "./work", false);
		evaluate(".*/work", ".*/work", false);
		
	}

	private void evaluate(String expects, String evaluateThis, boolean isWindows) {
		String fixed = FileMaskAdapter.adapt(evaluateThis, isWindows);
		
		assertEquals(expects, fixed);

		
	}

//	@Test
//	public void shouldAddStarToTheStart() throws Exception {
//		String dir = ".work.*log";
//		String fixed = FileMaskAdapter.fixAgainstLeadingDots(dir);
//		assertEquals(".*work.*log", fixed);
//		
//		String dir2 = ".*work.*log";
//		String fixed2 = FileMaskAdapter.fixAgainstLeadingDots(dir2);
//		assertEquals(".*work.*log", fixed2);		
//	}



}
