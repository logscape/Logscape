package com.liquidlabs.log.links;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class LinkTest  {
	
	@Test
	public void shouldMergeLinks() throws Exception {
		String[] lines = new String[] {
				"2012-06-08 20:22:02,913 INFO agent-local-sched-12-1 (StatsLogger)	AGENT Chronos-11003 <FONT COLOR='#0000FF'><b><a href='event:_search:System Utilization CPU!0'>CPU</a></b></FONT>:0 MemFree:5128 MemUsePC:63.27 DiskFree:427462 DiskUsePC:23.50 SwapFree:4095 SwapUsePC:0.00",
				"2012-06-08 20:22:02,913 INFO agent-local-sched-12-1 (StatsLogger)	AGENT Chronos-11003 CPU:0 MemFree:5128 <FONT COLOR='#0000FF'><b><a href='event:_search:System Utilization MEM!63.27'>MemUsePC</a></b></FONT>:63.27 DiskFree:427462 DiskUsePC:23.50 SwapFree:4095 SwapUsePC:0.00"
		};
		String merge = new Link().merge(Arrays.asList(lines[0], lines[1]));
		System.out.println(merge);
		String match = "2012-06-08 20:22:02,913 INFO agent-local-sched-12-1 (StatsLogger)	AGENT Chronos-11003 <FONT COLOR='#0000FF'><b><a href='event:_search:System Utilization CPU!0'>CPU</a></b></FONT>:0 MemFree:5128 <FONT COLOR='#0000FF'><b><a href='event:_search:System Utilization MEM!63.27'>MemUsePC</a></b></FONT>:63.27 DiskFree:427462 DiskUsePC:23.50 SwapFree:4095 SwapUsePC:0.00";
		assertEquals(match, merge);
		
	}
	@Test
	public void shouldAllow2LinksToWork() throws Exception {
		Link link1 = new Link("DoLogSearch1", "(*) user:(*) action:(*)", "DoLogSearch1", 2, 2);
		Link link2 = new Link("DoLogSearch2", "(*) user:(*) action:(*)", "DoLogSearch2", 3, 3);
		
		String line = "another user:Alan action:logoff";
		
		String line1 = link1.handle(line);
		String line2 = link2.handle(line);
		
		
		System.out.println("Parts:" + link1.getLineParts(line1));
		System.out.println("Parts:" + link2.getLineParts(line2));
		System.out.println("Parts:" + link2.getLineParts("another user:Alan action:<b><a href='event:Action Activity!reboot'>reboot</a></b>"));
		
		System.out.println("line0:" + line);
		System.out.println("line1:" + line1);
		System.out.println("line2:" + line2);
		
		String r = link1.merge(Arrays.asList(link1, link2), line);
		System.out.println("Merged:" + r);
		assertTrue(r.contains("DoLogSearch1"));
		assertTrue(r.contains("DoLogSearch2"));
		
		
	}


	@Test
	public void testLinkDoExceptionWithVar0() throws Exception {
		String line = "org.jdom.input.JDOMParseException: Error on line 1 of document file:/D:/HPC/DataSynapse/Engine/resources/gridlib/caf-server-dev-VT-4.29.0.26.1/grid-library.xml: Document root element is missing.";

		String expression = ".(w)(Exception)";
		
		Link link = new Link("ExceptionS", expression, "DoLogSearch", 2, 1);
		String result = link.handle(line);
		System.out.println("LinkTest result:" + result);
		
		assertTrue(result.contains("href"));
		assertTrue(result.contains("JDOMParse"));
		assertTrue(result.contains("line"));
		
		assertTrue("Variable was not filled in, \r\nresult:" + result, result.contains("!JDOMParse"));
	}


	@Test
	public void testLinkDoExceptionWithVar1() throws Exception {
		String line = "org.jdom.input.JDOMParseException: Error on line 1 of document file:/D:/HPC/DataSynapse/Engine/resources/gridlib/caf-server-dev-VT-4.29.0.26.1/grid-library.xml: Document root element is missing.";
		
		String expression = "(Exception): (*) .*";
		
		Link link = new Link("ExceptionS", expression, "DoLogSearch", 1, 2);
		String result = link.handle(line);
		System.out.println("LinkTest result:" + result);
		
		assertTrue(result.contains("href"));
		assertTrue(result.contains("JDOMParse"));
		assertTrue(result.contains("line"));
		
		assertTrue("Variable was not filled in", result.contains("!Error"));
	}
	
	@Test
	public void testLinkDoExceptionWithVar2() throws Exception {
		
		String line = "this is an Exception so do var stuff";
		String expression = "(Exception) (*) (*) (*)";
		
		Link link = new Link("ExceptionS", expression, "DoLogSearch", 1, 4);
		String result = link.handle(line);
		System.out.println("LinkTest result:" + result);
		
		assertTrue(result.contains("href"));
		assertTrue(result.contains("stuff"));
		assertTrue(result.contains("this"));
		
		assertTrue("Variable was not filled in", result.contains("!var"));
	}
	
	@Test
	public void testLinkDoException() throws Exception {
		Link link = new Link("ExceptionS", "(Exception)", "DoLogSearch", 1, -1);
		String result = link.handle("this is an Exception so do stuff");

		assertTrue(result.contains("href"));
		assertTrue(result.contains("stuff"));
		assertTrue(result.contains("this"));
	}
	
	@Test
	public void testLinkDonotMatch() throws Exception {
		Link link = new Link("ExceptionS", "(Exception)", "DoLogSearch", 1, -1);
		String result = link.handle("this is an Excddddeption so do stuff");
		System.out.println("LinkTest result:" + result);
		
		assertTrue(result.contains("stuff"));
		assertTrue(result.contains("this"));
	}
}
