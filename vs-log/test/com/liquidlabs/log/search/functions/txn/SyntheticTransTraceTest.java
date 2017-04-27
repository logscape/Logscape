package com.liquidlabs.log.search.functions.txn;

import org.junit.Test;

import java.text.DecimalFormat;
import java.util.Map;

import static org.junit.Assert.assertEquals;


public class SyntheticTransTraceTest {
	
	private String hostname = "";
	
	@Test
	public void shouldFormatStuff() throws Exception {
		String formatted = String.format("%07d", 1);
		DecimalFormat twoDForm = new DecimalFormat("#.##");
		double number = 1000 + 10 * 0.01;
		String format = twoDForm.format(number);
		System.out.println("Format:" + format);
		
	}
	
	@Test
	public void shouldCollectTWOTxns() throws Exception {
	SyntheticTransTrace trace = new SyntheticTransTrace("uid", "thread", "Demo");
		
		String[] line = {
				"2009-04-22 18:40:24.109 WARN main1 (ORMapperFactory) - Search[Demo111]:WorkAllocator available on:stcp://hal2:11000/WorkAllocator_startTime=1240422023234",
				"2009-04-22 18:40:24.109 WARN main2 (ORMapperFactory) - Search[Demo222]:WorkAllocator available on:stcp://hal2:11000/WorkAllocator_startTime=1240422023234",
				"2009-04-22 18:40:24.109 WARN main2 (ORMapperFactory) - trace line2",
				"2009-04-22 18:40:24.109 WARN main1(ORMapperFactory) - trace line1",
		};
		trace.traceIt(line[0], "Demo111", "main1", hostname);
		trace.traceIt(line[1], "Demo222", "main2", hostname);
		trace.traceIt(line[2], null, "main2", hostname);
		trace.traceIt(line[3], null, "main1", hostname);
		
		Map traceResults = trace.getResults();
		System.out.println(traceResults);
		String expected = "{Demo222=Trace:Demo222\n" + 
				"2009-04-22 18:40:24.109 WARN main2 (ORMapperFactory) - Search[Demo222]:WorkAllocator available on:stcp://hal2:11000/WorkAllocator_startTime=1240422023234\n" + 
				"--2009-04-22 18:40:24.109 WARN main2 (ORMapperFactory) - trace line2\n" + 
				", Demo111=Trace:Demo111\n" + 
				"2009-04-22 18:40:24.109 WARN main1 (ORMapperFactory) - Search[Demo111]:WorkAllocator available on:stcp://hal2:11000/WorkAllocator_startTime=1240422023234\n" + 
				"--2009-04-22 18:40:24.109 WARN main1(ORMapperFactory) - trace line1\n" + 
				"}";
		assertEquals(expected, traceResults.toString());
	}
	
	@Test
	public void shouldStopCollectingWhenAnotherTxnStarts() throws Exception {
		SyntheticTransTrace trace = new SyntheticTransTrace("uid", "thread", "Demo");
		
		String[] line = {
				"2009-04-22 18:40:24.109 WARN main (ORMapperFactory) - Search[Demo111]:WorkAllocator available on:stcp://hal2:11000/WorkAllocator_startTime=1240422023234",
				"2009-04-22 18:40:24.109 WARN main (ORMapperFactory) - trace line1",
				"2009-04-22 18:40:24.109 WARN main (ORMapperFactory) - trace line2",
		};
		trace.traceIt(line[0], "Demo111", "main", hostname);
		trace.traceIt(line[1], null, "main", hostname);
		trace.traceIt(line[1], null, "main-noo", hostname);
		trace.traceIt(line[2], null, "main", hostname);
		// another txn o the same thread - should stop it...
		trace.traceIt(line[0], "CRAP111", "main", hostname);
		trace.traceIt(line[2], null, "main", hostname);
		
		
		Map traceResults = trace.getResults();
		System.out.println(traceResults);
		String expected = "{Demo111=Trace:Demo111\n" + 
				"2009-04-22 18:40:24.109 WARN main (ORMapperFactory) - Search[Demo111]:WorkAllocator available on:stcp://hal2:11000/WorkAllocator_startTime=1240422023234\n" + 
				"--2009-04-22 18:40:24.109 WARN main (ORMapperFactory) - trace line1\n" + 
				"--2009-04-22 18:40:24.109 WARN main (ORMapperFactory) - trace line2\n" + 
				"}";
		assertEquals(expected, traceResults.toString());
	}

	@Test
	public void shouldTraceTxnOnly() throws Exception {
		SyntheticTransTrace trace = new SyntheticTransTrace("uid", "thread", "Demo");
		
		String[] line = {
				"2009-04-22 18:40:24.109 WARN main (ORMapperFactory) - Search[Demo111]:WorkAllocator available on:stcp://hal2:11000/WorkAllocator_startTime=1240422023234",
				"2009-04-22 18:40:24.109 WARN main (ORMapperFactory) - trace line1",
				"2009-04-22 18:40:24.109 WARN main (ORMapperFactory) - trace line2",
		};
		trace.traceIt(line[0], "Demo111", "main", hostname);
		trace.traceIt(line[1], null, "main", hostname);
		trace.traceIt(line[1], null, "main-noo", hostname);
		trace.traceIt(line[2], null, "main", hostname);
		
		Map traceResults = trace.getResults();
		System.out.println(traceResults);
		String expected = "{Demo111=Trace:Demo111\n" + 
				"2009-04-22 18:40:24.109 WARN main (ORMapperFactory) - Search[Demo111]:WorkAllocator available on:stcp://hal2:11000/WorkAllocator_startTime=1240422023234\n" + 
				"--2009-04-22 18:40:24.109 WARN main (ORMapperFactory) - trace line1\n" + 
				"--2009-04-22 18:40:24.109 WARN main (ORMapperFactory) - trace line2\n" + 
				"}";
		assertEquals(expected, traceResults.toString());
	}

}
