package com.liquidlabs.log.space;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class WatchStatsTest {
	
	@Test
	public void shouldPrintCorrectToString() throws Exception {
		WatchStats watchStats = new WatchStats("me", new WatchDirectory(), "hostname");
		String string = watchStats.toString();
		assertTrue(string.contains("me"));
		System.out.println(string);
	}

}
