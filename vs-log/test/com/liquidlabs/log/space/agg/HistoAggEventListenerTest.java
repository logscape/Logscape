package com.liquidlabs.log.space.agg;

import com.liquidlabs.log.space.LogRequest;
import org.joda.time.DateTimeUtils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class HistoAggEventListenerTest  {
	
	private HistoAggEventListener listener;
	long now = DateTimeUtils.currentTimeMillis();
	
	@Before
	public void setUp() throws Exception {
		listener = new HistoAggEventListener("provider", "sub", new LogRequest(), null, "handler", false);
	}
	
	
	@Test
	public void shouldGetPCComplete() throws Exception {
		listener.status("one", 1, 0);
		listener.status("two", 1, 0);
		assertEquals(0, listener.getPercentComplete());
		listener.status("one", -1, 0);
		assertEquals(50, listener.getPercentComplete());
		listener.status("two", -1, 0);
		assertEquals(100, listener.getPercentComplete());
		
		
		
	}
	
	@Test
	public void testShouldNOTFireWithDiffSizesWithinONESecond() throws Exception {
		assertFalse(listener.isReadyToFireNow(10, now, now - 100));
	}
	@Test
	public void testShouldFireWithDifferentSizesGTTenSeconds() throws Exception {
		assertTrue(listener.isReadyToFireNow(10, now, now - 10000));
	}
	@Test
	public void testShouldFireWithDifferentSizesLTTenSeconds() throws Exception {
		assertFalse(listener.isReadyToFireNow(10, now,  now - 1000));
        assertTrue(listener.isReadyToFireNow(10, now,  now - 4000));
	}

}
