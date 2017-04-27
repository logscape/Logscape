package com.liquidlabs.log.space;

import com.liquidlabs.log.search.Query;
import com.liquidlabs.log.search.functions.Count;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.format.DateTimeFormat;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class LogRequestTest {
	
	@Test
	public void shouldGetGroupByNotSoSimple() throws Exception {
		LogRequest request = new LogRequest("SUB", DateTimeUtils.currentTimeMillis(),DateTimeUtils.currentTimeMillis());
		Query query = new Query(0,0, "*", "*", false);
		query.addFunction(new Count("tag", "level","host"));
		query.addFunction(new Count("tag", "level","file"));
		request.addQuery(query);
		String groupByFieldname = request.getGroupByFieldname();
		assertEquals("level", groupByFieldname);
	}
	
	@Test
	public void shouldGetGroupBySimple() throws Exception {
		LogRequest request = new LogRequest("SUB", DateTimeUtils.currentTimeMillis(),DateTimeUtils.currentTimeMillis());
		Query query = new Query(0,0, "*", "*", false);
		query.addFunction(new Count("tag", "","level"));
		request.addQuery(query);
		String groupByFieldname = request.getGroupByFieldname();
		assertEquals("level", groupByFieldname);
	}
	
	@Test
	public void shouldRoundMe() throws Exception {
		LogRequest request = new LogRequest("SUB", DateTimeUtils.currentTimeMillis(),DateTimeUtils.currentTimeMillis());
		request.setTimePeriodMins((short)5);
		String end = Long.toString(request.getEndTimeMs());
		assertTrue(end.endsWith("0000"));
	}
	
	@Test
	public void shouldNotBlowUpOnToString() throws Exception {
		LogRequest request = new LogRequest("SUB", DateTimeUtils.currentTimeMillis(),DateTimeUtils.currentTimeMillis());
		assertNotNull(request.toString());
	}
	



    @Test
	public void testShouldRollBackAMinute() throws Exception {
		LogRequest logRequest = new LogRequest();
		DateTime from = new DateTime(2009, 12, 11, 13, 01, 10, 125);
		long roll = logRequest.rollbackward(from.getMillis(), 10);
		DateTime dateTime = new DateTime(roll);
		assertEquals(getTimeString(from, dateTime), 0, dateTime.getMinuteOfHour());
		assertEquals(0, dateTime.getSecondOfMinute());
		assertEquals(0, dateTime.getMillisOfSecond());
	}

	@Test
	public void testShouldRollForwardsMinute() throws Exception {
		LogRequest logRequest = new LogRequest();
		DateTime from = new DateTime(2009, 12, 11, 13, 01, 10, 125);
		long rollforward = logRequest.rollforward(from.getMillis(), 10);
		DateTime dateTime = new DateTime(rollforward);
		assertEquals(getTimeString(from, dateTime), 10, dateTime.getMinuteOfHour());
		assertEquals(0, dateTime.getSecondOfMinute());
		assertEquals(0, dateTime.getMillisOfSecond());
	}
	@Test
	public void testShouldNOTRollForwardsWhenAlreadyOnMinuteQ() throws Exception {
		LogRequest logRequest = new LogRequest();
		DateTime from = new DateTime(2009, 12, 11, 13, 00, 00, 00);
		System.out.println(from);
		long rollforward = logRequest.rollforward(from.getMillis(), 10);
		DateTime dateTime = new DateTime(rollforward);
		assertEquals(getTimeString(from, dateTime), 0, dateTime.getMinuteOfHour());
		assertEquals(0, dateTime.getSecondOfMinute());
		assertEquals(0, dateTime.getMillisOfSecond());
	}
	
	@Test
	public void testShouldRollForwardsHour() throws Exception {
		LogRequest logRequest = new LogRequest();
		// roll onto next hour
		DateTime from = new DateTime(2009, 12, 13, 12, 59, 01, 125);
		long rollforward = logRequest.rollforward(from.getMillis(), 10);
		DateTime dateTime = new DateTime(rollforward);
		assertEquals(getTimeString(from, dateTime), 13, dateTime.getHourOfDay());
		assertEquals(00, dateTime.getMinuteOfHour());
		assertEquals(0, dateTime.getSecondOfMinute());
		assertEquals(0, dateTime.getMillisOfSecond());
	}
	@Test
	public void testShouldRollForwardsDay() throws Exception {
		LogRequest logRequest = new LogRequest();
		// roll onto next day
		long rollforward = logRequest.rollforward(new DateTime(2009, 12, 20, 23, 59, 01, 125).getMillis(), 10);
		DateTime dateTime = new DateTime(rollforward);
		assertEquals(21, dateTime.getDayOfMonth());
		assertEquals(00, dateTime.getHourOfDay());
		assertEquals(00, dateTime.getMinuteOfHour());
		assertEquals(0, dateTime.getSecondOfMinute());
		assertEquals(0, dateTime.getMillisOfSecond());
	}
	
	private String getTimeString(DateTime from, DateTime dateTime) {
		return "\r\n>> " + DateTimeFormat.longDateTime().print(from) + "\r\n<< " +  DateTimeFormat.longDateTime().print(dateTime);
	}
	
	@Test
	public void testShouldShareHitLimitThreshold() throws Exception {
		ArrayList<LogRequest> copies = new ArrayList<LogRequest>();
		
		LogRequest parent = new LogRequest();
		parent.setStreaming(true);
		Query q = new Query(0, 0, "pattern", "pattern", true);
		q.setHitLimit(4);
		parent.addQuery(q);
		copies.add(parent.copy());
		copies.add(parent.copy());
		LogRequest copy3 = parent.copy();
		copies.add(copy3);
		
		LogRequest copy4 = copy3.copy();
		LogRequest copy5 = copy4.copy();
		
		copies.add(copy4);
		copies.add(copy5);
		int time = 100;
		int incremented = 0;
		
		// increment each individual query once only
		for (LogRequest logRequest : copies) {
			List<Query> queries = logRequest.queries();
			for (Query query : queries) {
				query.increment(time);
				incremented++;
			}
		}
		System.out.println("Total Incremented at:" + incremented);
		
		//verify - that all of them see the same hit limit = and that the threshold was passed
		for (LogRequest logRequest : copies) {
			List<Query> queries = logRequest.queries();
			for (Query query : queries) {
				assertTrue("Should have passed threshold", query.isHitLimitExceeded(time));
			}
			assertTrue(logRequest.isStreaming());
		}
	}
	
	@Test
	public void testShouldCancelChildCopies() throws Exception {
		
		ArrayList<LogRequest> copies = new ArrayList<LogRequest>();
		
		LogRequest parent = new LogRequest();
		copies.add(parent.copy());
		copies.add(parent.copy());
		LogRequest copy3 = parent.copy();
		copies.add(copy3);
		
		LogRequest copy4 = copy3.copy();
		LogRequest copy5 = copy4.copy();
		
		copies.add(copy4);
		copies.add(copy5);
		
		
		parent.cancel();
		
		for (LogRequest logRequest : copies) {
			assertTrue(logRequest.cancelled);
		}
	}

}
