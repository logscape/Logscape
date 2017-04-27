package com.liquidlabs.common;

import static org.junit.Assert.*;

import org.joda.time.DateTime;
import org.junit.Test;

public class DateUtilTest {

	@Test
	public void shouldFloorTo5Mins() throws Exception {
		long time = System.currentTimeMillis();
		long l = DateUtil.nearestMin(time, 5);
		System.out.println("tt:" + new DateTime(l));
	}


	@Test
	public void shouldFloorSec() throws Exception {
		long i = System.currentTimeMillis();
		DateTime bTime = new DateTime(i).secondOfMinute().roundFloorCopy();
		assertEquals(new DateTime(i) + " Expect:" + bTime + " floor:" + new DateTime(DateUtil.floorMin(i)), bTime.getMillis(), DateUtil.floorSec(i));
	}
	
	@Test
	public void shouldFloorMin() throws Exception {
		long i = System.currentTimeMillis();
		DateTime bTime = new DateTime(i).minuteOfHour().roundFloorCopy();
		assertEquals(new DateTime(i) + " Expect:" + bTime + " floor:" + new DateTime(DateUtil.floorMin(i)), bTime.getMillis(), DateUtil.floorMin(i));
	}
	
	@Test
	public void shouldFloorHour() throws Exception {
		long i = System.currentTimeMillis();
		DateTime bTime = new DateTime(i).hourOfDay().roundFloorCopy();
		assertEquals(new DateTime(i) + " Expect:" + bTime + " floor:" + new DateTime(DateUtil.floorMin(i)), bTime.getMillis(), DateUtil.floorHour(i));
	}
	@Test
	public void shouldFloorDay() throws Exception {
		long i = System.currentTimeMillis();

		DateTime bTime = new DateTime(i).dayOfYear().roundFloorCopy();
        long diff = bTime.getMillis() - DateUtil.floorDay(i);
        if (diff != 0) diff = diff/DateUtil.HOUR;
		assertEquals("DiffHours:" + diff + " " + new DateTime(i) + " Expect:" + bTime + " floor:" + new DateTime(DateUtil.floorMin(i)), bTime.getMillis(), DateUtil.floorDay(i));
	}
}
