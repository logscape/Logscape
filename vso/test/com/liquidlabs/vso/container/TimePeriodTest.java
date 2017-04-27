package com.liquidlabs.vso.container;

import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;

import com.liquidlabs.vso.container.NullRule;
import com.liquidlabs.vso.container.sla.TimePeriod;

import junit.framework.TestCase;

public class TimePeriodTest extends TestCase {

	public void testShouldKnowWhenTimePeriodIsActive() throws Exception {
		TimePeriod period = new TimePeriod(new NullRule(), "00:00", "02:00");
		setTime(1,0);
		assertTrue(period.isActive());
		setTime(2,1);
		assertFalse(period.isActive());
	}
	
	public void testShouldFireOnceEvenWhenInTimePeriod() throws Exception {
		TimePeriod period = new TimePeriod(new NullRule(), "00:00", "02:00");
		period.setOneOff(true);
		setTime(1,0);
		assertTrue(period.isActive());
		// make it inactive by firing
		period.evaluateRule(100, null, new Metric[0]);
		assertFalse(period.isActive());
		
	}
	public void testShouldResetOnceOfAfterBeingInactive() throws Exception {
		TimePeriod period = new TimePeriod(new NullRule(), "00:00", "02:00");
		period.setOneOff(true);
		setTime(1,0);
		assertTrue(period.isActive());
		
		// make it inactive by firing
		period.evaluateRule(100, null, new Metric[0]);
		
		// should be false now
		assertFalse(period.isActive());
		
		// inactive time period resets it
		setTime(2,1);
		assertFalse(period.isActive());
		setTime(1,0);		
		// isOne off allows Activation
		assertTrue(period.isActive());
		
		// execute to fire the oneOff
		period.evaluateRule(100, null, new Metric[0]);
		// isOne off flips over and no longer active
		assertFalse(period.isActive());
	}
	
	public void testShouldResetOnceOfAfterBeingInactiveUsingOtherMethod() throws Exception {
		TimePeriod period = new TimePeriod(new NullRule(), "00:00", "02:00");
		period.setOneOff(true);
		
		assertTrue(period.isActive(getTime(1,0).getMillis()));
		
		// make it inactive by firing
		period.evaluateRule(100, null, new Metric[0]);
		
		assertFalse(period.isActive(getTime(1,0).getMillis()));
		
		// inactive time period resets it
		assertFalse(period.isActive(getTime(2,1).getMillis()));
		
		// isOne off allows Activation
		assertTrue(period.isActive(getTime(1,0).getMillis()));
		
		// make it inactive by firing
		period.evaluateRule(100, null, new Metric[0]);
		
		// isOne off flips over and no longer active
		assertFalse(period.isActive());
	}
	
	private DateTime getTime(int hour, int min){
		DateTime time = new DateTime();
		time = time.withHourOfDay(hour);
		time = time.withMinuteOfHour(min);
		return time;
	}
	
	private void setTime(int hour, int min) {
		DateTime time = new DateTime();
		time = time.withHourOfDay(hour);
		time = time.withMinuteOfHour(min);
		DateTimeUtils.setCurrentMillisFixed(time.getMillis());
	}
}
