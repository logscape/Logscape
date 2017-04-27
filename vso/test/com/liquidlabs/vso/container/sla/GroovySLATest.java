package com.liquidlabs.vso.container.sla;

import junit.framework.TestCase;

import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;

import com.liquidlabs.vso.container.Action;
import com.liquidlabs.vso.container.Add;
import com.liquidlabs.vso.container.Metric;
import com.liquidlabs.vso.container.NullAction;
import com.liquidlabs.vso.container.Remove;
import com.liquidlabs.vso.container.sla.GroovySLA;
import com.liquidlabs.vso.container.sla.Rule;
import com.liquidlabs.vso.container.sla.TimePeriod;

public class GroovySLATest extends TestCase {
	
	int resourceCount = 1;

	public void testShouldEvaluateRuleForTimePeriod() throws Exception {
		GroovySLA sla = new GroovySLA();
		sla.addTimePeriod(timePeriod(add(), "00:00", "23:59"));
		Action action = sla.evaluate(resourceCount, null,new Metric[0]);
		assertNotNull("Should have got an action", action);
		assertTrue(action instanceof Add);
	}

	public void testShouldEvaluateRuleForAppropriateTimePeriod()
			throws Exception {
		GroovySLA sla = new GroovySLA();
		sla.addTimePeriod(timePeriod(add(), "00:00", "11:59"));
		sla.addTimePeriod(timePeriod(remove(), "12:00", "23:59"));
		setTime(12, 1);
		Action result = sla.evaluate(resourceCount, null,new Metric[0]);
		assertTrue(result instanceof Remove);
	}

	public void testShouldEvaluateRuleForOtherAppropriateTimePeriod()
			throws Exception {
		GroovySLA sla = new GroovySLA();
		sla.addTimePeriod(timePeriod(add(), "00:00", "11:59"));
		sla.addTimePeriod(timePeriod(remove(), "12:00", "23:59"));
		setTime(0,0);
		Action result = sla.evaluate(resourceCount, null, new Metric[0]);
		assertTrue(result instanceof Add);
	}
	
	private void setTime(int hour, int min) {
		DateTime time = new DateTime();
		time = time.withHourOfDay(hour);
		time = time.withMinuteOfHour(min);
		DateTimeUtils.setCurrentMillisFixed(time.getMillis());
	}
	

	public void testShouldEvaluateToNullActionWhenTimePeriodNotFound()
			throws Exception {
		GroovySLA sla = new GroovySLA();
		Action action = sla.evaluate(resourceCount, null, new Metric[0]);
		assertTrue(action instanceof NullAction);
	}

	private TimePeriod timePeriod(Rule rule, String start, String end) {
		return new TimePeriod(rule, start, end);
	}

	private Rule add() {
		Rule rule = new Rule("return new Add(\"foo\", 1)", 3, 10);
		rule.maxResources = 10;
		return rule;
	}

	private Rule remove() {
		Rule rule = new Rule("return new Remove(\"xxx\", 1)", 3, 10);
		rule.maxResources = 10;
		return rule;
	}

}
