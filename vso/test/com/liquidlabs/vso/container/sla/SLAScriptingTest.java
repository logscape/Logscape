package com.liquidlabs.vso.container.sla;

import junit.framework.TestCase;

import com.liquidlabs.vso.container.Action;
import com.liquidlabs.vso.container.Add;
import com.liquidlabs.vso.container.Metric;
import com.liquidlabs.vso.container.sla.GroovySLA;
import com.liquidlabs.vso.container.sla.Rule;
import com.liquidlabs.vso.container.sla.TimePeriod;

public class SLAScriptingTest extends TestCase {
	
	int resourceCount = 1;
	public static boolean afterOneAM = false;

	public void testShouldEvaluateRuleForTimePeriod() throws Exception {
		GroovySLA sla = new GroovySLA();
		sla.addTimePeriod(timePeriod(add(), "00:00", "23:59"));
		Action action = sla.evaluate(resourceCount, null, new Metric[0]);
		assertNotNull("Should have got an action", action);
		assertTrue(action instanceof Add);
		assertTrue(afterOneAM);
	}

	

	private TimePeriod timePeriod(Rule rule, String start, String end) {
		return new TimePeriod(rule, start, end);
	}

	private Rule add() {
		
		Rule rule = new Rule("println \"now is ${currentTime}\"\n" +
				"println \"nowDateTime is ${nowDateTime}\"\n" +
				"println \"function evals to \" + getTime(\"12:30\")\n"+
				"boolean result = \"${currentTime}\" > getTime(\"01:00\")\n"+
				"println \" currentTime comparison:\" + result\n"+
				"com.liquidlabs.vso.container.sla.SLAScriptingTest.afterOneAM = result\n" +
				"return new Add(\"foo\", 1)", 3, 10);
		rule.maxResources = 10;
		return rule;
	}

	private Rule remove() {
		Rule rule = new Rule("return new Remove(\"xxx\", 1)", 3, 10);
		rule.maxResources = 10;
		return rule;
	}

}
