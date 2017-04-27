package com.liquidlabs.vso.container;

import com.liquidlabs.vso.container.Action;
import com.liquidlabs.vso.container.Add;
import com.liquidlabs.vso.container.Metric;
import com.liquidlabs.vso.container.NullAction;
import com.liquidlabs.vso.container.Remove;
import com.liquidlabs.vso.container.sla.Rule;

import junit.framework.TestCase;

public class RuleTest extends TestCase {
	
	public void testShouldEvaluateToAddWithoutImport() throws Exception {
		String script = "return new Add(\"foo\", 1);";
		Action result = new Rule(script, 3, 10).evaluate(null, new Metric[0]);
		assertTrue(result instanceof Add);
	}
	
	public void testShouldEvaluateToRemoveWithoutImport() throws Exception {
		String script = "return new Remove(\"xxx\", 1);";
		Action result = new Rule(script, 3, 10).evaluate(null, new Metric[0]);
		assertTrue(result instanceof Remove);
	}
	
	public void testShouldEvaluateToNullWithoutImport() throws Exception {
		String script = "return new NullAction();";
		Action result = new Rule(script, 3, 10).evaluate(null, new Metric[0]);
		assertTrue(result instanceof NullAction);
	}
	
	public void testShouldWorkWhenActionsImported() {
		String script = "import com.liquidlabs.vso.container.Add;\n" +
				"import com.liquidlabs.vso.container.Remove;\n" +
				"import com.liquidlabs.vso.container.NullAction;\n" +
				"return new NullAction();";
		new Rule(script, 3, 10).evaluate(null, new Metric[0]);
	}
	
	public void testShouldBindMetricsForUseByScript() throws Exception {
		String script = "if (bar > 10 && foo < 5 && foo_bar > 15) \n" +
				"\treturn new Add(\"abc\", 1);\n" +
				"else return new Remove(\"XXX\");";
		Metric [] metrics = {new PretendMetric("bar", 11.0), new PretendMetric("foo", 4.0), new PretendMetric("foo_bar", 16.0)};
		Action result = new Rule(script, 3, 10).evaluate(null, metrics);
		assertTrue(result instanceof Add);
	}
}
