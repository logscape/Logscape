package com.liquidlabs.vso.container.sla;

import com.liquidlabs.vso.container.Action;
import com.liquidlabs.vso.container.DumbConsumer;
import com.liquidlabs.vso.container.Metric;
import com.liquidlabs.vso.container.NullAction;
import com.liquidlabs.vso.container.sla.Rule;

import junit.framework.TestCase;

/**
 * Use me to validate SLA Script elements
 *
 */
public class SLAScriptValidatorTest extends TestCase {
	
	
	public void testShouldCheckAScript() throws Exception {
		String script = "" + 
		"			System.out.println(\"B <40 ***** resourceCount \" + resourceCount + \" q:\"+ queueLength + \" idle:\" + idleEngines)\n" + 
				"					if (queueLength > 15 && resourceCount < 99)\n" + 
				"						return new Add(\"mflops > 10\", 1)\n" + 
				"					else  if (queueLength < 100  && idleEngines > 3)\n" + 
				"						return new Remove(\"mflops > 10\", 3)\n" + 
				"					else if (queueLength == 0 && idleEngines > 1)\n" + 
				"						return new Remove(\"mflops > 10\", 2)  ";
		
		Rule rule = new Rule(script, 2,2);
		DumbMetric resource = new DumbMetric("resourceCount", 1.0);
		DumbMetric queue = new DumbMetric("queueLength", 111.0);
		DumbMetric idle = new DumbMetric("idleEngines", 1.0);
		
		
		Action evaluate = rule.evaluate(null,new Metric[] { resource, queue, idle });
		assertFalse("should not have returned NullAction - failed to execute, check stdout", evaluate instanceof NullAction);
		assertNotNull(evaluate);
		
	}
	
	
	static class DumbMetric implements Metric {

		private final String name;
		private final Double value;

		public DumbMetric(String name, double value) {
			this.name = name;
			this.value = value;
		}
		
		public String name() {
			return name;
		}

		public Double value() {
			return value;
		}
		public String toString() {
			return "metric:" + name + "=" + value;
		}
		
	}
	

}
