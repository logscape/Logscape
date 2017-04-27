package com.liquidlabs.vso.container.sla;

import java.io.File;
import java.util.List;

import junit.framework.TestCase;

import org.apache.log4j.Logger;

import com.liquidlabs.vso.container.Consumer;
import com.liquidlabs.vso.container.Metric;

public class SLAValidatorTest extends TestCase {
	
	
	private SLAValidator validator;
	@Override
	protected void setUp() throws Exception {
		validator = new SLAValidator();
	}
	
	public void testSLAValidatorConfirmsWelformedXML() throws Exception {
		String errorMsg = validator.isWellFormed("<xml/>");
		
		assertEquals("", errorMsg);
	}
	
	public void testShouldProvideErrorMsgWithBadXML() throws Exception {
		String errorMsg = validator.isWellFormed("<xml>\n <stuff>\n </xml>");
		assertTrue(errorMsg.length() > 0);
		assertTrue("error msg was:" + errorMsg, errorMsg.contains("must be terminated by the matching end-tag"));
	}
	
	public void testShouldDeserialiseToSLA() throws Exception {
		
		String errorMsg = validator.isDeserializable(sla_xml);
		
		assertEquals("", errorMsg);
	}
	
	public void testShouldGiveGoodErrorMsgOnBadSLA() throws Exception {
		String errorMsg = validator.isDeserializable(bad_sla_xml);
		System.err.println(errorMsg);
	}
	
	public void testShouldValidateRulesScriptsWithGoodMetrics() throws Exception {
		
		Metric[] metrics = new Metric[] {  new DumbMetric("engineCount", 0.0), new DumbMetric("queueLength", 0.0), 
						new DumbMetric("idleEngines", 0.0), new DumbMetric("busyEngines", 0.0), 
						new DumbMetric("eodPercentComplete", 0.0) };
		
		String errorMsg = validator.isRulesValid(sla_xml, metrics, null);
		
		assertEquals("", errorMsg);
		
	}
	public void testShouldERRORScriptsWithInvalidMetrics() throws Exception {
		
		Metric[] metrics = new Metric[] { new DumbMetric("queueLength", 0.0), 
					new DumbMetric("idleEngines", 0.0), new DumbMetric("busyEngines", 0.0), 
					new DumbMetric("eodPercentComplete", 0.0) };
		
		String errorMsg = validator.isRulesValid(sla_xml, metrics, null);
		
		System.err.println("Errormsg:" + errorMsg);
		assertTrue(errorMsg, errorMsg.length() > 0);
		assertTrue(errorMsg, errorMsg.contains("engineCount"));
		
	}
	
	public void XtestShouldHaveGoodSLA() throws Exception {
		SLASerializer serializer = new SLASerializer();
		System.out.println("Running from:" + new File(".").getAbsolutePath());
		
		
		
		SLA sla = serializer.deSerialize(null, "example/sla-example.xml");
		
		assertTrue(sla.getTimePeriods().get(0).isOneOff());
		assertFalse(sla.getTimePeriods().get(1).isOneOff());
		
		sla.setScriptLogger(Logger.getLogger(Consumer.class));
		// now iterate and execute each groovy part
		List<TimePeriod> timePeriods = sla.getTimePeriods();
		Metric[] metrics = new Metric[] {  new DumbMetric("engineCount", 0.0), new DumbMetric("queueLength", 0.0), 
											new DumbMetric("idleEngines", 0.0), new DumbMetric("busyEngines", 0.0), 
											new DumbMetric("eodPercentComplete", 0.0) 
		};
		for (TimePeriod timePeriod : timePeriods) {
			List<Rule> rules = timePeriod.getRules();
			for (Rule rule : rules) {
				rule.evaluate(null, metrics);
			}
		}
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

	String sla_xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" + 
			"<sla consumerClass=\"com.liquidlabs.flow.sla.FlowConsumer\">\n" + 
			"	<variable name=\"GridName\" value=\"@app.name@@cg.name@\" />\n" + 
			"	<variable name=\"MasterPort\" value=\"@cg.port@\" />\n" + 
			"	<!-- OVERRIDE for a time period-->\n" + 
			"	<timePeriod start=\"08:30\" end=\"09:30\" isOneOff=\"true\" label='GeneralRules'>\n" + 
			"		<rule maxResources=\"650\" priority=\"8\">\n" + 
			"			<evaluator>\n" + 
			"				<![CDATA[\n" + 
			"					if (eodPercentComplete < 51 && currentTime > getTime(\"01:00\")) {\n" + 
			"						logger.warn(\"RAISE_ALERT - Current EODWorkLoad is behind, completed:\" + eodPercentComplete + \" and is behind schedule\");\n" + 
			"					}\n" + 
			"					log.info(\"@app.name@@cg.name@ - ADD OVERRIDE SLA ***** engineCount \" + engineCount + \" q:\"+ queueLength + \" idle:\" + idleEngines + \" busy:\" + busyEngines)\n" + 
			"				\n" + 
			"					if (engineCount < 9)	return new Add(\"mflops > 10\", 20);\n" +

			"					]]>\n" + 
			"			</evaluator>\n" + 
			"		</rule>\n" + 
			"	</timePeriod>\n" + 
			"</sla>";
	String bad_sla_xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" + 
	"<sla consumerClass=\"com.liquidlabs.flow.sla.FlowConsumer\">\n" + 
	"	<variable name=\"GridName\" value=\"@app.name@@cg.name@\" />\n" + 
	"	<variable name=\"MasterPort\" value=\"@cg.port@\" />\n" + 
	"	<!-- OVERRIDE for a time period-->\n" + 
	"	<timePeriod start=\"08:30\" end=\"09:30\" isOneOff=\"true\">\n" + 
	"		<arule maxResources=\"650\" priority=\"8\">\n" + 
	"			<evaluator>\n" + 
	"				<![CDATA[\n" + 
	"\n" + 
	"					if (eodPercentComplete < 51 && currentTime > getTime(\"01:00\")) {\n" + 
	"						logger.warn(\"RAISE_ALERT - Current EODWorkLoad is behind, completed:\" + eodPercentComplete + \" and is behind schedule\");\n" + 
	"					}\n" + 
	"					log.info(\"@app.name@@cg.name@ - ADD OVERRIDE SLA ***** engineCount \" + engineCount + \" q:\"+ queueLength + \" idle:\" + idleEngines + \" busy:\" + busyEngines)\n" + 
	"				\n" + 
	"					if (engineCount < 9)	return new Add(\"mflops > 10\", 20);\n" + 
	"					]]>\n" + 
	"			</evaluator>\n" + 
	"		</rule>\n" + 
	"	</timePeriod>\n" + 
	"</sla>";
}
