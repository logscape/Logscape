package com.liquidlabs.vso.container.sla;

import java.util.List;

import junit.framework.TestCase;

public class SLASerializerTest extends TestCase {
	
	String slaString = "<sla consumerName=\"Grid1\" consumerClass=\"com.liquidlabs.SomeConsumer\">\n" +
	        " <variable name=\"a\" value=\"value1\"/> " +
	        " <variable name=\"b\" value=\"value2\"/> " +
	        " <variable name=\"c\" value=\"value3\"/> " +
			"	<timePeriod start=\"00:00\" end=\"23:50\">\n" + 
			"		<rule maxResources=\"10\" priority=\"1000\">\n" + 
			"			<resourceGroup>ONE</resourceGroup>\n" +
			"			<resourceGroup>TWO</resourceGroup>\n" +
			"			<evaluator>\n" + 
			"				<![CDATA[\n" + 
			"					if (queueLength > 10 || engineCount < 2)\n" + 
			"						return new Add(\"mflops > 10\", 1);\n" + 
			"					else if (queueLength == 0 && engineCount > 2)\n" + 
			"						return new Remove(\"mflops > 10\", 1) \n" + 
			"					]]>\n" + 
			"			</evaluator>\n" + 
			"		</rule>\n" + 
			"    </timePeriod>\n" + 
			"	<timePeriod maxResources=\"50\"  start=\"23:50\" end=\"23:59\" priority=\"10\">\n" + 
			"        <rule>\n" + 
			"            <evaluator>\n" + 
			"                <![CDATA[\n" + 
			"               	if (queueLength > 15)\n" + 
			"						return new Add(\"mflops > 50\", 1)\n" + 
			"					else\n" + 
			"						return new Remove(\"mflops > 10\", 1) \n" + 
			"                    ]]>\n" + 
			"            </evaluator>\n" + 
			"        </rule>\n" + 
			"    </timePeriod>\n" + 
			"</sla>";
	
	public void testShouldLoadSLAFromString() throws Exception {
		
		SLASerializer serializer = new SLASerializer();
		SLA sla = serializer.deSerialize(null, slaString);
		List<Variable> variables = sla.getVariables();
		assertEquals("Should have 3 vars but got:" + variables, 3, variables.size());
		assertEquals("Should have 2 timeperiods:" + variables, 2, sla.getTimePeriods().size());
		assertEquals("com.liquidlabs.SomeConsumer", sla.getConsumerClass());
		assertEquals("maxResources not set", 10, sla.getTimePeriods().get(0).getRules().get(0).maxResources);
		assertEquals("priority not set", 1000, sla.getTimePeriods().get(0).getRules().get(0).priority);
		assertEquals("priority not set", 1000, sla.getTimePeriods().get(0).getRules().get(0).priority);
		assertEquals("Should have 2 resource groups", 2, sla.getTimePeriods().get(0).getRules().get(0).getResourceGroups().size());
	}

}
