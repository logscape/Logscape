package com.liquidlabs.log.fields;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

public class FieldSetIntegrationTest {


    @Test
    public void shouldHandleLog4JAutoCountMultipleTypeTRUE() throws Exception {
        FieldSet log4JFieldSet = FieldSets.getLog4JFieldSet();
        log4JFieldSet.addDefaultFields("log3j","host","filename.log","/opt/filename.log","myTag:","someAgent","sourceURL",10l,true);

        for (int i = 0; i < 10; i++) {
            List<String> fieldNames1 = log4JFieldSet.getFieldNames(true, false, true, false, true);
            System.out.println("FieldNames1:" + fieldNames1);
            List<String> fieldNames2 = log4JFieldSet.getFieldNames(true, false, true, true, true);
            System.out.println("FieldNames2:" + fieldNames2);


            assertTrue(fieldNames2.size() < fieldNames1.size());
        }
    }

	
	@Test
	public void shouldWorkWithSplit() throws Exception {
		FieldSet fieldSet = new FieldSet("split(\t,3)","one","two","three");
		String test = FieldSetUtil.test(fieldSet,new String[] { "a	b	c", "d	e	f" });
		System.out.println(test);
		assertTrue(test.contains("three	= c"));
		//assertFalse(test.contains("FAILED"));
	}
	
	@Test
	public void shouldHandleCiscoIDS() throws Exception {
		runTest(FieldSets.getCiscoIDSIPDLog());
	}
	@Test
	public void shouldHandleCiscoASA() throws Exception {
		runTest(FieldSets.getCiscoASALog());
	}

	@Test
	public void shouldHandleNTEventLog() throws Exception {
		runTest(FieldSets.getNTEventLog());
	}
	@Test
	public void shouldHandleNetScreenMsg() throws Exception {
		runTest(FieldSets.getNetScreenMsg());
	}
	@Test
	public void shouldHandleNetScreenTRFIC() throws Exception {
		runTest(FieldSets.getNetScreenTraffic());
	}
	
	@Test
	public void shouldHandleCollectiveFeuh() throws Exception {
		runTest(FieldSets.getCollectiveFeuh());
	}
	@Test
	public void shouldHandleCentrisWeblogLog() throws Exception {
		runTest(FieldSets.getCentrisWebLogicLog());
		
	}
	@Test
	public void shouldHandleCiscoPIX() throws Exception {
		runTest(FieldSets.getCiscoPIXLog());
	}
	
	@Test
	public void shouldHandleAccessCombined() throws Exception {
		runTest(FieldSets.getAccessCombined());
	}
	@Test
	public void shouldHandleSysLog() throws Exception {
		runTest(FieldSets.getSysLog());
	}
	@Test
	public void shouldHandleBasic() throws Exception {
		runTest(FieldSets.getBasicFieldSet());
	}
	
	@Test
	public void shouldHandleWin2008Format() throws Exception {
		FieldSet fieldSet = FieldSets.get2008EVTFieldSet();
		System.out.println(FieldSetUtil.test(fieldSet,fieldSet.example));
		runTest(fieldSet);		
	}
	
	@Test
	public void shouldHandleLog4J() throws Exception {
		String result = runTest(FieldSets.getLog4JFieldSet());
		assertTrue(result.contains("cpu2	= 99"));
//		assertTrue(result.contains("exception	= java.io.FileNotFoundException"));
	}
	
	@Test
	public void shouldHandleLogStats() throws Exception {
		String result = runTest(FieldSets.getAgentStatsFieldSet());
	}

	private String runTest(FieldSet fieldSet) {
		String test = FieldSetUtil.test(fieldSet,fieldSet.example);
		System.out.println(test);
		assertTrue("Test Failed:" + test, !test.contains("Fail:</b>0"));
//		assertFalse("test was:" + test, test.contains("FAIL"));
		return test;
	}

}
