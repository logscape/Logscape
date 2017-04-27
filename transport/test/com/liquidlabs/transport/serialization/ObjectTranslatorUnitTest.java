package com.liquidlabs.transport.serialization;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

@SuppressWarnings("unused")
public class ObjectTranslatorUnitTest  {

	private ObjectTranslator query;

	@Before
	public void setUp() throws Exception {
		query = new ObjectTranslator();
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testShouldHandleEmptyList() throws Exception {
		ArrayList<Integer> payload = new ArrayList<Integer>();
		String stringFromObject = query.getStringFromObject(payload);
		ArrayList objectFromFormat = query.getObjectFromFormat(ArrayList.class, stringFromObject);
		assertNotNull(objectFromFormat);
		assertEquals(0, objectFromFormat.size());
	}
	
	@Test
	public void testShouldUpdateSingleFieldOnObject() throws Exception {
		TestObjectWithStringPropertyToMatch instance = new TestObjectWithStringPropertyToMatch();
		 TestObjectWithStringPropertyToMatch updatedObject = query.getUpdatedObject(instance, "someRubbish = rubbish");
		 assertEquals(updatedObject.someRubbish, "rubbish");
	}
	@Test
	public void testShouldUpdateMultipleFieldsOnObject() throws Exception {
		TestObjectWithStringPropertyToMatch instance = new TestObjectWithStringPropertyToMatch();
		TestObjectWithStringPropertyToMatch updatedObject = query.getUpdatedObject(instance, "someRubbish = rubbish AND field1 = 1223");
		assertEquals(updatedObject.someRubbish, "rubbish");
		assertEquals(updatedObject.field1, 1223);
	}
	
	
	@Test
	public void testShouldMatchOnFieldWithWhiteSPACE() throws Exception {
		TestObjectWithStringPropertyToMatch instance = new TestObjectWithStringPropertyToMatch();
		assertTrue(query.isMatch("fieldToMatch contains      \"matchme bit\"" , instance));
	}
	@Test
	public void testShouldMatchOnField() throws Exception {
		TestObjectWithStringPropertyToMatch instance = new TestObjectWithStringPropertyToMatch();
		assertTrue(query.isMatch("fieldToMatch contains \"matchme bit\"" , instance));
	}
	@Test
	public void testShouldNotMatchWhenNoMatchesOnField() throws Exception {
		TestObjectWithStringPropertyToMatch instance = new TestObjectWithStringPropertyToMatch();
		assertFalse(query.isMatch("fieldToMatch contains \"matchMISSMATCHme bit\"" , instance));
	}

	@Test
	public void testShouldHandleEmptyStringArray() throws Exception {
		String stringFromObject = query.getStringFromObject(new TestObjectWithEmptyStringArrayField());
		TestObjectWithEmptyStringArrayField objectFromFormat = query.getObjectFromFormat(TestObjectWithEmptyStringArrayField.class, stringFromObject);
		assertNotNull(objectFromFormat.stringArr);
		assertEquals(0, objectFromFormat.stringArr.length);
	}
	
	@Test
	public void testShouldCreateDefaultInstance() throws Exception {
		assertNotNull(query.getDefaultConstructedInstance(MyObject.class));
	}

	@Test
	public void testShouldCreateQueryStringSupportingAND() throws Exception {
		String escapeAND = query.escapeAND("'"," not AND \"this AND this\" not AND 'yes AND no'");
		escapeAND = query.escapeAND("\"", escapeAND);
		System.out.println("Got:" + escapeAND);
		assertEquals(" not AND \"this A_N_D this\" not AND 'yes A_N_D no'", escapeAND);
		String[] queryString = query.getQueryStringTemplate(MyObject.class, "startTimeSecs == 9 AND osVersion equals 'Windows AND XP''");
		System.out.println(Arrays.toString(queryString));
		assertEquals(1, queryString.length);
	}
	
	@Test
	public void testShouldCreateQueryString() throws Exception {		
		String[] queryString = query.getQueryStringTemplate(MyObject.class, "startTimeSecs == 9 AND aIntValue == 999 OR aIntValue >= 999 AND osVersion equals \"Windows XP\"");
		System.out.println(Arrays.toString(queryString));
		assertEquals(2, queryString.length);
	}
	@Test
	public void testShouldCreateQueryStringUsingSingleQuote() throws Exception {		
		String[] queryString = query.getQueryStringTemplate(MyObject.class, "startTimeSecs == 9 AND aIntValue == 999 OR aIntValue >= 999 AND osVersion equals 'Windows XP'");
		System.out.println(Arrays.toString(queryString));
		assertEquals(2, queryString.length);
	}
	@Test
	public void testShouldCreateQueryStringWithFieldANDs() throws Exception {		
		String[] queryString = query.getQueryStringTemplate(MyObject.class, "startTimeSecs == 9 AND startTimeSecs > 0");
		System.out.println(Arrays.toString(queryString));
		assertTrue(queryString[0].contains("AND:"));
	}
	@Test
	public void testShouldCreateQueryStringWithObjectLevel() throws Exception {		
		String[] queryString = query.getQueryStringTemplate(MyObject.class, "startTimeSecs == 9 AND aIntValue");
		System.out.println(Arrays.toString(queryString));
		assertTrue(!queryString[0].contains("AND:"));
	}

	@Test
	public void testShouldConvertObjectToString() throws Exception {
		String stringValue = query.getStringFromObject(new MyObject());
		assertTrue("String should have started MyObjectclass but had:" + stringValue, stringValue.startsWith(MyObject.class.getName()));
	}

	@Test
	public void testShouldPopulateObjectFromString() throws Exception {
		String stringValue = query.getStringFromObject(new MyObject());
		System.out.println(stringValue);
		MyObject result = query.getObjectFromFormat(MyObject.class,stringValue);
		assertEquals(10, result.aIntValue);
	}
	
	@Test
	public void testShouldMatchStuff() throws Exception {
		String workId = ",mobius.local-111000:myBundle-0.01:myService";
		boolean match = query.isMatch("workId contains myBundle-0.01:myService", new Fake(workId, 1));
		assertTrue(match);
	}
	
	
	public static class Fake {
		private String workId;
		private int val;

		public Fake(String workId, int val) {
			this.workId = workId;
			this.val = val;
		}
	}
	
	public static class MyObject {
		int cSecondIntValue = 99;
		long startTimeSecs = 9L;
		private String bStringValue = "SomeValue";
		private int aIntValue = 10;
		private String dSecondStringValue = "SecondStringValue";
		private String[] someValues = new String[] { "s1", "s2", "s3", "s4" };
		String zCustomProperties = "[FPGA=NO JobTypePreference=MonteCarlo PreferIOBound=TRUE AvoidDB=TRUE]";
		String osVersion = "Windows XP";
		String[] emptyStringArray = new String[0];

		public int getAIntValue() {
			return aIntValue;
		}

		public void setAIntValue(int intValue) {
			aIntValue = intValue;
		}

		public String getDSecondStringValue() {
			return dSecondStringValue;
		}

		public void setDSecondStringValue(String secondStringValue) {
			dSecondStringValue = secondStringValue;
		}
	}
	
	public static class TestObjectWithEmptyStringArrayField {
		int a = 0;
		String[] stringArr = new String[0];
	}
	public static class TestObjectWithStringPropertyToMatch {
		int field1 = 0;
		String fieldToMatch = " this is a field and the is the matchme bit";
		String someRubbish = "crap";
	}

}
