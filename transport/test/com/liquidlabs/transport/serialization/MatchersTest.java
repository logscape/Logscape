package com.liquidlabs.transport.serialization;

import java.util.List;

import static org.junit.Assert.*;
import org.junit.Test;

import com.liquidlabs.transport.serialization.Matchers.AND;
import com.liquidlabs.transport.serialization.Matchers.NumberGreaterThan;
import com.liquidlabs.transport.serialization.Matchers.NumberLessThan;
import com.liquidlabs.transport.serialization.Matchers.StringContainsAny;
import com.liquidlabs.transport.serialization.Matchers.StringEqualsAny;
import com.liquidlabs.transport.serialization.Matchers.StringNotContains;
import com.liquidlabs.transport.serialization.Matchers.StringNotContainsAny;

public class MatchersTest  {
	
	Matchers matcher = new Matchers();
	private int column;
	
	@Test
	public void testShouldSplitOpertorsOK() throws Exception {
		String[][] trimSearchTemplate = matcher.trimSearchTemplate(new String[] { null,"","","equals:R1"});
		assertEquals(2, trimSearchTemplate.length);
		assertEquals(4, trimSearchTemplate[0].length);
		assertEquals(4, trimSearchTemplate[1].length);
		assertEquals("equals", trimSearchTemplate[0][3]);
		assertEquals("R1", trimSearchTemplate[1][3]);
	}
	@Test
	public void testShouldSplitOpertorsOK2() throws Exception {
		String[][] trimSearchTemplate = matcher.trimSearchTemplate("A,,,,equals:B,equals:C,,".split(","));
		assertEquals(2, trimSearchTemplate.length);
		assertEquals(6, trimSearchTemplate[0].length);
		assertEquals(6, trimSearchTemplate[1].length);
		assertEquals("A", trimSearchTemplate[1][0]);
		assertEquals("equals", trimSearchTemplate[0][4]);
		assertEquals("B", trimSearchTemplate[1][4]);
	}
	@Test
	public void testShouldSplitOpertorsOKWithAND() throws Exception {
		String[][] trimSearchTemplate = matcher.trimSearchTemplate("==:100, AND:>:0".split(","));
		assertEquals("==", trimSearchTemplate[0][0]);
		assertEquals("AND:>", trimSearchTemplate[0][1]);
	}
	@Test
	public void testShouldSplitOpertorsOKWithOR() throws Exception {
		String[][] trimSearchTemplate = matcher.trimSearchTemplate("==:A, OR:>:0".split(","));
		assertEquals("==", trimSearchTemplate[0][0]);
		assertEquals("OR:>", trimSearchTemplate[0][1]);
	}
	@Test
	public void testShouldHandleTemplateTooLongForItem() throws Exception {
		boolean match = matcher.isMatch("R1".split(","), new String[] { "","","","equals:R1"}, column);
		assertFalse(match);		
	}
	@Test
	public void testShouldHandleNotMatchThis() throws Exception {
		assertTrue(matcher.isMatch("ServiceType=System".split(","), new String[] { "contains:ServiceType=System"}, column));
		assertFalse(matcher.isMatch("ServiceType=System".split(","), new String[] { "notContains:ServiceType=System"}, column));
	}
	@Test
	public void testShouldHandleOROnFirstItem() throws Exception {
		assertTrue(matcher.isMatch("R1, R2, R3, R4".split(","), new String[] { "equals:R1", "OR:equals:R1"}, column));
	}
	@Test
	public void testShouldHandleOROnOthertemItem() throws Exception {
		assertTrue(matcher.isMatch("R1, R2, R3, R4".split(","), new String[] { "", "", "", "equals:R4", "OR:equals:R4"}, column));
	}
	@Test
	public void testShouldHandleANDOnFirstItem() throws Exception {
		assertTrue(matcher.isMatch("R1, R2, R3, R4".split(","), new String[] { "equals:R1", "AND:equals:R1"}, column));
	}
	
	@Test
	public void testShouldHandleANDnOthertemItem() throws Exception {
		assertTrue(matcher.isMatch("R1, R2, R3, R4".split(","), new String[] { "", "", "", "equals:R4", "AND:equals:R4"}, column));
	}
	
	@Test
	public void testShouldExtractANDItems() throws Exception {
		List<AND> result = matcher.getANDArray(new String[] { "one", "AND:operation:VALUE", "AND:operation2:VALUE", "", "stuff", "AND:op:Value"} );
		assertEquals(3, result.size());
		assertEquals(0, result.get(0).fieldPos);
		assertEquals(0, result.get(1).fieldPos);
		assertEquals(2, result.get(2).fieldPos);
	}
	
	@Test
	public void testShouldSupportANDFunction() throws Exception {
		int processANDs = matcher.processANDs("R1, R2, R2, R3,".split(","), "equals,AND:equals".split(","), "R1,R1".split(","), column);
		assertEquals(1, processANDs);
		
	}
	@Test
	public void testShouldSupportANDFunctionWithNumbers() throws Exception {
		int processANDs = matcher.processANDs("100".split(","), "==,AND:>".split(","), "100,0".split(","), column );
		assertEquals(1, processANDs);
	}
	@Test
	public void testShouldSupportANDFunctionUsingContains() throws Exception {
		int processANDs = matcher.processANDs("100".split(","), "contains,AND:notContains".split(","), "100,XXX".split(","), column);
		assertEquals(1, processANDs);
	}
	@Test
	public void testShouldSupportMatchOnOtherPosition() throws Exception {
		int processANDs = matcher.processANDs("R1, R2, R3, R4,".split(","), ",,,equals,AND:equals".split(","), ",,,R4,R4".split(","), column);
		assertEquals(1, processANDs);
	}
	@Test
	public void testShouldSupportMultplieANDsOnDifferentPositions() throws Exception {
		int processANDs = matcher.processANDs("R1, R2, R3, R4,".split(","), "equals,AND:equals,,equals,AND:equals".split(","), "R1,R1,,R3,R3".split(","), column);
		assertEquals(2, processANDs);
	}
	@Test
	public void testShouldSupportMultplieANDsOnSamePositions() throws Exception {
		int processANDs = matcher.processANDs("R1, R2, R3, R4,".split(","), ",,,equals,AND:equals,AND:equals,AND:equals".split(","), ",,,R3,R4,R4,R4".split(","), column);
		assertEquals(3, processANDs);
	}
	
	@Test
	public void testShouldContainsAny() throws Exception {
		StringContainsAny matcher = new Matchers.StringContainsAny("containsAny");
		assertTrue(matcher.isApplicable("containsAny"));
		
		assertTrue(matcher.match("R1", "R1, R2, R3,", column));
		assertTrue(matcher.match("R3", "R1, R2,R3,", 2));
		assertFalse(matcher.match("R3", "F1, F2,F3,", 0));
		assertTrue(matcher.match("1=A, 2=A,3=A", "1=A,3=A,", 0));
		assertTrue(matcher.match("1=A, 2=A,3=A", "1=A,3=A,", 1));
	}
	
	@Test
	public void testShouldNotContainsAny() throws Exception {
		StringNotContainsAny matcher = new Matchers.StringNotContainsAny("notContainsAny");
		assertTrue(matcher.isApplicable("notContainsAny"));
		
		assertFalse("String DOES contains entry so return FALSE", matcher.match("R1", "R1, R2, R3,", column));
		assertTrue(!matcher.match("R3", "R1, R2,R3,", column));
		assertFalse(!matcher.match("R3", "F1, F2,F3,", column));
		assertTrue(!matcher.match("1=A, 2=A,3=A", "1=A,3=A,", column));
	}
	@Test
	public void testShouldWorkNotContainsAny() throws Exception {
		StringNotContainsAny matcher = new Matchers.StringNotContainsAny("notContainsAny");
		assertTrue(matcher.isApplicable("notContainsAny"));
		String lhs = "localhost-80-0"; 
		String rhs = "localhost-80-48, localhost-80-47, localhost-80-11, localhost-80-33, localhost-80-36, localhost-80-55, localhost-80-26, localhost-80-56, localhost-80-24, localhost-80-35, localhost-80-41, localhost-80-34, localhost-80-12, localhost-80-59, localhost-80-29, localhost-80-22, localhost-80-54, localhost-80-32, localhost-80-15, localhost-80-10, localhost-80-52, localhost-80-38, localhost-80-23, localhost-80-28, localhost-80-39, localhost-80-17, localhost-80-58, localhost-80-19, localhost-80-25, localhost-80-14, localhost-80-40, localhost-80-13, localhost-80-27, localhost-80-50, localhost-80-57, localhost-80-46, localhost-80-49, localhost-80-51, localhost-80-43, localhost-80-44, localhost-80-21, localhost-80-18, localhost-80-37, localhost-80-45, localhost-80-20, localhost-80-31, localhost-80-16, localhost-80-30, localhost-80-53, localhost-80-42, localhost-80-7, localhost-80-5, localhost-80-3, localhost-80-9, localhost-80-1, localhost-80-2, localhost-80-6, localhost-80-4, localhost-80-8, localhost-80-0";
		assertFalse(matcher.match(lhs, rhs, column));
		
	}
	@Test
	public void testShouldContainsAnyWithListStringArg() throws Exception {
		StringContainsAny matcher = new Matchers.StringContainsAny("containsAny");
		String[] resourceIds = new String[] { "R1", "R2", "R3"};
		
		String string = com.liquidlabs.common.collection.Arrays.toString(resourceIds);
		assertTrue(matcher.match("R1", string, column));
	}
	@Test
	public void testShouldNOTContainsAnyWithListStringArg() throws Exception {
		StringContainsAny matcher = new Matchers.StringContainsAny("containsAny");
		String[] resourceIds = new String[] { "R1", "R2", "R3"};
		
		String string = com.liquidlabs.common.collection.Arrays.toString(resourceIds);
		System.out.println(string);
		assertTrue(matcher.match("XR1", string, column));
		assertFalse(matcher.match("XRX1", string, column));
	}
	@Test
	public void testShouldNOTEqualsAnyWithListStringArg() throws Exception {
		StringEqualsAny matcher = new Matchers.StringEqualsAny("equalsAny");
		String[] resourceIds = new String[] { "R1", "R2", "R3"};
		
		String string = com.liquidlabs.common.collection.Arrays.toString(resourceIds);
		System.out.println(string);
		assertFalse(matcher.match("XR1", string, column));
	}
	
	@Test
	public void testNotContains() throws Exception {
		StringNotContains matcher = new Matchers.StringNotContains("notContains");
		assertFalse("should be False - true - ie itContains it - but not", matcher.match("mobius.local-11025:myBundle-0.01:serviceName","myBundle-0.01:serviceName", column));
		assertTrue("should be false, contains == false, return !false",matcher.match("mobius.local-11025:myBundle-0.01:serviceName", "myBundle-0.01:otherName", column));
	}
	@Test
	public void testNumberShouldBeLessThan() throws Exception {
		NumberLessThan matcher = new Matchers.NumberLessThan("");
		assertFalse(matcher.match("100", "100", column));
		assertTrue(matcher.match("100", "101", column));
	}
	@Test
	public void testNumberShouldBeGreaterThanThan() throws Exception {
		NumberGreaterThan matcher = new Matchers.NumberGreaterThan("");
		assertFalse(matcher.match("100", "100", column));
		assertTrue(matcher.match("100", "99", column));
	}
}
