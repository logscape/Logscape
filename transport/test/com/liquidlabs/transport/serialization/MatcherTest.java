package com.liquidlabs.transport.serialization;

import junit.framework.TestCase;

public class MatcherTest extends TestCase {
	Matchers matcher = new Matchers();
	private int column;
	
	
	public void testShouldNOTMatch() throws Exception {
		assertTrue(!matcher.isMatch(new String[] {"A", "B", "C"}, new String[] { "B", "B", "C"}, column));
	}

	public void testPerformance() throws Exception {
		String[] split = "com.liquidlabs.orm.Mapping,http://nyceqgrd0603:9000,com.db.dbeg.ds.BasicConfig,idle,http://nyceqgrd0603:8000,com.db.dbeg.ds.DataSynapseConfiguration,<Mapping>,".split(",");
		String[] split2 = "equals:com.liquidlabs.orm.Mapping,,,,equals:http://nyceqgrd0603:8000,equals:com.db.dbeg.ds.DataSynapseConfiguration,,".split(",");
		long currentTimeMillis = System.currentTimeMillis();
		int amount = 100000;
		for (int i = 0; i < 100000; i++){
			matcher.isMatch(split,split2, column);
		}
		// 156MS elapsed time for 100,000 items
		System.out.println("Elapsed MS:" + (System.currentTimeMillis() - currentTimeMillis));
		System.out.println("WorkDone:" + (amount/((System.currentTimeMillis() - currentTimeMillis))));
	}
	public void testShouldMatchMappingString() throws Exception {
		assertTrue("Should have matched but didnt!", matcher.isMatch(
				"A,STUFF,STUFF2,STUFF3,B,C,<Mapping>,".split(","), 
				"A,,,,equals:B,equals:C,,".split(","), column));
	}
	
	public void testManyMatch() throws Exception {
		assertTrue(matcher.isMatch(new String[] {"A", "500", "C"}, new String[] { null, ">:499", null}, column));
		assertTrue(matcher.isMatch(new String[] {"acharacter", "500", "C"}, new String[] { "contains:char", null, null}, column));
		assertTrue(matcher.isMatch(new String[] {"acharacter", "500", "C"}, new String[] { "contains:char", ">=:500", null}, column));
	}
	public void testShouldNotMatchTemplateWithUserType() throws Exception {
		assertFalse("Should NOT have matched!", matcher.isMatch("1000, com.liquidlabs.orm.RemoteClientSideTest$Address, homeAddress, userId, com.liquidlabs.orm.RemoteClientSideTest$User, <Mapping>".split(","), 
				",,,,,,equals:User,,".split(","), column));
	}
	public void testShouldMatchTemplateAndArgsCompletely() throws Exception {
		assertTrue(matcher.isMatch(new String[] {"A", "B", "C"}, new String[] { "A", "B", "C"}, column));
	}
	public void testShouldMatchWithFunnyTemplate() throws Exception {
		assertTrue(matcher.isMatch(new String[] {"A", "B", "C"}, "A,".split(","), column));
	}

	public void testShouldPartialMatch() throws Exception {
		assertTrue(matcher.isMatch(new String[] {"A", "B", "C"}, ",B,".split(","), column ));
	}
	public void testShouldPartialSMatch() throws Exception {
		assertTrue(matcher.isMatch(new String[] {"A", "B", "C"}, ",B".split(","), column ));
	}
	public void testShouldParitalMatchWithWhiteSpace() throws Exception {
		assertTrue(matcher.isMatch(new String[] {"A", "B", "C"}, "  ,   B  ".split(","), column ));
	}
	public void testMatchBothParts() throws Exception {
		assertFalse(matcher.isMatch(new String[] {"acharacter", "500", "C"}, new String[] { "equals:acharacter", "<:400", null}, column));
	}
	public void testShouldNotMatch() throws Exception {
		assertFalse(matcher.isMatch(new String[] {"A", "B", "C"}, new String[0], column));
	}
	public void testShouldMAtchWithKeyAndField() throws Exception {
		assertTrue("Should have matched", matcher.isMatch("_lease_,WriteLease,1193083079,someKeyA,A,B,C".split(","), "equals:_lease_,,<=:1193083079".split(","), column));
	}
	
}
