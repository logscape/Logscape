package com.liquidlabs.space.raw;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.Executors;

import org.junit.Before;
import org.junit.Test;

import com.liquidlabs.space.Space;
import com.liquidlabs.space.map.ArrayStateSyncer;
import com.liquidlabs.space.map.MapImpl;
import com.liquidlabs.space.notify.EventHandler;
import com.liquidlabs.space.notify.NotifyEventHandler;

public class SpaceoutTest  {

	private SpaceImpl space;
	private long timeout = 9999;
	private int limit = 9999;
	private String partition = "SPACE";
	private String split = Space.DELIM;
	EventHandler eventHandler = new NotifyEventHandler("xx", "partitionName", 1024, Executors.newScheduledThreadPool(1));
	private ArrayStateSyncer stateSyncer;

	@Before
	public void setUp() throws Exception {
		space = new SpaceImpl(partition, new MapImpl("333", partition, 1024, true, stateSyncer), eventHandler);
	}

	
	@Test
	public void testSplitSymbol() throws Exception {
		String[] splitResults = com.liquidlabs.common.collection.Arrays.split(split, "0123" + split + "201034");
		assertTrue(splitResults.length == 2);
	}
	@Test
	public void testShouldMatch2PartsA_AND_B() throws Exception {
		space.write("someKeyA", "One|A".replaceAll("\\|", Space.DELIM), timeout);
		space.write("someKeyB", "One|B".replaceAll("\\|", Space.DELIM), timeout);
		String[] keys1 = space.readKeys(new String [] { "equals:One|equals:A".replaceAll("\\|", Space.DELIM)}, 99);
		assertEquals(1, keys1.length);
		String[] keys2 = space.readMultiple(new String [] { "equals:One|equals:A".replaceAll("\\|", Space.DELIM)}, 99);
		assertEquals(1, keys2.length);
	}
	
	@Test
	public void testShouldMatchWithEqualsContainsRHS() throws Exception {
		space.write("someKeyA", "One|  1=A, 2=A,3=A".replaceAll("\\|", Space.DELIM), timeout);
		space.write("someKeyB", "Two|  1=A,2=B,3=A".replaceAll("\\|", Space.DELIM), timeout);
		space.write("someKeyC", "Three|1=B, 2=B, 3=A".replaceAll("\\|", Space.DELIM), timeout);
		space.write("someKeyD", "Four |1=B, 2=A,3=A".replaceAll("\\|", Space.DELIM), timeout);
		String[] keys = space.getKeys(new String [] { "|equalsAny:1=A,3=A".replaceAll("\\|", Space.DELIM)}, 99);
		assertEquals(4, keys.length);
	}
	@Test
	public void testShouldMatchWithAnyContainsRHS() throws Exception {
		space.write("someKeyA", "One|  1=A, 2=A,3=A".replaceAll("\\|", Space.DELIM), timeout);
		space.write("someKeyB", "Two|  1=A,2=B,3=A".replaceAll("\\|", Space.DELIM), timeout);
		space.write("someKeyC", "Three|1=B, 2=B, 3=A".replaceAll("\\|", Space.DELIM), timeout);
		space.write("someKeyD", "Four |1=B, 2=A, 3=A".replaceAll("\\|", Space.DELIM), timeout);
		String[] keys = space.getKeys(new String [] { "|containsAny:1=A,3=A".replaceAll("\\|", Space.DELIM)}, 99);
		assertEquals(4, keys.length);
	}
	@Test
	public void testShouldMatchWithMultipleContainsRHS() throws Exception {
		space.write("someKeyA", "One|  1=A, 2=A,3=A".replaceAll("\\|", Space.DELIM), timeout);
		space.write("someKeyB", "Two|  1=A,2=B,3=A".replaceAll("\\|", Space.DELIM), timeout);
		space.write("someKeyC", "Three|1=B, 2=B, 3=A".replaceAll("\\|", Space.DELIM), timeout);
		space.write("someKeyD", "Four |1=B, 2=A, 3=A".replaceAll("\\|", Space.DELIM), timeout);
		String[] keys = space.getKeys(new String [] { "|contains:1=A,3=A".replaceAll("\\|", Space.DELIM)}, 99);
		assertEquals(2, keys.length);
		
		String[] keys2 = space.getKeys(new String [] { "|contains:1=B,3=A".replaceAll("\\|", Space.DELIM)}, 99);
		assertEquals(2, keys2.length);
		
	}
	
	@Test
	public void testGetKeys() throws Exception {
		space.write("someKeyA", "A|B|C".replaceAll("\\|", Space.DELIM), timeout);
		space.write("someKeyB", "B|B|C".replaceAll("\\|", Space.DELIM), timeout);
		space.write("someKeyC", "A|A|D".replaceAll("\\|", Space.DELIM), timeout);
		space.write("someKeyD", "DA|DA|DC".replaceAll("\\|", Space.DELIM), timeout);
		String[] keys = space.getKeys(new String [] { "A", "|B".replaceAll("\\|", Space.DELIM),"DA"}, 99);
		assertEquals(4, keys.length);
	}
	@Test
	public void testGetKeysLimit() throws Exception {
		space.write("someKeyA", "A|B|C".replaceAll("\\|", Space.DELIM), timeout);
		space.write("someKeyB", "B|B|C".replaceAll("\\|", Space.DELIM), timeout);
		space.write("someKeyC", "A|A|D".replaceAll("\\|", Space.DELIM), timeout);
		space.write("someKeyD", "DA|DA|DC".replaceAll("\\|", Space.DELIM), timeout);
		String[] keys = space.getKeys(new String [] { "A", "|B".replaceAll("\\|", Space.DELIM),"DA"}, 2);
		assertEquals(2, keys.length);
	}
	
	@Test
	public void testCount() throws Exception {
		space.write("someKeyA", "A|B|C".replaceAll("\\|", Space.DELIM), timeout);
		space.write("someKeyB", "B|B|C".replaceAll("\\|", Space.DELIM), timeout);
		space.write("someKeyC", "A|A|D".replaceAll("\\|", Space.DELIM), timeout);
		space.write("someKeyD", "DA|DA|DC".replaceAll("\\|", Space.DELIM), timeout);
		int count = space.count(new String [] { "A", "|B".replaceAll("\\|", Space.DELIM),"DA"}, -1);
		assertEquals(4, count);
	}
	@Test
	public void testCountLimit() throws Exception {
		space.write("someKeyA", "A|B|C".replaceAll("\\|", Space.DELIM), timeout);
		space.write("someKeyB", "B|B|C".replaceAll("\\|", Space.DELIM), timeout);
		space.write("someKeyC", "A|A|D".replaceAll("\\|", Space.DELIM), timeout);
		space.write("someKeyD", "DA|DA|DC".replaceAll("\\|", Space.DELIM), timeout);
		int count = space.count(new String [] { "A", "|B".replaceAll("\\|", Space.DELIM),"DA"}, 2);
		assertEquals(2, count);
	}
	
	@Test
	public void testPutGet() throws Exception {
		space.write("someKeyA", "A|B|C".replaceAll("\\|", split), timeout);
		space.write("someKeyB", "B|B|C".replaceAll("\\|", split), timeout);
		space.write("someKeyC", "A|A|D".replaceAll("\\|", split), timeout);
		space.write("someKeyD", "DA|DA|DC".replaceAll("\\|", split), timeout);
		testGetWithTemplateMatch(2, "|B|C".replaceAll("\\|", split));
		testGetWithTemplateMatch(2, "|A|".replaceAll("\\|", split));
		testGetWithTemplateMatch(3, "||C".replaceAll("\\|", split));
		testGetWithTemplateMatch(2, "A||C".replaceAll("\\|", split));
		testGetWithTemplateMatch(2, "A||C".replaceAll("\\|", split));
		
		
	}
	@Test
	public void testPutRegExpStuff() throws Exception {
		String regexp  = ".*\\[Full GC (\\d+)K->(\\d+)K\\((\\d+)K\\), (\\d+.\\d+) secs.*";
		space.write("someKeyA",  regexp, timeout);
		String result = space.read("someKeyA");
		assertEquals(regexp, result);
		
	}

	private void testGetWithTemplateMatch(int i, String template) {
		System.out.println("\n=====  =:" + template);
		String[] matchingItems = space.readMultiple(new String[] { template }, limit);
		for (String string : matchingItems) {
			System.out.println("       =:" + string);
		}
		assertEquals(i, matchingItems.length);
	}
	@Test
	public void testGetKeysForTemplateMatch() throws Exception {
		space.write("machineA", "A|B|C", timeout );
		space.write("machineB", "B|B|C", timeout);
		space.write("machineC", "A|A|D", timeout);
		space.write("machineD", "DA|DA|DC", timeout);
		String[] keys = space.readKeys(new String[] { "|B" }, limit );
		assertEquals(2, keys.length);
	}
	@Test
	public void testGetKeysForFunctionalMatch() throws Exception {
		space.write("lonlt3151", "lonlt3151.uk.db1.com|500|40".replaceAll("\\|", split), timeout);
		space.write("lonlt3152", "lonlt3152.uk.db1.com|600|50".replaceAll("\\|", split), timeout);
		space.write("lonlt3161", "lonlt3161.uk.db2.com|900|60".replaceAll("\\|", split), timeout);
		space.write("lonlt3171", "lonlt3171.uk.db3.com|1000|70".replaceAll("\\|", split), timeout);
		space.write("lonlt3181", "lonlt3181.uk.db4.com|1100|90".replaceAll("\\|", split), timeout);
		String[] keys = space.readKeys(new String[] { "3151" }, limit);
		assertEquals(1, keys.length);
		assertEquals("lonlt3151", keys[0]);
	}
	@Test
	public void testGetKeysForFunctionalMatch2() throws Exception {
		space.write("lonlt3151", "lonlt3151.uk.db1.com|500|40".replaceAll("\\|", split), timeout);
		space.write("lonlt3152", "lonlt3152.uk.db1.com|600|50".replaceAll("\\|", split), timeout);
		space.write("lonlt3161", "lonlt3161.uk.db2.com|900|60".replaceAll("\\|", split), timeout);
		space.write("lonlt3171", "lonlt3171.uk.db3.com|1000|70".replaceAll("\\|", split), timeout);
		space.write("lonlt3181", "lonlt3181.uk.db4.com|1100|90".replaceAll("\\|", split), timeout);
		String[] keys = space.readKeys(new String[] { "db1" }, limit );
		assertEquals(2, keys.length);
		assertEquals("lonlt3151", keys[0]);
	}
	@Test
	public void testGetKeysForFunctionalMatch3() throws Exception {
		space.write("lonlt3151", "lonlt3151.uk.db1.com|500|40".replaceAll("\\|", split), timeout);
		space.write("lonlt3152", "lonlt3152.uk.db1.com|600|50".replaceAll("\\|", split), timeout);
		space.write("lonlt3161", "lonlt3161.uk.db2.com|900|60".replaceAll("\\|", split), timeout);
		space.write("lonlt3171", "lonlt3171.uk.db3.com|1000|70".replaceAll("\\|", split), timeout);
		space.write("lonlt3181", "lonlt3181.uk.db4.com|1100|90".replaceAll("\\|", split), timeout);
		String[] keys = space.readKeys(new String[] { "contains:db1", "contains:db2" }, limit );
		assertEquals(3, keys.length);
		assertEquals("lonlt3151", keys[0]);
	}

	
	@Test
	public void testShouldBreakIt() throws Exception {
		SpaceImpl spaceImpl = new SpaceImpl(partition, new MapImpl("srcUID", partition, 3, true, stateSyncer), eventHandler);
		spaceImpl.write("neil", "neil|neil|neil".replaceAll("\\|", split), Long.MAX_VALUE);
		spaceImpl.write("damian", "damian|damian|damian".replaceAll("\\|", split), Long.MAX_VALUE);
		spaceImpl.write("tony", "tony|tony|tony".replaceAll("\\|", split), Long.MAX_VALUE);
		spaceImpl.write("jim", "jim|jim|jim".replaceAll("\\|", split), Long.MAX_VALUE);
		
		String[] readMultiple = spaceImpl.readMultiple(new String[] { "jim||".replaceAll("\\|", split) }, limit);
		assertEquals(1, readMultiple.length);
	}

	@Test
	public void testGetKeysDoesNotReturnDuplicates() throws Exception {
		space.write("lonlt3151", "lonlt3151.uk.db1.com,500,40", timeout);
		space.write("lonlt3152", "lonlt3152.uk.db1.com,600,50", timeout);
		space.write("lonlt3161", "lonlt3161.uk.db2.com,900,60", timeout);
		space.write("lonlt3171", "lonlt3171.uk.db3.com,1000,70", timeout);
		space.write("lonlt3181", "lonlt3181.uk.db4.com,1100,90", timeout);
		String[] keys = space.readKeys(new String[] { "contains:db2", "contains:db2" }, limit );
		assertEquals(1, keys.length);
		assertEquals("lonlt3161", keys[0]);
	}
	@Test
	public void testGetKeysEqualsAny() throws Exception {
		for (int i = 0; i < 100; i++){
			space.write("Resource=" + i, "Resource="+i,timeout);
		}
		String resourceIds = "Resource=0,Resource=1,Resource=10,Resource=13,Resource=14,Resource=15,Resource=17,Resource=18,Resource=19,Resource=2,Resource=20,Resource=21,Resource=22,Resource=24,Resource=25,Resource=26,Resource=27,Resource=28,Resource=29,Resource=3,Resource=34,Resource=39,Resource=4,Resource=42,Resource=44,Resource=45,Resource=5,Resource=50,Resource=54,Resource=58,Resource=6,Resource=60,Resource=61,Resource=62,Resource=64,Resource=65,Resource=67,Resource=68,Resource=69,Resource=7,Resource=70,Resource=71,Resource=72,Resource=73,Resource=8,Resource=80,Resource=81,Resource=83,Resource=9,Resource=98";

		String[] readKeys = space.readKeys(new String[] { "equalsAny:\"" + resourceIds + "\""}, 1000);

		ArrayList<String> arrayList1 = new ArrayList<String>(Arrays.asList(readKeys));
		Collections.sort(arrayList1);
		
		ArrayList<String> arrayList2 = new ArrayList<String>(Arrays.asList(resourceIds.split(",")));
		Collections.sort(arrayList2);
		
		assertEquals("Lengths did not match, wanted:" + resourceIds.split(",").length + " got:" + readKeys.length ,resourceIds.split(",").length, readKeys.length);

	}
	@Test
	public void testGetKeysContainsAnyWithMultipleArgs() throws Exception {
		for (int i = 0; i < 100; i++){
			space.write("Resource=" + i, "Resource="+i,timeout);
		}
		String[] readKeys = space.readKeys(new String[] { "containsAny:Resource=30,Resource=20,Resource=10"}, 1000);
		
		System.out.println("Keys:" + Arrays.asList(readKeys));
		
		// should only evaluate them in order
		assertEquals(3, readKeys.length);
		assertEquals("Resource=30", readKeys[0]);
		assertEquals("Resource=20", readKeys[1]);
		assertEquals("Resource=10", readKeys[2]);
		
	}
	@Test
	public void testGetKeysContainsAny() throws Exception {
		for (int i = 0; i < 100; i++){
			space.write("Resource=" + i, "Resource="+i,timeout);
		}
		String[] readKeys = space.readKeys(new String[] { "containsAny:Resource=1"}, 1000);
		
		System.out.println("Keys:" + Arrays.asList(readKeys));
		
		assertEquals(11, readKeys.length);
		
	}
	@Test
	public void testGetKeysNotContainsAny() throws Exception {
		for (int i = 0; i < 100; i++){
			space.write("Resource=" + i, "Resource="+i,timeout);
		}
		
		String[] readKeys = space.readKeys(new String[] { "notContainsAny:Resource=1"}, 1000);
		System.out.println("Keys:" + Arrays.asList(readKeys));
		
		assertEquals(100 - 11, readKeys.length);
		
	}

}
