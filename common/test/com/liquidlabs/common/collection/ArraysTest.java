package com.liquidlabs.common.collection;

import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.util.List;

import org.joda.time.DateTimeUtils;
import org.junit.Test;

public class ArraysTest {
	
	public String aaa;
	public String ddd;
	public String ccc;
	public String bbb;
	
	@Test
	public void shouldMakeCollectionToStringLookCommaDelimited() throws Exception {
		List<String> data = java.util.Arrays.asList("one", "two", "three");
		String string = Arrays.toString(data);
		assertEquals("one,two,three", string);
		
		
	}
	
	@Test
	public void shouldSplitAndDOIncludeData() throws Exception {
		String searchText = "one\n\ntwo\nthree\n";
		List<Integer> splits = Arrays.getSplits("\n".toCharArray(),searchText.toCharArray(), true, false);
		assertTrue("Didnt get any splits", splits.size() > 0);
		assertEquals("one\n", searchText.substring(0, splits.get(0)));
		assertEquals("\n", searchText.substring(splits.get(0), splits.get(1)));
		assertEquals("two\n", searchText.substring(splits.get(1), splits.get(2)));
		assertEquals("three\n", searchText.substring(splits.get(2), splits.get(3)));
	}
	@Test
	public void shouldSplitAndNotIncludeData() throws Exception {
		String searchText = "one-SP-two-SP-three";
		List<Integer> splits = Arrays.getSplits("-SP-".toCharArray(),searchText.toCharArray(), true, false);
		assertTrue("Didnt get any splits", splits.size() > 0);
		assertEquals("one-SP-", searchText.substring(0, splits.get(0)));
		assertEquals("two-SP-", searchText.substring(splits.get(0), splits.get(1)));
	}
	
	@Test
	public void shouldMatchArrays() throws Exception {
			assertEquals(-1, Arrays.arrayContains("one".getBytes(), "this is on".getBytes()));
			assertEquals(8, Arrays.arrayContains("one".getBytes(), "this is one".getBytes()));
			assertEquals(-1, Arrays.arrayContains("NOT".getBytes(), "this is one".getBytes()));
			assertEquals(-1, Arrays.arrayContains("oneStuff".getBytes(), "one".getBytes()));
	}
	

	@Test
	public void testShouldGetFieldsSorted() throws Exception {
		
		Field[] sortFields = Arrays.sortFields(getClass().getDeclaredFields());
		for (Field field : sortFields) {
			assertNotNull(field);
		}
	}
}
