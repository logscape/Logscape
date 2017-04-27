package com.liquidlabs.log.search.functions;


import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.search.filters.Filter;
import com.liquidlabs.log.search.functions.CountDelta.Data;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CountDeltaTest {

	private CountDelta countDelta;
	private ValueSetter valueSetter;
	private List<Filter> filters;
	FieldSet fieldSet = FieldSets.get();

	@Before
	public void setUp() throws Exception {
		countDelta = new CountDelta("tag", FieldSets.fieldName, FieldSets.fieldName);
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void shouldCountPositiveDeltaValue() throws Exception {
		Data object = new Data(10L, getMap(new String[0], new Integer[0]), valueSetter);
		Data object2 = new Data(10L,getMap(new String[] { "a" }, new Integer[] { 10 }), valueSetter);
		Map<String, Integer> result = countDelta.getDeltaFromBucketDatas(object, object2);
		assertTrue(result.containsKey("a"));
		assertEquals(new Integer(10), result.get("a"));
	}
	@Test
	public void shouldCountNegativeDeltaValue() throws Exception {
		Data object = new Data(10L,getMap(new String[] { "a" }, new Integer[] { 10 }), valueSetter);
		Data object2 = new Data(10L,getMap(new String[0], new Integer[0]), valueSetter);
		Map<String, Integer> result = countDelta.getDeltaFromBucketDatas(object, object2);
		assertTrue(result.containsKey("a"));
		assertEquals( new Integer(-10), result.get("a"));
	}

	private HashMap<String, Object> getMap(String[] keys, Integer[] values) {
		HashMap<String, Object> hashMap = new HashMap<String, Object>();
		int i = 0;
		for (String key : keys) {
			hashMap.put(key, values[i++]);
		}
		return hashMap;
	}
}
