package com.liquidlabs.log.search.functions;


import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.search.functions.CountSingleDelta.Data;
import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class CountSingleDeltaTest {

	private CountSingleDelta countDelta;
	FieldSet fieldSet = FieldSets.get();

	long time = DateTimeUtils.currentTimeMillis();

	private MatchResult matchResult;
	private String rawLineData;
	private long requestStartTimeMs;



	@Before
	public void setUp() throws Exception {
		countDelta = new CountSingleDelta("tag", FieldSets.fieldName, FieldSets.fieldName);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testShouldDoCountSingleWithGroupBy() throws Exception {
		String pattern = "(\\w+) (\\w+)";
		FieldSet fieldSet = new FieldSet("(*) (*)", "data", "value");
		CountSingle count = new CountSingle("min", "data", "value");
		count.execute(fieldSet, fieldSet.getFields("groupA applyABC"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 1);

		count.execute(fieldSet, fieldSet.getFields("groupA applyABC"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 2);

		Map<String,Integer> results = count.getResults();
		System.out.println("R:" + results);
		assertEquals(1,results.size());
		assertEquals(1, results.get("groupA" + LogProperties.getFunctionSplit() + "applyABC").intValue());
	}
	
	@Test
	public void shouldCountFlipFlopValueDaltas() throws Exception {
		Data object1 = new Data(100,getMap(new String[0], new Integer[0]), null);
		Data object2 = new Data(100,getMap(new String[] { "a" }, new Integer[] { 10 }), null);
		
		// FLIP
		Map<String, Integer> result = countDelta.getDeltaFromBucketDatas(object1, object2);
		assertTrue(result.containsKey("a"));
		assertEquals(new Integer(1), result.get("a"));
		
		// FLOP
		Map<String, Integer> result2 = countDelta.getDeltaFromBucketDatas(object2, object1);
		assertTrue(result2.containsKey("a"));
		assertEquals(new Integer(-1), result2.get("a"));
	}
	@Test
	public void shouldCountPositiveDeltaValue() throws Exception {
		Data object = new Data(100,getMap(new String[0], new Integer[0]), null);
		Data object2 = new Data(100,getMap(new String[] { "a" }, new Integer[] { 10 }), null);
		Map<String, Integer> result = countDelta.getDeltaFromBucketDatas(object, object2);
		assertTrue(result.containsKey("a"));
		assertEquals(new Integer(1), result.get("a"));
	}
	@Test
	public void shouldCountNegativeDeltaValue() throws Exception {
		Data object = new Data(100,getMap(new String[] { "a" }, new Integer[] { 10 }), null);
		Data object2 = new Data(100,getMap(new String[0], new Integer[0]), null);
		Map<String, Integer> result = countDelta.getDeltaFromBucketDatas(object, object2);
		assertTrue(result.containsKey("a"));
		assertEquals( new Integer(-1), result.get("a"));
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
