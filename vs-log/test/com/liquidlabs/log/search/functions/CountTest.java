package com.liquidlabs.log.search.functions;

import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.transport.serialization.Convertor;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class CountTest {
	
	
	private String hostname;
	private String filenameOnly;
	private String filename;
	private String pattern;
	private long time;
	FieldSet fieldSet = new FieldSet("(*) (**)","group","user");
	private MatchResult matchResult;
	private String rawLineData;
	private long requestStartTimeMs;
	
	@Test
	public void shouldBeGoodWithTOPLimit() throws Exception {
		Count count2 = new Count("TAG", "group", "user");
		count2.setTopLimit(5);
		count2.execute(fieldSet, fieldSet.getFields("one user1,user2"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 1);
		count2.execute(fieldSet, fieldSet.getFields("one user3,user4"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 2);
		count2.execute(fieldSet, fieldSet.getFields("one user5,user6"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 3);
		count2.execute(fieldSet, fieldSet.getFields("one user7,user8"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 4);
		count2.execute(fieldSet, fieldSet.getFields("two user9,user10"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 5);
		count2.execute(fieldSet, fieldSet.getFields("one user7,user8"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 6);
		count2.execute(fieldSet, fieldSet.getFields("two user9,user10"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 7);
		count2.execute(fieldSet, fieldSet.getFields("one user7,user8"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 8);
		count2.execute(fieldSet, fieldSet.getFields("two user9,user10"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 9);
		count2.execute(fieldSet, fieldSet.getFields("one user7,user8"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 11);
		count2.execute(fieldSet, fieldSet.getFields("two user9,user10"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 12);
		
		Map results = count2.getResults();
		assertEquals(5, results.size());
	}

	
	@Test
	public void shouldAggregateToAnotherBucket() throws Exception {
		Count count2 = new Count("TAG", "group", "user");
		count2.execute(fieldSet, fieldSet.getFields("one user1,user2"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 1);
		count2.execute(fieldSet, fieldSet.getFields("one user2,user2"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 2);
		count2.execute(fieldSet, fieldSet.getFields("one user3,user2"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 3);
		count2.execute(fieldSet, fieldSet.getFields("one user3,user2"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 4);
		count2.execute(fieldSet, fieldSet.getFields("two user3,user2"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 5);

		Count aggCount = new Count("TAG", "group", "user");
		Set<String> keySet = count2.getResults().keySet();
		for (String key : keySet) {
			aggCount.updateResult(key, ((Integer) count2.getResults().get(key)).intValue());
		}
		
		assertEquals(aggCount.getResults().size(), count2.getResults().size());
		System.out.println("Agg:" + aggCount.getResults());
		
	}

	@Test
	public void shouldDoCountOnGroup0() throws Exception {
		Count count2 = new Count("TAG", "", "");
		count2.execute(fieldSet, fieldSet.getFields("one user1"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 1);
		count2.execute(fieldSet, fieldSet.getFields("one user2"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 2);
		count2.execute(fieldSet, fieldSet.getFields("one user3"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 3);
		count2.execute(fieldSet, fieldSet.getFields("one user3"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 4);
		
		Map results = count2.getResults();
		System.out.println("Results:" + results);
		assertEquals(1, results.size());
		assertEquals(4, ((Integer) results.get("TAG")).intValue());
	}
//	@Test - dont do it - count needs to be done explicitly
	public void shouldDoCountOnGroupSPLIT() throws Exception {
		Count count2 = new Count("TAG", "group", "user");
		count2.execute(fieldSet, fieldSet.getFields("one user1,user2"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 1);
		count2.execute(fieldSet, fieldSet.getFields("one user2,user2"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 2);
		count2.execute(fieldSet, fieldSet.getFields("one user3,user2"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 3);
		count2.execute(fieldSet, fieldSet.getFields("one user3,user2"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 4);
		count2.execute(fieldSet, fieldSet.getFields("two user3,user2"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 5);
		
		Map results = count2.getResults();
		System.out.println("Results:" + results);
		assertEquals(5, results.size());
		assertEquals(5, ((IntValue) results.get("one_user2")).value);
	}
	
	
// TEST IS DISABLED as we need to only support explicit count-splitting (i.e. split and count values with a delimited - as in the test below	
//	@Test
	public void shouldDoCountOnSPLIT() throws Exception {
		Count count2 = new Count("TAG", "user", "user");
		count2.execute(fieldSet, fieldSet.getFields("one user1,user2"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 1);
		count2.execute(fieldSet, fieldSet.getFields("one user2,user2"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 2);
		count2.execute(fieldSet, fieldSet.getFields("one user3,user2"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 3);
		count2.execute(fieldSet, fieldSet.getFields("one user3,user2"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 4);
		
		Map results = count2.getResults();
		System.out.println("Results:" + results);
		assertEquals(3, results.size());
		assertEquals(5, results.get("user2"));
	}
	@Test
	public void shouldDoCountOnGroupSTAR() throws Exception {
		Count count2 = new Count("TAG", "*", "group");
		count2.execute(fieldSet, fieldSet.getFields("one user1"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 1);
		count2.execute(fieldSet, fieldSet.getFields("one user2"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 2);
		count2.execute(fieldSet, fieldSet.getFields("one user3"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 3);
		count2.execute(fieldSet, fieldSet.getFields("one user3"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 4);
		
		Map results = count2.getResults();
		System.out.println("Results:" + results);
		assertEquals(1, results.size());
		assertEquals(4, ((Integer) results.get("TAG")).intValue());
	}
	@Test
	public void shouldDoCountOnGroup() throws Exception {
		Count count2 = new Count("TAG", "", "group");
		count2.execute(fieldSet, fieldSet.getFields("one user1"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 1);
		count2.execute(fieldSet, fieldSet.getFields("one user2"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 2);
		count2.execute(fieldSet, fieldSet.getFields("one user3"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 3);
		count2.execute(fieldSet, fieldSet.getFields("one user3"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 4);
		
		Map results = count2.getResults();
		System.out.println("Results:" + results);
		assertEquals(1, results.size());
		assertEquals(4, ((Integer) results.get("one")).intValue());
	}
	@Test
	public void shouldDoCountOnGroupAndApply() throws Exception {
		Count count2 = new Count("TAG", "group", "user");
		count2.execute(fieldSet, fieldSet.getFields("one user1"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 1);
		count2.execute(fieldSet, fieldSet.getFields("one user2"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 2);
		count2.execute(fieldSet, fieldSet.getFields("one user3"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 3);
		count2.execute(fieldSet, fieldSet.getFields("one user3"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 4);
		
		Map results = count2.getResults();
		System.out.println("Results:" + results);
		assertEquals(3, results.size());
		assertEquals(2, ((Integer) results.get("one!user3")).intValue());
	}
	@Test
	public void shouldDoCountOnGroupAndApply2() throws Exception {
		FieldSet fieldSet = new FieldSet("(*) (**)","filename","level");
		Count count2 = new Count("TAG", "filename", "level");
		count2.execute(fieldSet, fieldSet.getFields("agent.log INFO"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 1);
		count2.execute(fieldSet, fieldSet.getFields("agent.log ERROR"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 2);
		count2.execute(fieldSet, fieldSet.getFields("f1.log INFO"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 3);
		count2.execute(fieldSet, fieldSet.getFields("f1.log INFO"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 4);
		
		Map results = count2.getResults();
		System.out.println("Results:" + results);
		assertEquals(3, results.size());
		assertEquals(2, ((Integer) results.get("f1.log!INFO")).intValue());
	}
	@Test
	public void shouldDoCountOnGroupAndApplyWithNullField() throws Exception {
		fieldSet.addSynthField("cpu", "group", "cpu:(*)", "count()", false, false);
		Count count2 = new Count("TAG", "cpu", "user");
		
		count2.execute(fieldSet, fieldSet.getFields("cpu:100 user1"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 1);
		count2.execute(fieldSet, fieldSet.getFields("cpu: user2"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 2);
		
		Map results = count2.getResults();
		System.out.println("Results:" + results);
		assertEquals(1, results.size());
	}

	@Test
	public void shouldDoCountFaster() throws Exception {
		fieldSet.addSynthField("cpu", "group", "cpu:(*)", "count()", false, false);
		CountFaster count2 = new CountFaster("TAG", "user");
		count2.increment("TAG", "user1", 100);

		Map results = count2.getResults();

		byte[] serialize = Convertor.serialize(count2);
		CountFaster deserialize = (CountFaster) Convertor.deserialize(serialize);


		System.out.println("Results:" + results);
		assertEquals(1, results.size());
	}

}
