package com.liquidlabs.log.search.functions;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.quantile.TDigest;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.fields.FieldSet;
import org.joda.time.DateTimeUtils;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

@SuppressWarnings("unchecked")
public class FunctionsTest  {
	
	
	String hostname = "host";
	String filename = "file";
	String filenameOnly = "file";
	long time = DateTimeUtils.currentTimeMillis();
	FieldSet fieldSet = new FieldSet("(*) (*)", "data", "value");
	private MatchResult matchResult;
	private String rawLineData;
	private long requestStartTimeMs;

    // https://github.com/addthis/stream-lib/blob/master/src/test/java/com/clearspring/analytics/stream/quantile/TDigestTest.java
	@Test
	public void shouldDoPercentile() throws Exception {
		Percentile func = new Percentile("0.9", "", "a");
		func.execute("100", "");
        func.execute("10", "");
        func.execute("9", "");
		Map results = func.getResults();
        Map.Entry<String, TDigest> next = (Map.Entry) results.entrySet().iterator().next();
        TDigest tdig = next.getValue();
        tdig.compress();

        double quantile = tdig.quantile(99);
        System.out.println("Q:" + quantile);
        assertNotNull(quantile);
        System.out.println(tdig.quantile(0.01));
        System.out.println(tdig.quantile(0.25));
        System.out.println(tdig.quantile(0.50));
        System.out.println(tdig.quantile(0.75));
        System.out.println(tdig.quantile(0.99));



	}


	@Test
	public void shouldDoExpressions() throws Exception {
		Function first = new EvaluateExpr("test", "a * 100", "a");
        first.execute("100", "");
        Map results = first.getResults();
        Object results1 = results.get("RESULT");
        assertNotNull(results1);

    }

	@Test
    public void shouldUseByFunctionForLastValue() throws Exception {
        Function last = new By("TAG", "data","value", true);
        Function first = new By("TAG", "data","value", false);

        for (int i = 1; i < 10; i++) {
            first.execute(fieldSet, fieldSet.getFields("host " + i), "", time, matchResult, rawLineData, requestStartTimeMs, i);
            last.execute(fieldSet, fieldSet.getFields("host " + i), "", time, matchResult, rawLineData, requestStartTimeMs, i);
        }
        Map results = last.getResults();
        assertEquals(1,results.size());
        assertEquals("9", results.get("host"));
        assertEquals("1", first.getResults().get("host"));

    }


    @Test
    public void avgDeltaPercentWorks() throws Exception {
        AverageDeltaPercent fun = new AverageDeltaPercent();
        assertEquals(10.0, fun.calculateDelta(11.00,10.0),0.05);
        assertEquals(-10.0, fun.calculateDelta(9.0,10.0),0.05);


    }
	
	@Test
	public void shouldEscapeOurChars() throws Exception {
			Average avg = new Average("TAG", "data","value");
		
		for (int i = 1; i < 10; i++) {
			avg.execute(fieldSet, fieldSet.getFields("ho!st " + i), "", time, matchResult, rawLineData, requestStartTimeMs, i);
		}
		Map results = avg.getResults();
		assertEquals(1,results.size());
		assertEquals(5.0, ((Averager)results.get("ho_st")).average(), 0.0);
		
	}
	@Test
	public void shouldGetCorrectName() throws Exception {
		FunctionBase fb = new Average("TAG", "host","cpu");
		assertEquals("Average", fb.name());
	}
	
	@Test
	public void shouldAverageWithSynthNull() throws Exception {
		fieldSet.addSynthField("cpu", "value", "cpu:(*)", "count()", true, true);
		fieldSet.addSynthField("host", "data", "host:(*)", "count()", true, true);
		Average avg = new Average("TAG", "host","cpu");
		
		for (int i = 1; i < 10; i++) {
			String nextLine = "host:AAA cpu:" + i;
			if (i == 5) {
				nextLine = nextLine.replaceAll("AAA", "");
			}
			avg.execute(fieldSet, fieldSet.getFields(nextLine), "", time, matchResult, rawLineData, requestStartTimeMs, i);
		}
		Map results = avg.getResults();
		assertEquals(1,results.size());
		assertEquals(5.0, ((Averager)results.get("AAA")).average(), 0.0);
		
	}
	
	@Test
	public void shouldAverageWithGroup() throws Exception {
		Average avg = new Average("TAG", "data","value");
		
		for (int i = 1; i < 10; i++) {
			avg.execute(fieldSet, fieldSet.getFields("host " + i), "", time, matchResult, rawLineData, requestStartTimeMs, i);
		}
		Map results = avg.getResults();
		assertEquals(1,results.size());
		assertEquals(5.0, ((Averager)results.get("host")).average(), 0.0);
		
	}
	@Test
	public void shouldAverage() throws Exception {
		Average avg = new Average("TAG", "","value");
		
		for (int i = 1; i < 10; i++) {
			avg.execute(fieldSet, fieldSet.getFields("abc " + i), "", time, matchResult, rawLineData, requestStartTimeMs, i);
		}
		Map results = avg.getResults();
		assertEquals(1,results.size());
		assertEquals(5.0, ((Averager)results.values().iterator().next()).average(),0.0);
		
	}
	
	@Test
	public void testShouldDoMin() throws Exception {
		Min min = new Min("min","data","value");
		
		for (int i = 1; i < 10; i++) {
			min.execute(fieldSet, fieldSet.getFields("abc " + i), "", time, matchResult, rawLineData, requestStartTimeMs, 1);
		}
		Map results = min.getResults();
		assertEquals(1,results.size());
		assertEquals(1.0, results.get("abc"));
	}

    @Test
    public void shouldApplyCountWithConcatenateFieldAndGroupField() throws Exception {


        // normal use
        FieldSet fieldSet = new FieldSet("(*) (*) (*)", "user", "action", "host");

        String[] fields = fieldSet.getFields("u1 login server1");


        // concat group-by-use
        Count count2 = new Count("TAG", "1+host","user");
        matchResult = new MatchResult("mr1", "mr2");
        count2.execute(fieldSet, fields, "data", time, matchResult, rawLineData, requestStartTimeMs, 1);
        Map results2 = count2.getResults();
        System.out.println("Results:" + results2);
        // i.e. login + server
        assertTrue(results2.toString().contains("mr2-server1"));

    }

    @Test
    public void shouldApplyCountWithConcatenateField() throws Exception {


        // normal use
        FieldSet fieldSet = new FieldSet("(*) (*) (*)", "user", "action", "host");
        Count count = new Count("TAG", "action","user");

        String[] fields = fieldSet.getFields("u1 login server1");
        count.execute(fieldSet, fields, "data", time, matchResult, rawLineData, requestStartTimeMs, 1);

        Map results = count.getResults();
        System.out.println("Results:" + results);

        // concat group-by-use
        Count count2 = new Count("TAG", "action+host","user");
        count2.execute(fieldSet, fields, "data", time, matchResult, rawLineData, requestStartTimeMs, 1);
        Map results2 = count2.getResults();
        System.out.println("Results:" + results2);
        // i.e. login + server
        assertTrue(results2.toString().contains("login-server"));


        // concat field-use
        Count count3 = new Count("TAG", "host","user+action");
        count3.execute(fieldSet, fields, "data", time, matchResult, rawLineData, requestStartTimeMs, 1);
        Map results3 = count3.getResults();
        System.out.println("Results:" + results3);
        // i.e. login + server
        assertTrue(results3.toString().contains("u1-login"));




    }

	
	@Test
	public void shouldApplyCountUnique() throws Exception {
		CountUnique count = new CountUnique("TAG", "","value");
		
		//MatchResult match = RegExpUtil.matches(pattern, "abc groupA");
		count.execute(fieldSet, fieldSet.getFields("abc groupA"), "data", time, matchResult, rawLineData, requestStartTimeMs, 1);
		count.execute(fieldSet, fieldSet.getFields("abc groupA"), "data", time, matchResult, rawLineData, requestStartTimeMs, 2);
		count.execute(fieldSet, fieldSet.getFields("abc groupA"), "data", time, matchResult, rawLineData, requestStartTimeMs, 3);
		
		count.execute(fieldSet, fieldSet.getFields("abc groupB"), "data", time, matchResult, rawLineData, requestStartTimeMs, 4);
		count.execute(fieldSet, fieldSet.getFields("abc groupB"), "data", time, matchResult, rawLineData, requestStartTimeMs, 5);
		count.execute(fieldSet, fieldSet.getFields("abc groupB"), "data", time, matchResult, rawLineData, requestStartTimeMs, 6);
		
		Map<?,HyperLogLog> results  = count.getResults();
		assertEquals(2, results.get("TAG").cardinality());
		
	}
	@Test
	public void shouldApplyCountUniqueWithGroup() throws Exception {
		String pattern = "(\\w+) (\\w+)";
		CountUnique count = new CountUnique("countUnique", "data","value");
		
		count.execute(fieldSet, fieldSet.getFields("groupA group1"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 1);
		count.execute(fieldSet, fieldSet.getFields("groupA group2"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 2);
		count.execute(fieldSet, fieldSet.getFields("groupA group3"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 3);
		count.execute(fieldSet, fieldSet.getFields("groupB group1"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 4);
		count.execute(fieldSet, fieldSet.getFields("groupB group2"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 5);
		count.execute(fieldSet, fieldSet.getFields("groupB group3"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 6);
		
		Map results = count.getResults();
		assertEquals(2, results.size());
		assertNotNull(results.get("groupA"));
		assertNotNull(results.get("groupB"));
		
		
	}
    
//	@Test - DONT DO IT - count needs to be done on explicit
	public void testShouldDoCountWithGroupByOnCommaList() throws Exception {
		String pattern = "(\\w+) (\\S+)";
		Count count = new Count("min", "data", "value");
		count.execute(fieldSet, fieldSet.getFields("abc groupA,groupB,groupC"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 1);
		
		count.execute(fieldSet, fieldSet.getFields("abc groupA,groupB,groupC"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 2);
		
		Map<String, IntValue> results = count.getResults();
		assertEquals(3,results.size());
		System.out.println("RR:" + results);
		assertEquals(2, results.get("abc_groupA").value);
		assertEquals(2, results.get("abc_groupB").value);
		assertEquals(2, results.get("abc_groupC").value);
		System.out.println("Results:" + results);
	}
	@Test
	public void testShouldDoCountWithGroupBy() throws Exception {
		String pattern = "(\\w+) (\\w+)";
		Count count = new Count("min", "data", "value");
		count.execute(fieldSet, fieldSet.getFields("abc groupA"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 1);
		
		count.execute(fieldSet, fieldSet.getFields("abc groupA"), pattern, time, matchResult, rawLineData, requestStartTimeMs, 2);
		
		Map<String, Integer> results = count.getResults();
		assertEquals(1,results.size());
        System.out.println("Results:" + results);
		Integer c =  results.get("abc" + LogProperties.getFunctionSplit() + "groupA");
		assertEquals(2, c.intValue());
		System.out.println("Results:" + results);
	}
	
	@Test
	public void testShouldDoMinWithGroupBy() throws Exception {
		String pattern = "(\\w+) (\\d+)";
		Min min = new Min("min", "data", "value");
		for (int i = 3; i < 10; i++) {
			min.execute(fieldSet, fieldSet.getFields("abc " + i), pattern, time, matchResult, rawLineData, requestStartTimeMs, i);
		}
		
		for (int i = 5; i < 11; i++) {
			min.execute(fieldSet, fieldSet.getFields("def " + i), pattern, time, matchResult, rawLineData, requestStartTimeMs, i);
		}
		
		Map results = min.getResults();
		assertEquals(2,results.size());
		assertEquals(3.0, results.get("abc"));
		assertEquals(5.0, results.get("def"));
	}

	@Test
	public void testShouldDoNothingWhenNotANumber() throws Exception {
		String pattern = "(\\w+) (\\w+)";
		Min min = new Min("min", "data", "value");
		for (int i = 3; i < 10; i++) {
			min.execute(fieldSet, fieldSet.getFields("abc abc"), pattern, time, matchResult, rawLineData, requestStartTimeMs, i);
		}
		
		
		Map results = min.getResults();
	}


    @Test
    public void testShouldDoSummary() throws Exception {
        String pattern = "\\w+ (\\d+)";
        Summary summary = new Summary("summary", "data", "value");
        for (int i = 0; i < 10; i++) {
            summary.execute(fieldSet, fieldSet.getFields("abc " + i), pattern, time, matchResult, rawLineData, requestStartTimeMs, i);
        }
        Map results = summary.getResults();
        assertEquals(1,results.size());
        assertTrue(results.get("abc").toString().contains("max: 9.0"));

        Map<String, Object> other = new HashMap<String, Object>();
        other.put("summary", results.values().iterator().next());

        summary.updateResult("summary", other);
        summary.updateResult("summary", other);
        Map secondResults = summary.getResults();


    }

    @Test
    public void testShouldDoSummaryWithGroupBy() throws Exception {
        String pattern = "(\\w+) (\\d+)";
        Summary max = new Summary("min", "data", "value");
        for (int i = 3; i < 10; i++) {
            max.execute(fieldSet, fieldSet.getFields("abc " + i), pattern, time, matchResult, rawLineData, requestStartTimeMs, i);
        }

        for (int i = 5; i < 11; i++) {
            max.execute(fieldSet, fieldSet.getFields("def " + i), pattern, time, matchResult, rawLineData, requestStartTimeMs, i);
        }

        Map results = max.getResults();
        assertEquals(2,results.size());
        assertTrue(results.get("abc").toString().contains("max: 9.0"));
        assertTrue(results.get("def").toString().contains("max: 10.0"));
    }


    @Test
	public void testShouldDoMax() throws Exception {
		String pattern = "\\w+ (\\d+)";
		Max max = new Max("max", "data", "value");
		for (int i = 0; i < 10; i++) {
			max.execute(fieldSet, fieldSet.getFields("abc " + i), pattern, time, matchResult, rawLineData, requestStartTimeMs, i);
		}
		Map results = max.getResults();
		assertEquals(1,results.size());
		assertEquals(9.0, results.get("abc"));
	}
	
	@Test
	public void testShouldDoMaxWithGroupBy() throws Exception {
		String pattern = "(\\w+) (\\d+)";
		Max max = new Max("min", "data", "value");
		for (int i = 3; i < 10; i++) {
			max.execute(fieldSet, fieldSet.getFields("abc " + i), pattern, time, matchResult, rawLineData, requestStartTimeMs, i);
		}
		
		for (int i = 5; i < 11; i++) {
			max.execute(fieldSet, fieldSet.getFields("def " + i), pattern, time, matchResult, rawLineData, requestStartTimeMs, i);
		}
		
		Map results = max.getResults();
		assertEquals(2,results.size());
		assertEquals(9.0, results.get("abc"));
		assertEquals(10.0, results.get("def"));
	}

	@Test
	public void testShouldDoNothingWhenNotANumberMax() throws Exception {
		String pattern = "(\\w+) (\\w+)";
		Max max = new Max("min", "data", "value");
		for (int i = 3; i < 10; i++) {
			max.execute(fieldSet, fieldSet.getFields("abc abc"), pattern, time, matchResult, rawLineData, requestStartTimeMs, i);
		}
		Map results = max.getResults();
	}
}
