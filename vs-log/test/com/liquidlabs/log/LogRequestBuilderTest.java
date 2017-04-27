package com.liquidlabs.log;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.common.regex.RegExpUtil;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.search.Query;
import com.liquidlabs.log.search.filters.*;
import com.liquidlabs.log.search.functions.*;
import com.liquidlabs.log.search.functions.txn.SyntheticTransAccumulate;
import com.liquidlabs.log.search.functions.txn.SyntheticTransTrace;
import com.liquidlabs.log.space.LogRequest;
import com.liquidlabs.log.space.Replay;
import com.liquidlabs.log.space.ReplayType;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class LogRequestBuilderTest {

    private LogRequestBuilder builder;
    DateTime to = new DateTime();
    DateTime from = to.minusHours(1);

    @Before
    public void setUp() throws Exception {
        builder = new LogRequestBuilder();
    }


    @Test
    public void shouldMatchHashes() throws Exception {
        LogRequest request1 = builder.getLogRequest("sub", Arrays.asList("* | _type.equals(log4j) level.count(_host) summary.index(read)"), null, to.minusMinutes(5).getMillis(), to.getMillis());
        LogRequest request2 = builder.getLogRequest("sub", Arrays.asList("* | _type.equals(log4j) level.count(_host) summary.index(write)"), null, to.minusMinutes(5).getMillis(), to.getMillis());

        assertEquals(request1.cacheKey(), request2.cacheKey());
    }
    @Test
    public void shouldApplyByFirstAndLast() throws Exception {
        LogRequest request = builder.getLogRequest("sub", Arrays.asList("* | _tag.by(_host,H) _host.first(_filename) _path.last(_filename)"), null, to.minusMinutes(5).getMillis(), to.getMillis());
        assertEquals(3, request.query(0).functions().size());
    }

    @Test
    public void shouldRemoveSystemFilters() throws Exception {
        LogRequest request = builder.getLogRequest("sub", Arrays.asList("* | _tag.equals(xxx) crap.equals(crap)"), null, to.minusMinutes(5).getMillis(), to.getMillis());
        assertEquals(2, request.query(0).filters().size());
        request.removeSystemFieldFilters();
        assertEquals(1, request.query(0).filters().size());
    }

    @Test
    public void shouldFilterMultiTagsCorrectlty() throws Exception {
        String q = "* | _agent.equals(Agent) _tag.count() _tag.exclude(logscape-logs) _tag.equals(UnixApp)";
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(q), null, to.minusMinutes(5).getMillis(), to.getMillis());
        LogFile logFile = new LogFile("filename",1,"fieldSet","DontMatch");

        assertFalse(request.isSearchable(logFile, "stuff"));
        logFile.setTags("UnixApp,OtherStuff");
        assertTrue(request.isSearchable(logFile, "stuff"));
    }
    @Test
    public void shouldFilterFileCorrectlty() throws Exception {
        String q = "* | _agent.equals(Agent) _tag.count() _tag.exclude(logscape-logs) _tag.equals(UnixApp)";
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(q), null, to.minusMinutes(5).getMillis(), to.getMillis());
        LogFile logFile = new LogFile("filename",1,"fieldSet","DontMatch");

        assertFalse(request.isSearchable(logFile, "stuff"));
        logFile.setTags("UnixApp");
        assertTrue(request.isSearchable(logFile, "stuff"));
    }



    @Test
	public void shouldGetBookBucketWidth() throws Exception {
    	 LogRequest request = builder.getLogRequest("sub", Arrays.asList("* | bucketWidth(1m)"), null, to.minusMinutes(5).getMillis(), to.getMillis());
    	 // 20 buckets when at 5 minutes
         assertEquals(60, request.getBucketWidthSecs());
	}

    @Test
    public void shouldRecordErrorsForUnknownFunction() {
        final LogRequest request = builder.getLogRequest("f", Arrays.asList("* | f.contents(adf)"), null, to.minusMinutes(5).getMillis(), to.getMillis());
        assertThat(request.hasErrors(), is(true));
    }

    @Test
	public void shouldGetSecondBuckets() throws Exception {
    	String q1 = "*";
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(q1), null, to.minusMinutes(1).getMillis(), to.getMillis());
        assertEquals(60, request.getBucketCount());
	}
    @Test
    public void shouldParseFieldAnalyticWithDots() throws Exception {
        String q1 = "type='stuff' | agent.cpu.avg()";
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(q1), null, from.getMillis(), to.getMillis());
        Query query = request.queries().get(0);
        Average avg = (Average) query.functions().get(0);
        assertEquals("agent.cpu", avg.getApplyToField());
    }
    @Test
    public void shouldParseFieldFilterWithDots() throws Exception {
        String q1 = "type='stuff' | agent.cpu.contains(99)";
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(q1), null, from.getMillis(), to.getMillis());
        Query query = request.queries().get(0);
        Contains contains = (Contains) query.filters().get(0);
        assertEquals("agent.cpu", contains.group());
    }


    @Test
	public void shouldParseExpressionProperly() throws Exception {
    	String q1 = "(A|B) | 1.count()";
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(q1), null, from.getMillis(), to.getMillis());
        Query query = request.query(0);
        assertTrue(query.matches("some A").isMatch());
        assertTrue(query.matches("some B").isMatch());

	}

    @Test
	public void shouldPassThroughTTL() throws Exception {
    	String q1 = " CPU AND STUFF | ttl(5)";
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(q1), null, from.getMillis(), to.getMillis());
        assertEquals(5, request.getTimeToLiveMins());
        
    	String q2 = " CPU AND STUFF |autocancel(6)";
        LogRequest request2 = builder.getLogRequest("sub", Arrays.asList(q2), null, from.getMillis(), to.getMillis());
        assertEquals(6, request2.getTimeToLiveMins());

	}
    @Test
	public void shouldSupportOFFSET_Hour() throws Exception {
    	LogRequest request = builder.getLogRequest("sub", Arrays.asList("CPU AND STUFF | offset(1h)"), null, from.getMillis(), to.getMillis());

        System.out.println("Times:" + request.toString());
        assertTrue("Start time was identical", request.getStartTimeMs() < from.getMillis());
        assertTrue("End time was identical R:" + new DateTime(request.getEndTimeMs()) + " T:" + to, request.getEndTimeMs() < to.getMillis());
        
    	LogRequest request2 = builder.getLogRequest("sub", Arrays.asList("CPU AND STUFF | offset(1w)"), null, from.getMillis(), to.getMillis());
        assertTrue("Start time was identical", request2.getStartTimeMs() < from.getMillis());
        assertTrue("End time was identical", request2.getEndTimeMs() < to.getMillis());
        
        assertTrue(request.getStartTimeMs() != request2.getStartTimeMs());
	}

    
    @Test
    public void shouldDetectOldSchoolSearchType() throws Exception {
        String q1 = " CPU AND STUFF | contains(CPU) 1.contains(CPU) 0.count(,c_tag) 1.avg(2, a_tag) 1.avgBy(host, avgby_tag)";
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(q1), null, from.getMillis(), to.getMillis());
        Query query = request.query(0);
        Contains filter = (Contains) query.filters().get(0);
        assertNotNull(filter);
        Contains filter1 = (Contains) query.filters().get(1);
        assertEquals("1", filter1.group);
        
        
        Count count = (Count) query.functions().get(0);
        assertNotNull(count);
        assertEquals("c_tag", count.getTag());
        assertEquals("", count.applyToGroup);

        Average avg = (Average) query.functions().get(1);
        assertNotNull(avg);
        assertEquals("a_tag", avg.getTag());
        assertEquals("2", avg.groupByGroup);
        assertEquals("1", avg.applyToGroup);
        
    }

    @Test
    public void shouldPopulateFiltersWithGroups() throws Exception {
        String q1 = " * | contains(CPU) 1.contains(CPU) 1.not(xxx,yy) cpu.gt(10) mem.lt(100)";
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(q1), null, from.getMillis(), to.getMillis());
        
        Query query = request.query(0);
        List<Filter> filters = query.filters();
		Contains contains0 = (Contains) filters.get(0);
        assertEquals("0", contains0.group);
        
        Contains contains1 = (Contains) filters.get(1);
        assertEquals("1", contains1.group);
        assertNotNull(contains1.toStringId());
        
        Not not1 = (Not) filters.get(2);
        assertEquals("1", not1.group);
        assertNotNull(not1.value());
        assertEquals(2, ((String[])not1.value()).length);
        assertNotNull(not1.toStringId());
        
        GreaterThan gtTen = (GreaterThan) filters.get(3);
        assertEquals("cpu", gtTen.group());
        assertNotNull(gtTen.value());
        assertNotNull(gtTen.toStringId());
        
        LessThan ltOneHundred = (LessThan) filters.get(4);
        assertEquals("mem", ltOneHundred.group());
        assertNotNull(ltOneHundred.value());
        assertNotNull(ltOneHundred.toStringId());
        
    }

    @Test
    public void shouldHandleStar() throws Exception {
        String q1 = "type='one' | data.count(*) data.avg() data.countUnique()";
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(q1), null, from.getMillis(), to.getMillis());
        Query query = request.query(0);
        assertEquals(3, query.functions().size());
        Count count = (Count) query.functions().get(0);
        assertEquals("data", count.getTag());
        assertEquals("data", count.applyToGroup);
    }

    @Test
    public void shouldHandleZeroParamsAndSetTagProperly() throws Exception {
        String q1 = "sourceType='one' | data.count() ";
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(q1), null, from.getMillis(), to.getMillis());
        Query query = request.query(0);
        assertTrue(query.functions().size() > 0);
        Count count = (Count) query.functions().get(0);
        assertTrue(count.getTag().equals("data"));
        assertTrue(count.group().equals("data"));
        assertTrue(count.applyToGroup.equals("data"));
    }

    @Test
    public void shouldHandleONEParamsAndSetTagProperly() throws Exception {
        String q1 = "sourceType='one' | dataApplyGroup.count(hostGroupBy,myCountTag) dataApplyGroup.count(hostGroupBy,myCountTag2) ";
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(q1), null, from.getMillis(), to.getMillis());
        Query query = request.query(0);
        assertTrue(query.functions().size() > 0);
        Count count = (Count) query.functions().get(0);
        assertEquals("myCountTag", count.getTag());
        assertEquals("hostGroupBy", count.groupByGroup);
        assertEquals("dataApplyGroup", count.applyToGroup);

        Count count2 = (Count) query.functions().get(1);
        assertEquals("myCountTag2", count2.getTag());
    }

    @Test
    public void shouldFieldExpressionWhereQueryLevelFilterIsApplied() throws Exception {
        // type='log4j' CPU | cpu.avg()
        String q1 = "type='log4j' CPU | cpu.avg()";
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(q1), null, from.getMillis(), to.getMillis());
        Query query = request.query(0);
        assertEquals("CPU", query.pattern());
    }

    @Test
    public void shouldFieldExpressionWhereQueryLevelFilterIsNOTApplied() throws Exception {
        // type='log4j' CPU | cpu.avg()
        String q1 = "type='log4j'  | cpu.avg()";
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(q1), null, from.getMillis(), to.getMillis());
        Query query = request.query(0);
        assertEquals("*", query.pattern());
    }

 

    @Test
    public void shouldDoPipesQuery() throws Exception {
        String q1 = "(*) (*)|(*)|(*)|(*)|(*)|(*)|(*)|(*)|(*) | doStuff()";
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(q1), null, from.getMillis(), to.getMillis());
        String pattern = request.query(0).pattern();
        System.out.println("Got Expression:" + pattern);
        assertNotNull(pattern);
    }
    
    @Test
    public void shouldObeyBucketWidth() throws Exception {
        DateTime to = new DateTime();
        DateTime from = to.minusDays(7);

        ArrayList<Query> queries = new ArrayList<Query>();
        String pattern = ".* | bucketWidth(1h)";
        queries.add(new Query(0, 0, pattern, pattern, true));


        LogRequest request = builder.getLogRequest("sub", Arrays.asList(pattern), null, from.getMillis(), to.getMillis());
        // 168 hours
        System.out.println("Times:" + new DateTime(request.getStartTimeMs()));
        assertEquals(169, request.getBucketCount());
    }


    @Test
    public void shouldCalcDaysBucketsForLongSearchs() throws Exception {
        int days = 3;
        long durationMins = days * DateUtil.DAY;
        DateTime currentTime = new DateTime();
        int addExtraMins = 24 * 60 - currentTime.getMinuteOfDay();
        DateTime to = currentTime.plusMinutes(addExtraMins);
        DateTime from = to.minusMillis((int) durationMins);

        ArrayList<Query> queries = new ArrayList<Query>();
        String pattern = ".* | buckets(" + days + ")";
        queries.add(new Query(0, 0, pattern, pattern, true));

        int buckets = builder.getBuckets(queries, from.getMillis(), to.getMillis(), 1);

        assertEquals(days, buckets);

        LogRequest request = builder.getLogRequest("sub", Arrays.asList(pattern), null, from.getMillis(), to.getMillis());
        assertEquals(days, request.getBucketCount());

        System.out.println("Request:" + request);

        long totalMs = request.getBucketSizeMs() * days;
        assertEquals("Bad request:" + request, DateUtil.DAY, request.getBucketSizeMs());
        long gotDays = (totalMs / DateUtil.DAY);

        long expectedDays = (to.getMillis() - from.getMillis()) / DateUtil.DAY;
        assertEquals("GotDays[" + gotDays + "]", to.getMillis() - from.getMillis(), totalMs);

    }

    @Test
    public void shouldBlowupOnBadRegexp() throws Exception {
        String q1 = "*";
        DateTime to = new DateTime();
        DateTime from = to.minusHours(1);

        LogRequest request = builder.getLogRequest("sub", Arrays.asList(q1), null, from.getMillis(), to.getMillis());
    }

    @Test
    public void shouldNotHaveNotFilteronLOGGERRequest() throws Exception {
        String q1 = "LOGGER | doStuff()";
        DateTime to = new DateTime();
        DateTime from = to.minusHours(1);
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(q1), null, from.getMillis(), to.getMillis());
        assertEquals(0, request.query(0).filters().size());
    }


    @Test
    public void shouldGetLiveFlagCorrect() throws Exception {
        String q1 = "(.*) | live(true) ";
        DateTime to = new DateTime();
        DateTime from = to.minusHours(1);
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(q1), null, from.getMillis(), to.getMillis());

        assertTrue(request.isStreaming());
    }

    @Test
    public void shouldShouldNotHaveTrailingQhiteSpace() throws Exception {
        String q1 = "(\\S+)\\s(\\S+)\\s(\\S+)\\s(\\S+)\\s(\\S+)\\s(\\S+)\\s(\\S+)\\s(\\S+)\\s(\\S+)\\s(\\S+)\\s(\\S+)\\s(.*) ";
        DateTime to = new DateTime();
        DateTime from = to.minusHours(1);
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(q1), null, from.getMillis(), to.getMillis());

        Query query = request.queries().get(0);
        assertEquals("(\\S+)\\s(\\S+)\\s(\\S+)\\s(\\S+)\\s(\\S+)\\s(\\S+)\\s(\\S+)\\s(\\S+)\\s(\\S+)\\s(\\S+)\\s(\\S+)\\s(.*)", query.pattern());
    }

    	@Test
    public void testShouldDoHandleCountWithoutTag() throws Exception {
        int length = 290;
        String q1 = "* |  level.count(filename,cpu-tag)  data.countDistinct(cpu,cpu-dist-tag) data.countSingleDelta(,csingle-delta-tag)";
        DateTime to = new DateTime();
        DateTime from = to.minusMinutes(length);
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(q1), null, from.getMillis(), to.getMillis());

        Count function2 = (Count) request.queries().get(0).functions().get(0);
        assertEquals("cpu-tag", function2.getTag());

        CountSingle function3 = (CountSingle) request.queries().get(0).functions().get(1);
        assertEquals("cpu-dist-tag", function3.getTag());
        
        CountSingleDelta function4 = (CountSingleDelta) request.queries().get(0).functions().get(2);
        assertEquals("csingle-delta-tag", function4.getTag());
    }
    	
    @Test
    public void testShouldDoBucketWidthMINSCommand() throws Exception {
        int length = 290;
        String q1 = ".* |  bucketWidth(5m)";
        DateTime to = new DateTime();
        DateTime from = to.minusMinutes(length);
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(q1), null, from.getMillis(), to.getMillis());
        int bucketCount = request.getBucketCount();
        System.out.println("b:" + bucketCount + " ==" + (length / bucketCount) + " total:");
        assertEquals(5, length / bucketCount);
    }
    @Test
    public void testShouldDoBucketWidthHoursCommand() throws Exception {
        int hoursLength = 24;
        String q1 = ".* |  bucketWidth(1h)";
        DateTime to = new DateTime();
        DateTime from = to.minusHours(hoursLength);
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(q1), null, from.getMillis(), to.getMillis());
        int bucketCount = request.getBucketCount();
        System.out.println("b:" + bucketCount + " ==" + (hoursLength / bucketCount) + " total:");
        assertEquals(25, bucketCount);
    }
    @Test
    public void testShouldDoBucketWidthDaysCommand() throws Exception {
        int daysLength = 24;
        String q1 = ".* |  bucketWidth(1d)";
        DateTime to = new DateTime();
        DateTime from = to.minusDays(daysLength);
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(q1), null, from.getMillis(), to.getMillis());
        System.out.println("From:"+ new DateTime(request.getStartTimeMs()));
        System.out.println("To:"+ new DateTime(request.getEndTimeMs()));
        int bucketCount = request.getBucketCount();
        System.out.println("b:" + bucketCount + " ==" + (daysLength / bucketCount) + " total:");
        assertEquals(25, bucketCount);
    }


    @Test
    public void testShouldDoBucketWidthCommand() throws Exception {
        int length = 290;
        String q1 = ".* |  count(HTML,7,7) count(PDF,77) bucketWidth(5)";
        DateTime to = new DateTime();
        DateTime from = to.minusMinutes(length);
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(q1), null, from.getMillis(), to.getMillis());
        int bucketCount = request.getBucketCount();
        System.out.println("b:" + bucketCount + " ==" + (length / bucketCount) + " total:");
        assertEquals(5, length / bucketCount);
    }


    @Test
    public void shouldHandleMultipleFunctions() throws Exception {
        String q1 = ".* |  data.count(7,HTML) data.count(7,PDF)";
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(q1), null, 1000, 2000);
        assertEquals(2, request.query(0).functions().size());
        assertEquals("HTML", request.query(0).functions().get(0).getTag());
        assertEquals("PDF", request.query(0).functions().get(1).getTag());
    }

    @Test
    public void shouldMakeBucketsTimeSensitive() throws Exception {
        String query = "Exception |  buckets(timeBased)";
        DateTime toTime = new DateTime();
        DateTime fromTime = toTime.minusDays(14);

        LogRequest request = builder.getLogRequest("sub", Arrays.asList(query), null, fromTime.getMillis(), toTime.getMillis());
        int buckets = request.getBucketCount();
        assertEquals(85, buckets);


    }

    @Test
    public void testShouldNotScrewupRegexp() throws Exception {
        String query = "[0-9]+\\-[0-9]+\\-[0-9]+\\s+[0-9]+\\:[0-9]+\\:[0-9]+\\,[0-9]+\\s+[A-Z\\_\\-]+\\s+ResourceAgent\\-2\\-1\\s+([A-Za-z\\.]+)\\s+\\w+\\s+\\w+\\s+\\w+\\s+\\w+\\s+[A-Z\\_\\-]+";
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(query), null, 1, 2);

        Query query2 = request.query(0);
        System.out.println("1:" + query);
        System.out.println("2:" + query2.pattern());
        assertEquals(query, query2.pattern());

    }


    @Test
    public void shouldCollectGroupContainsFilter() throws Exception {
        String query = " (\\S+)\\s(\\S+)\\s(\\S+)\\s(\\S+)\\s(\\S+)\\s(\\S+)\\s(\\S+)\\s(\\S+)\\s(\\S+)\\s(\\S+)\\s(\\S+)\\s(.*) | data.sum(Data) data.groupContains(200)";
        String var = "host1";
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(query), var, 1, 2);
        Filter filter = request.query(0).filters().get(0);
        assertNotNull(filter);
        Contains contains = (Contains) filter;
        assertEquals("data", contains.group);
        System.out.println("r:" + request.copy());
    }

    @Test
    public void shouldHandleVariableSubsOnLHS() throws Exception {
        String query = "{0} | data.not(SEARCH, LOGGER) data.contains({0})";
        String var = "host1";
        //LogRequest request = builder.getLogRequest(sber, logFilter, variables, logFileFilter, fromTime.getMillis(), toTime.getMillis(), user.fileExcludes, user.fileIncludes);
        LogRequest request =  builder.getLogRequest("sub", Arrays.asList(query), var, 1, 2);
        String pattern = request.query(0).pattern();
        assertEquals("host1", pattern);
    }
    @Test
    public void shouldHandleVariableSubsOnRHS() throws Exception {
    	String query = "Exception | data.not(SEARCH, LOGGER) data.contains({0})";
    	String var = "host1";
    	LogRequest request = builder.getLogRequest("sub", Arrays.asList(query), var, 1, 2);
    	String filter = request.query(0).filters().get(1).toString();
    	assertTrue("Got:" + filter, filter.contains("host1"));
    }
    @Test
    public void shouldHandleMultiVariableSubsOnRHS() throws Exception {
    	String query = "Exception | data.contains({0}) data.contains({1})";
    	String var = "host1,host2";
    	LogRequest request = builder.getLogRequest("sub", Arrays.asList(query), var, 1, 2);
    	String filter = request.query(0).filters().get(0).toString();
    	assertTrue("Got:" + filter, filter.contains("host1"));
    	String filter2 = request.query(0).filters().get(1).toString();
    	assertTrue("Got:" + filter, filter2.contains("host2"));

    }


    @Test
    public void shouldHandleVariableSubsOnNOTRHS() throws Exception {
        String query = "Exception | data.not(SEARCH, LOGGER) data.not({0})";
        String var = "host1";
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(query), var, 1, 2);
        String filter = request.query(0).filters().get(1).toString();
        assertTrue("Got:" + filter, filter.contains("host1"));
    }

    @Test
    public void testShouldDoAverageWhenGroupByNotGiven() throws Exception {
        String query = ".*\t.*\t(.*)\t(.*)\t(.*)\t(.*) | rx_1.avg() rx_2.avg(RX_2) id.not(Batch)";
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(query), "", 1, 2);

        assertEquals(2, request.query(0).functions().size());
        assertTrue(request.query(0).isGroupBy());
    }

    @Test
    public void testGetPipeIndex() throws Exception {
        assertEquals(0, builder.getPipeIndex("|bc|d"));
        assertEquals(3, builder.getPipeIndex("abc|d"));
        assertEquals(6, builder.getPipeIndex("abc\\|d"));

    }

    @Test
    public void testRequestStringHandlesPipe() throws Exception {
        String request1 = ".*TradeService\\.(\\w+).*\\|(\\d+)ms.* | trans('search',1, elapsedOnly=true, elapsedMin=1000)";
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(request1), "", 1, 2);
        System.out.println(">>>>>>>R:" + request.toStringId());
        String string = request.queries().get(0).toString();
        assertTrue("Got BAD String(Stuff) missing:" + string, string.contains(".*TradeService\\.(\\w+).*\\|(\\d+)ms.*"));
        assertFalse("Got BAD String(trans) exists:" + string, string.contains("trans"));
    }

    @Test
    public void testRequestStringIsUnique() throws Exception {
        String request1 = "WARN OR ERROR | trans('search',1, elapsedOnly=true, elapsedMin=1000)";
        String request2 = ".*CPU.* | hitLimit(99)";
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(request1, request2), "", 1, 2);
        System.out.println(">>>>>>>R:" + request.toStringId());
    }
    
    @Test
	public void shouldGetSynthTxn() throws Exception {
		String string = "SEARCH[(*)] | 1.txn(perf) ";
		LogRequest request = builder.getLogRequest("sub", Arrays.asList(string), "", 1, 2);
		assertEquals(1, request.queries().get(0).functions().size());
		Query query = request.query(0);
		SyntheticTransAccumulate txn = (SyntheticTransAccumulate) query.functions().get(0);
		assertNotNull(txn);
	}

    @Test
	public void shouldGetSynthTxnTrace() throws Exception {
		String string = "type='log4j' | uid.trace(thread,USERA)";
		LogRequest request = builder.getLogRequest("sub", Arrays.asList(string), "", 1, 2);
		assertEquals(1, request.queries().get(0).functions().size());
		Query query = request.query(0);
		SyntheticTransTrace txn = (SyntheticTransTrace) query.functions().get(0);
		assertNotNull(txn);
	}


    @Test
    public void testTransIsCreatedAndCopied() throws Exception {
        String request1 = "WARN OR ERROR | id.txn(search)";
        String request2 = ".*CPU.* | hitLimit(99)";
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(request1, request2), "", 1, 2);
        request.setReplay(new Replay(ReplayType.START, 10));

        LogRequest copy = request.copy(1, 2);

        assertEquals(1, copy.queries().get(0).functions().size());
    }

    @Test
    public void testHitLimitIsCopied() throws Exception {
        String request1 = "WARN OR ERROR | hitLimit(88)";
        String request2 = ".*CPU.* | hitLimit(99)";
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(request1, request2), "", 1, 2);
        request.setReplay(new Replay(ReplayType.START, 10));

        LogRequest copy = request.copy(1, 2);

        assertEquals(88, copy.queries().get(0).hitLimit());
        assertEquals(88, copy.queries().get(1).hitLimit());
        assertEquals(99, copy.queries().get(2).hitLimit());
    }

    @Test
    public void testPositionsAreCorrectlySet() throws Exception {
        String request1 = "WARN OR ERROR | hitsPerBucket(99)";
        String request2 = ".*CPU.* | hitsPerBucket(99)";
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(request1, request2), "", 1, 2);
        LogRequest copy = request.copy(1, 2);
        assertEquals(0, copy.query(0).position());
        assertEquals(0, copy.query(0).getSourcePos());
        assertEquals(1, copy.query(1).position());
        assertEquals(0, copy.query(1).getSourcePos());

        assertEquals(2, copy.query(2).position());
        assertEquals(1, copy.query(2).getSourcePos());
    }

    @Test
    public void testAutocancel() throws Exception {
        String request = "WARN OR ERROR | autocancel(100)";
        LogRequest replayRequestSimple = builder.getLogRequest("sub", Arrays.asList(request), "", 1, 2);
        assertEquals(100, replayRequestSimple.autocancel());
        System.out.println(replayRequestSimple.queries().get(0).pattern());
        LogRequest copy = replayRequestSimple.copy(1, 2);
        assertEquals(100, copy.autocancel());
    }

    @Test
    public void testExpirey() throws Exception {
        String request = "WARN OR ERROR | verbose(true)";
        LogRequest replayRequestSimple = builder.getLogRequest("sub", Arrays.asList(request), "", 1, 2);
        assertFalse(replayRequestSimple.isExpired());
        LogRequest copy = replayRequestSimple.copy(1, 2);
        assertFalse(copy.isExpired());
    }

    @Test
    public void testShouldSetUseVerbose() throws Exception {
        String request = "WARN OR ERROR | verbose(true)";
        LogRequest replayRequestSimple = builder.getLogRequest("sub", Arrays.asList(request), "", 1, 2);
        assertTrue(replayRequestSimple.isVerbose());
        LogRequest copy = replayRequestSimple.copy(1, 2);
        assertTrue(copy.isVerbose());
    }
    @Test
    public void testShouldConfigORUsageProperly() throws Exception {
        String request = "ERROR OR CPU | _filename.count()";
        LogRequest rq = builder.getLogRequest("sub", Arrays.asList(request), "", 1, 2);

        Query query = rq.query(0);
        System.out.println("Q::" + query);


    }

    @Test
    public void shouldApplyCountUnique() throws Exception {
        String request = "WARN  | data.countUnique(Stuff)";
        LogRequest replayRequestSimple = builder.getLogRequest("sub", Arrays.asList(request), "", 1, 2);
        Query query1 = replayRequestSimple.query(0);
        CountUnique count = (CountUnique) query1.functions().get(0);
        assertEquals("Stuff", count.groupByGroup);
        assertEquals("data", count.applyToGroup);
        assertEquals("data", count.tag);
    }

    @Test
    public void testShouldApplyAverageByToRegexp() throws Exception {
        String request = ".*WARN(\\d+) .* | 1.avg(source, tag)";
        LogRequest replayRequestSimple = builder.getLogRequest("sub", Arrays.asList(request), "", 1, 2);
        Query query1 = replayRequestSimple.query(0);
        assertEquals(1, query1.functions().size());
        assertTrue(query1.isGroupBy());

        Average average = (Average) query1.functions().get(0);
        assertEquals("file", average.group(), "1");

    }

    @Test
    public void testShouldHandleGTWithDecimalValue() throws Exception {
        LogRequest replayRequest = builder.getLogRequest("subscriber", Arrays.asList(".*\\[Full GC.*(\\d+\\.\\d+) secs.* | data.not(LOGGER) value.gt(0.5)"), "", 1, 10);
        assertNotNull(replayRequest);
        assertEquals("Got filters:" + replayRequest.query(0).filters().toString(), 2, replayRequest.query(0).filters().size());
    }

    @Test
    public void testHandlesMultipleSpacesInQueryWithoutBombing() throws Exception {
        String one = ".*Problem Downloading file.*";
        LogRequest request = builder.getLogRequest("subscriber", Arrays.asList(one), "", 1, 10);
        assertTrue(request.queries().size() == 1);

    }

    public void testSimpleRequestIsBuiltOkayWith2Args() throws Exception {
        String requestString = "WARN OR ERROR";
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(requestString), "", 1, 2);
        assertTrue(request.queries().size() > 0);
        Query query1 = request.query(0);
        Query query2 = request.query(1);
        assertEquals(".*WARN.*", query1.pattern());
        assertEquals(".*ERROR.*", query2.pattern());
    }


    @Test
    public void testShouldSetupGroupByProperly() throws Exception {
        String one = ".*AGENT (.+)-\\d+-\\d+ MEM.+AVAIL:(\\d+)";
        String two = ".*AGENT (.+)-\\d+-\\d+ MEM.+USED:(\\d+).*";
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(one + " | data.average(one, host)", two + " | average(two, 2)"), "", 1, 10);
        assertTrue(request.queries().get(0).isGroupBy());
        //assertFalse(request.queries().get(1).isGroupBy());
    }

    @Test
    public void testShouldNotMessUpArguments() throws Exception {
        String query = ".*AGENT (.+)-\\d+ CPU:(\\d).* | groupBy(1) average(2)";
        assertEquals(".*AGENT", builder.getNextSegment(0, query));
        assertEquals("(.+)-\\d+", builder.getNextSegment(".*AGENT".length() + 1, query));
        assertEquals("CPU:(\\d).*", builder.getNextSegment(".*AGENT (.+)-\\d+".length() + 1, query));
        assertEquals("|", builder.getNextSegment(".*AGENT (.+)-\\d+ CPU:(\\d).*".length() + 1, query));
        assertEquals("groupBy(1)", builder.getNextSegment(".*AGENT (.+)-\\d+ CPU:(\\d).* |".length() + 1, query));
        assertEquals("average(2)", builder.getNextSegment(".*AGENT (.+)-\\d+ CPU:(\\d).* | groupBy(1)".length() + 1, query));

    }

    @Test
    public void testShouldStripApartSpaces() throws Exception {
        String[] spaceSplitArgs = builder.parseArgs("filterOne() countWithFilter(Pages,7,7, groupContains(7,html))");
        assertEquals(Arrays.toString(spaceSplitArgs), 2, spaceSplitArgs.length);
        assertEquals("Arg was[" + spaceSplitArgs[0] + "]", "filterOne()", spaceSplitArgs[0]);
        assertEquals("Arg was[" + spaceSplitArgs[1] + "]", "countWithFilter(Pages,7,7, groupContains(7,html))", spaceSplitArgs[1]);
    }

    @Test
    public void testShouldGetAggfunctionCTOR() throws Exception {
        Object functionInstance = builder.getFunctionInstance(Count.class, "tag", "1", "1", null, null, null, null);
        assertNotNull(functionInstance);
        assertTrue(functionInstance instanceof Count);

    }

    @Test
    public void testShouldGetAggfunctionCTORWithNumber() throws Exception {
        Object functionInstance = builder.getFunctionInstance(LessThan.class, "tag", "1", "1", null, null, new Integer(100), null);
        assertNotNull(functionInstance);
        assertTrue(functionInstance instanceof LessThan);

    }

    @Test
    public void testShouldExtractParamPartsWithNestedParams() throws Exception {
        String param = "1.average(1.function(2))";
        List<String> paramParts = builder.getParamParts(param);
        assertEquals(paramParts.toString(), 3, paramParts.size());
        assertEquals("1", paramParts.get(0));
        assertEquals("average", paramParts.get(1));
        assertEquals("1.function(2)", paramParts.get(2).trim());

    }

    @Test
    public void testShouldExtractParamPartsWithNestedBrackets() throws Exception {
        String param = "2.average(1.function(),2.function()) sutff";
        List<String> paramParts = builder.getParamParts(param);
        assertEquals(paramParts.toString(), 4, paramParts.size());
        assertEquals("2", paramParts.get(0));
        assertEquals("average", paramParts.get(1));
        assertEquals("1.function()", paramParts.get(2).trim());
        assertEquals("2.function()", paramParts.get(3).trim());

    }

    @Test
    public void testShouldExtractParamWithNestedBrackets() throws Exception {
        String param = "package.equals((proxy.ProxyClient))";
        List<String> paramParts = builder.getParamParts(param);
        assertEquals(3, paramParts.size());
        assertEquals("package", paramParts.get(0));
        assertEquals("equals", paramParts.get(1));
        assertEquals("(proxy.ProxyClient)", paramParts.get(2));
    }

    @Test
    public void testShouldExtractParamParts() throws Exception {
        String param = "2.average()";
        List<String> paramParts = builder.getParamParts(param);
        assertEquals(2, paramParts.size());
        assertEquals("2", paramParts.get(0));
        assertEquals("average", paramParts.get(1));
    }

    @Test
    public void testShouldExtractParamPartsAgain() throws Exception {
        String param = "1.average()";
        List<String> paramParts = builder.getParamParts(param);
        assertEquals(2, paramParts.size());
        assertEquals("1", paramParts.get(0));
        assertEquals("average", paramParts.get(1));

    }

    @Test
    public void testShouldGetGroupBy() throws Exception {

        int queryPart = builder.getQueryPart("groupBy", "this is a query | groupBy(100) iDontExist(100) average(1) sum(100) max(2)");
        assertEquals(100, queryPart);
    }

    @Test
    public void testShouldMatchRegExp() throws Exception {

        String lineToMatch = "2008-10-16 18:44:49,774 INFO Pool[20]LogSpace:ORM:10000-71--72-6 (Schedule.java:102)	 - REPORT_SCHEDULE Schedule[STUFF] TRIGGERED[2]Threshold[1]";
        boolean matches = lineToMatch.matches(".*REPORT_SCHEDULE.*STUFF.*TRIGGERED.*");
        assertTrue(matches);
    }

    @Test
    public void testShouldParseRangeIncludes() throws Exception {
        LogRequest replayRequest = builder.getLogRequest("subscriber", Arrays.asList(".* | data.rangeIncludes(100, 200)"), "", 1, 10);
        List<Filter> filters = replayRequest.query(0).filters();
        assertEquals(filters.toString(), 1, filters.size());
        assertEquals("rI-0", filters.get(0).getTag());
    }

    @Test
    public void testShouldParseRangeExcludes() throws Exception {
        LogRequest replayRequest = builder.getLogRequest("subscriber", Arrays.asList(".* | data.rangeExcludes(100, 200)"), "", 1, 10);
        List<Filter> filters = replayRequest.query(0).filters();
        assertEquals("The filter was not found", 1, filters.size());
        assertEquals("rE-0", filters.get(0).getTag());
    }

    @Test
    public void testShouldDoBrackets() throws Exception {
        LogRequest replayRequest = builder.getLogRequest("subscriber", Arrays.asList("type='log4j' | package.equals()"), "", 1, 10);
        List<Filter> filters = replayRequest.query(0).filters();
        assertEquals(1, filters.size());
        Equals filter = (Equals) filters.get(0);
        assertEquals("package", filter.group);

    }

    @Test
    public void testShouldDoEquals() throws Exception {
        LogRequest replayRequest = builder.getLogRequest("subscriber", Arrays.asList(".* | data.equals(stuff) data.equals()"), "", 1, 10);
        List<Filter> filters = replayRequest.query(0).filters();
        assertEquals(2, filters.size());
        assertEquals("e-0", filters.get(0).getTag());
        assertEquals("e-1", filters.get(1).getTag());
    }

    @Test
    public void testShouldParseNotAndContains() throws Exception {
        LogRequest replayRequest = builder.getLogRequest("subscriber", Arrays.asList(".* | data.not(foo) data.contains(blah)"), "", 1, 10);
        List<Filter> filters = replayRequest.query(0).filters();
        assertEquals(2, filters.size());
        assertEquals("not-0", filters.get(0).getTag());
        assertEquals("c-1", filters.get(1).getTag());
    }

    @Test
    public void testShouldParseNotAndContainsTheOtherWay() throws Exception {
        LogRequest replayRequest = builder.getLogRequest("subscriber", Arrays.asList(".* | data.contains(foo) data.not(blah)"), "", 1, 10);
        List<Filter> filters = replayRequest.query(0).filters();
        assertEquals("Got Filters:" + filters, 2, filters.size());
        assertEquals("c-0", filters.get(0).getTag());
        assertEquals("not-1", filters.get(1).getTag());
    }

    @Test
    public void testShouldDoItProper() throws Exception {
        String one = ".*AGENT (.+)-\\d+ MEM.+AVAIL:(\\d+).* | memUsed.average(one, host1) chart(clustered) buckets(10) cut(AVAILABLE MEMORY {2} MB) verbose(true) not(do stuff)";
        String two = ".*AGENT (.+)-\\d+-\\d+ MEM.+USED:(\\d+).* | memUsed.avg(two, host2)\"";
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(one, two), "", 1, 10);
        assertEquals(2, request.queryCount());
        Query query = request.query(0);
        assertTrue("unexpected, \ngot:" + query.pattern() + " \nnot:" + one, one.startsWith(query.pattern()));
        assertEquals(one, query.sourceQuery());
        assertEquals(1, query.functions().size());
        Function function = query.functions().get(0);
        assertEquals("host1", function.getTag());
        assertTrue(query.isGroupBy());
        System.out.println("Query:" + query.toString());

        Query query2 = request.query(1);
        assertTrue("Got:" + query2.pattern(), two.startsWith(query2.pattern()));
        assertEquals(two, query2.sourceQuery());
        assertEquals(1, query2.functions().size());
        Function function2 = query2.functions().get(0);
        assertEquals("host2", function2.getTag());
        assertTrue(query2.isGroupBy());

        assertEquals("do stuff", ((Not) query.filters().get(0)).getNot()[0]);
    }

    @Test
    public void testShouldDoItProperAndVerifyCopy() throws Exception {
        String one = ".*AGENT (.+)-\\d+ MEM.+AVAIL:(\\d+).* | mem.average(one) chart(clustered) buckets(10) mem.cut(AVAILABLE MEMORY {2} MB) verbose(true)";
        String two = ".*AGENT (.+)-\\d+-\\d+ MEM.+USED:(\\d+).* | mem.avg(two)\"";
        LogRequest requestMaster = builder.getLogRequest("sub", Arrays.asList(one, two), "", 1, 10);
        LogRequest request = requestMaster.copy(1, 10);
        assertEquals(2, request.queryCount());
        Query query = request.query(0);
        assertTrue(one.startsWith(query.pattern()));
        assertEquals(one, query.sourceQuery());
        assertEquals(1, query.functions().size());
        Function function = query.functions().get(0);
        assertEquals("mem", function.getTag());
        assertTrue(query.isGroupBy());
        System.out.println("Query:" + query.toString());

        Query query2 = request.query(1);
        assertTrue(two.startsWith(query2.pattern()));
        assertEquals(two, query2.sourceQuery());
        assertEquals(1, query2.functions().size());
        Function function2 = query2.functions().get(0);
        assertEquals("mem", function2.getTag());
        assertTrue(query2.isGroupBy());
    }


    @Test
    public void testShouldApplyNotFunction() throws Exception {

        LogRequest request = new LogRequest();
        Query query = new Query(0, 0, "foo", "foo", true);
        request.addQuery(query);
        query.addFilter(RequestParamAppliers.applyNotFunction("group", Arrays.asList("0","not", "this"), 1));

        assertEquals(1, query.filters().size());
        assertEquals("not-1", ((Not) query.filters().get(0)).getTag());
        assertEquals("this", ((Not) query.filters().get(0)).getNot()[0]);
    }

    @Test
    public void testShouldApplyNotFunctionWithTag() throws Exception {

        LogRequest request = new LogRequest();
        Query query = new Query(0, 0, "foo", "foo", true);
        request.addQuery(query);

        query.addFilter(RequestParamAppliers.applyNotFunction("group", Arrays.asList("0","not", "this"), 1));

        assertEquals(1, query.filters().size());
        assertEquals("not-1", ((Not) query.filters().get(0)).getTag());
        assertEquals("this", ((Not) query.filters().get(0)).getNot()[0]);
    }

    @Test
    public void testShouldApplyMultiNotFunctionWithTag() throws Exception {

        LogRequest request = new LogRequest();
        Query query = new Query(0, 0, "foo", "foo", true);
        request.addQuery(query);

        query.addFilter(RequestParamAppliers.applyNotFunction("group", Arrays.asList("0", "not", "stuff 1", "stuff 2"), 1));

        assertEquals(1, query.filters().size());
        assertEquals("not-1", ((Not) query.filters().get(0)).getTag());
        assertEquals("stuff 1", ((Not) query.filters().get(0)).getNot()[0]);
        assertEquals("stuff 2", ((Not) query.filters().get(0)).getNot()[1]);
    }

    @Test
    public void testShouldApplyMultipleNotFunctions() throws Exception {

        LogRequest request = new LogRequest();
        Query query = new Query(0, 0, "foo", "foo", true);
        request.addQuery(query);

        query.addFilter(RequestParamAppliers.applyNotFunction("group", Arrays.asList("0", "not", "this1"), 1));
        query.addFilter(RequestParamAppliers.applyNotFunction("group", Arrays.asList("0", "not", "this2"), 2));

        assertEquals(2, query.filters().size());
        String stuff1 = ((Not) query.filters().get(0)).getTag();
        String stuff2 = ((Not) query.filters().get(1)).getTag();

        assertTrue((stuff1 + stuff2).contains("not-1"));
        assertTrue((stuff1 + stuff2).contains("not-2"));
    }

    @Test
    public void testShouldDoContains() throws Exception {
        LogRequest request = new LogRequest();
        Query query = new Query(0, 0, "foo", "foo", true);
        request.addQuery(query);

        query.addFilter(RequestParamAppliers.applyContainsFunction("group", Arrays.asList("0", "contains", "stuff"), 1));
        assertEquals(1, query.filters().size());
    }

    @Test
    public void testShouldProperlyConstructSimpleQueryWithNOTOperators() throws Exception {
        String logFilter = "AGENT AND CPU OR AGENT AND MEM | data.not(LOGGER,LOGSEARCH)";
        LogRequest result = builder.getLogRequest("sub", Arrays.asList(logFilter), "", 0L, 0L);
        assertEquals(2, result.queryCount());
        assertEquals("AGENT AND CPU", result.query(0).pattern());
        assertEquals("AGENT AND MEM", result.query(1).pattern());
    }

    @Test
    public void testShouldProperlyConstructRegExp() throws Exception {
        String logFilter = ".*AGENT (.+)-\\d+ CPU:(\\d+).* | average(2,1)";
        LogRequest result = builder.getLogRequest("sub", Arrays.asList(logFilter), "", 0, 0);
        assertEquals(1, result.queryCount());
        assertEquals(".*AGENT (.+)-\\d+ CPU:(\\d+).*", result.query(0).pattern());
    }

    @Test
    public void testShouldApplyRegExpProperly() throws Exception {
        String log = "2008-10-17 20:39:57,987 INFO pool-1-thread-2 (Resource.java:170)	 - AGENT alteredcarbon.local-12015 CPU:9.000000";
        String regex = ".*AGENT (.+)-\\d+ CPU:(\\d+\\.\\d+)*";
        MatchResult matcher = RegExpUtil.matches(regex, log);
        System.err.println("Groups:" + matcher.groups());
        System.err.println("Group1:" + matcher.group(1));
        System.err.println("Group2:" + matcher.group(2));
        System.err.println("Group2:" + matcher.group(2));

    }

    @Test
    public void testShouldCleanQuery() throws Exception {
        assertEquals("a stuff", builder.removeTailEndFromPipe("a stuff"));
        assertEquals("a stuff", builder.removeTailEndFromPipe("a stuff | tail end bit"));
    }

    @Test
    public void testShouldFindQueryPart() throws Exception {
        assertEquals(1, builder.getQueryPart("groupBy", "this is a query | groupBy(1)"));
        assertEquals(0, builder.getQueryPart("groupBy", "this is a query | groupBy(0)"));

    }
    
    @Test
	public void shouldDoElapsedWithGroupForLabelGroup() throws Exception {
    	String query = "type='log4j' Profiling: (**) (started OR Ended) | msg.elapsed(tag, started, Ended, ms, 1)";
        DateTime to = new DateTime();
        DateTime from = to.minusHours(1);
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(query), null, from.getMillis(), to.getMillis());
        List<Function> functions = request.query(0).functions();
        assertEquals(1, functions.size());
        Function function = functions.get(0);
        assertEquals(Elapsed.class, function.getClass());
        Elapsed elapsed = (Elapsed) function;
        String labelGroup = elapsed.getLabelGroup();
        assertEquals("1", labelGroup);
        
	}

    @Test
    public void shouldAddElapsedFunctionWithField() throws Exception {
        String q1 = "(.*) | data.elapsed(foo, start, end)";
        DateTime to = new DateTime();
        DateTime from = to.minusHours(1);
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(q1), null, from.getMillis(), to.getMillis());
        List<Function> functions = request.query(0).functions();
        assertEquals(1, functions.size());
        Function function = functions.get(0);
        assertEquals(Elapsed.class, function.getClass());
    }

    @Test
    public void shouldAddElapsedFunctionWithFieldAndSeconds() throws Exception {
        String q1 = "(.*) | data.elapsed(foo, start, end, s)";
        DateTime to = new DateTime();
        DateTime from = to.minusHours(1);
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(q1), null, from.getMillis(), to.getMillis());
        List<Function> functions = request.query(0).functions();
        assertEquals(1, functions.size());
        Function function = functions.get(0);
        assertEquals(Elapsed.class, function.getClass());
        Elapsed f = (Elapsed) function;
        System.out.println("f:" + f);
        
    }

    @Test
    public void shouldAddElapsedFunctionWithLineChart() throws Exception {
        String q1 = "(.*) | data.elapsed(foo, start, end) chart(line)";
        DateTime to = new DateTime();
        DateTime from = to.minusHours(1);
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(q1), null, from.getMillis(), to.getMillis());
        List<Function> functions = request.query(0).functions();
        assertEquals(1, functions.size());
        Elapsed function = (Elapsed) functions.get(0);
        assertThat(function.isLine(), is(true));
    }

    @Test
    public void shouldAddElapsedFunctionWithGroup() throws Exception {
        String q1 = "(.*) | elapsed(foo, 1, start, end)";
        DateTime to = new DateTime();
        DateTime from = to.minusHours(1);
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(q1), null, from.getMillis(), to.getMillis());
        List<Function> functions = request.query(0).functions();
        assertEquals(1, functions.size());
        assertEquals(Elapsed.class, functions.get(0).getClass());
    }

    @Test
    public void shouldAddElapsedFunctionWithGroupAndSeconds() throws Exception {
        String q1 = "(.*) | 1.elapsed(foo, start, end, s)";
        DateTime to = new DateTime();
        DateTime from = to.minusHours(1);
        LogRequest request = builder.getLogRequest("sub", Arrays.asList(q1), null, from.getMillis(), to.getMillis());
        List<Function> functions = request.query(0).functions();
        assertEquals(1, functions.size());
        assertEquals(Elapsed.class, functions.get(0).getClass());
    }

    /**
     * No longer sure about what these tests should do.
     */
  //  @Test
    public void shouldReplaceOrWithGroup() {
        final LogRequest request = builder.getLogRequest("s", Arrays.asList("cpu OR host | _filename.count()"), null, from.getMillis(), to.getMillis());
        final Query query = request.query(0);
        final Query query2 = request.query(1);
        assertThat(query.pattern(), is("(cpu)"));
        assertThat(query2.pattern(), is("(host)"));
    }

   // @Test
    public void shouldReplaceCountWIthCount1WhenOr() {
        final LogRequest request = builder.getLogRequest("s", Arrays.asList("cpu OR host | _filename.count()"), null, from.getMillis(), to.getMillis());
        final Query query = request.query(0);
        final Query query2 = request.query(1);
        assertThat(query.sourceQuery(), is("(cpu) | _filename.count(1)"));
        assertThat(query2.sourceQuery(), is("(host) | _filename.count(1)"));
    }

   // @Test
    public void shouldDoThisOrProperly() {
        final LogRequest request = builder.getLogRequest("s", Arrays.asList("(cpu OR host) | _filename.count()"), null, from.getMillis(), to.getMillis());
        final Query query = request.query(0);
        final Query query2 = request.query(1);
        assertThat(query.sourceQuery(), is("(cpu) | _filename.count(1)"));
        assertThat(query2.sourceQuery(), is("(host) | _filename.count(1)"));
    }

    //@Test
    public void shouldReplaceThisOrProperly() {
        final LogRequest request = builder.getLogRequest("s", Arrays.asList("(cpu|host) | _filename.count()"), null, from.getMillis(), to.getMillis());
        final Query query = request.query(0);
        assertThat(query.sourceQuery(), is("(cpu|host) | _filename.count(1)"));
    }

    //@Test
    public void shouldDoSensibleStuffWithThis() {
        final LogRequest request = builder.getLogRequest("s", Arrays.asList("cpu | _filename.count()", "host | _filename.count()"), null, from.getMillis(), to.getMillis());
        final Query query = request.query(0);
        final Query query2 = request.query(1);
        assertThat(query.sourceQuery(), is("(cpu) | _filename.count(1)"));
        assertThat(query2.sourceQuery(), is("(host) | _filename.count(1)"));
    }


    @Test
    public void shouldDoThis() {
        final LogRequest x = builder.getLogRequest("x", Arrays.asList("(Bar|Foo) something OR (cluster) | 1.count()"), null, from.getMillis(), to.getMillis());
        assertThat(x.getErrors().toString(), x.getErrors().size(), is(0));
    }


}
