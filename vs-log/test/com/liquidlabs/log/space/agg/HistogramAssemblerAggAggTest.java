package com.liquidlabs.log.space.agg;

import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.filters.*;
import com.liquidlabs.log.search.functions.*;
import com.liquidlabs.log.space.LogRequest;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.*;

/**
 * Agg count() -> Agg max()
 *
 * Use cases
 * 1. Visitors per min
 *         _date+_hour+_minute.count(, POST)  +POST.max() +POST.avg() +POST.min()
 *
 * 2. Daily Average total trades booked per day - i.e. for each day sum all trades booked => Avg Trades booked per day
 *          tradesBooked.sum(_date,"booked") +booked.avg()
 *
 */
public class HistogramAssemblerAggAggTest {

    public static final String SPLIT = LogProperties.getFunctionSplit();
    private LogRequest request;
    private ClientHistoAssembler assembler;
    long time = 1000;
    long fileStartTime = 1000;
    long fileEndTime = 1000;
    private Map<String, Object> sharedFunctionData;
    private List<Filter> filters;
    private int lineNumber;
    private Map<Integer, Set<String>> functionFilters = new HashMap<Integer,Set<String>>();
    private String filenameOnly;
    FieldSet fieldSet = FieldSets.get();
    private String lineData;
    private MatchResult matchResult;
    private boolean isSummary;
    private String subscriber = "subscriber";
    ConcurrentHashMap<String, LogRequest> runningRequests = new ConcurrentHashMap<String, LogRequest>();
    private Set<FieldSet> fieldSets;
    private long requestStartTimeMs;
    private long requestEndMs;
    private long timeMs;


    @Before
    public void setUp() throws Exception {
        assembler = new ClientHistoAssembler();
        runningRequests.put(subscriber, new LogRequest());

        sharedFunctionData = new HashMap<String,Object>();
    }

    @Test
    public void sortListOfPostAggsToBeLast() throws Exception {
        ArrayList<Function> functions = new ArrayList<Function>();
        functions.add(new Max("beLast", "", "+hitCount"));
        functions.add(new Count("hitCount", "", "hits"));

        List<Function> sorted = FunctionBase.sortPostAggFunctions(functions);
        assertTrue(sorted.get(0).toStringId().contains("hits"));
        assertTrue(sorted.get(1).toStringId().contains("beLast"));
    }
    @Test
    public void shouldFindPostAggFunction() throws Exception {
        ArrayList<Function> functions = new ArrayList<Function>();
        functions.add(new Max("beLast", "", "+hitCount"));
        functions.add(new Count("hitCount", "", "hits"));

        assertTrue(FunctionBase.isPostAggFunction(functions.get(0), functions));
        assertFalse(FunctionBase.isPostAggFunction(functions.get(1), functions));
    }


    @Test
    public void testShouldPutGroupCountIntoHistogram() throws Exception {
        long fromTime = 1000000;
        long toTime = 2000000;

        List<ClientHistoItem> histogram = assembler.newClientHistogram(new DateTime(fromTime), new DateTime(toTime), 10);

        FieldSet fieldSet = new FieldSet("(w) (.*)", "part0", "hits");

        ArrayList<Function> functions = new ArrayList<Function>();
        functions.add(new Count("hitCount", "", "hits"));


        Bucket bucket = new Bucket(fromTime, fromTime + 10, functions, 0, "pattern", "hp", "sub", "");
        bucket.increment(fieldSet, fieldSet.getFields("hit aaa.168.10.24", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hit bbb.168.10.24", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hit bbb.168.10.24", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hit ccc.168.10.24", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hit ccc.168.10.24", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hit ccc.168.10.24", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hit ccc.168.10.24", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);




        ArrayList<Function> aggFunctions = new ArrayList<Function>();
        aggFunctions.add(new Count("hitCount", "", "hits"));
        aggFunctions.add(new Max("hitMax", "", "+hitCount"));

        assembler.pushIntoClientHistoItem(histogram.get(0), bucket, 10, aggFunctions, true, 0, 0, sharedFunctionData,histogram.size(), filters, functionFilters, subscriber, fieldSets, requestStartTimeMs, request);

        System.out.println("RESULTS:" + histogram);
        assertNotNull("Keys:" + histogram.get(0).keys(), histogram.get(0).getSeriesValue("hitMax!hits"));

        assertEquals(4.0, histogram.get(0).getSeriesValue("hitMax!hits").value, 0);
    }
    @Test
    public void testShouldPutGroupMinIntoHistogram() throws Exception {
        long fromTime = 1000000;
        long toTime = 2000000;

        List<ClientHistoItem> histogram = assembler.newClientHistogram(new DateTime(fromTime), new DateTime(toTime), 10);

        FieldSet fieldSet = new FieldSet("(w) (.*)", "part0", "hits");

        ArrayList<Function> functions = new ArrayList<Function>();
        functions.add(new Count("hitCount", "", "hits"));


        Bucket bucket = new Bucket(fromTime, fromTime + 10, functions, 0, "pattern", "hp", "sub", "");
        bucket.increment(fieldSet, fieldSet.getFields("hit aaa.168.10.24", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hit aaa.168.10.24", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hit bbb.168.10.24", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hit bbb.168.10.24", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hit ccc.168.10.24", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hit ccc.168.10.24", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hit ccc.168.10.24", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hit ccc.168.10.24", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);




        ArrayList<Function> aggFunctions = new ArrayList<Function>();
        aggFunctions.add(new Count("hitCount", "", "hits"));
        aggFunctions.add(new Min("hitMin", "", "+hitCount"));

        assembler.pushIntoClientHistoItem(histogram.get(0), bucket, 10, aggFunctions, true, 0, 0, sharedFunctionData, histogram.size(), filters, functionFilters, subscriber, fieldSets, requestStartTimeMs, request);

        System.out.println("RESULTS:" + histogram);
        String seriesKeys = histogram.get(0).keys().toString();
        assertTrue("PORT-Agg-Failed Didnt find hitMin Post Agg:" + seriesKeys, seriesKeys.contains("hitMin"));
        assertNotNull("Keys:" + histogram.get(0).keys(), histogram.get(0).getSeriesValue("hitMin!hits"));

        assertEquals(2.0, histogram.get(0).getSeriesValue("hitMin!hits").value, 0);
    }
}
