package com.liquidlabs.log.space.agg;

import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.common.regex.RegExpUtil;
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

public class HistogramAssemblerAggTest {

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
    public void shouldPutCountSingleDeltaCorrectly() throws Exception {
        long fromTime = 1000000;
        long toTime = 2000000;

        List<ClientHistoItem> histogram = assembler.newClientHistogram(new DateTime(fromTime), new DateTime(toTime), 10);

        String thePattern = "(\\w+)";


        ArrayList<Function> functions1 = new ArrayList<Function>();
        functions1.add(new CountSingleDelta("count", FieldSets.fieldName, FieldSets.fieldName));
        Bucket bucketONE = new Bucket(histogram.get(0).timeMs, histogram.get(0).timeMs + 10, functions1, 0, thePattern, "hp", "sub", "");
        bucketONE.increment(fieldSet, fieldSet.getFields("groupA", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucketONE.increment(fieldSet, fieldSet.getFields("groupB", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);

        ArrayList<Function> functions2 = new ArrayList<Function>();
        functions2.add(new CountDelta("count", FieldSets.fieldName, FieldSets.fieldName));
        Bucket bucketTWO = new Bucket(histogram.get(1).timeMs, histogram.get(1).timeMs + 10, functions2, 0, thePattern, "hp", "sub", "");
        bucketTWO.increment(fieldSet, fieldSet.getFields("groupB", -1, -1, timeMs), filenameOnly, "one", histogram.get(1).timeMs, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);


        ArrayList<Function> aggFunctionForBucket = new ArrayList<Function>();
        aggFunctionForBucket.add(new CountSingleDelta("count", FieldSets.fieldName, FieldSets.fieldName));
        int pos = 0;
        assembler.pushIntoClientHistoItem(histogram.get(0), bucketONE, 10, aggFunctionForBucket, true, 0, pos++, sharedFunctionData, histogram.size(), filters, functionFilters, subscriber,fieldSets, requestStartTimeMs, request);
        assembler.pushIntoClientHistoItem(histogram.get(1), bucketTWO, 10, aggFunctionForBucket, true, 0, pos++, sharedFunctionData, histogram.size(), filters, functionFilters, subscriber,fieldSets, requestStartTimeMs, request);

        for (Function func : aggFunctionForBucket) {
            func.flushAggResultsForBucket(bucketONE.getQueryPos(), sharedFunctionData);
        }


        String group = "count!groupA" + SPLIT + "groupA";
        assertNotNull("Keys:" + histogram.get(1).keys(), histogram.get(1).getSeriesValue(group));
        assertEquals(-1.0, histogram.get(1).getSeriesValue(group).value, 0);

        assertTrue("Keys:" + histogram.get(1).keys(), histogram.get(1).getSeriesValue("count!groupB_groupB") == null);
    }

    @Test
    public void shouldPutNonGroupNumberAndApplyGTFilter() throws Exception {
        long fromTime = 1000000;
        long toTime = 2000000;

        List<ClientHistoItem> histogram = assembler.newClientHistogram(new DateTime(fromTime), new DateTime(toTime), 10);

        String thePatternONE = ".*aaa.*";

        ArrayList<Function> functions = new ArrayList<Function>();

        Bucket bucket = new Bucket(fromTime, fromTime +10, functions, 0, thePatternONE, "hp", "sub", "");
        bucket.increment(fieldSet, fieldSet.getFields("hello aaa-foo", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hello aaa-foo", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hello aaa-foo", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.convertFuncResults(true);

        // TEST GT - filter > 2
        filters = new ArrayList<Filter>();
        filters.add(new Contains("crap", Arrays.asList("aaa")));
        filters.add(new GreaterThan("GT", "part2", 4));

        assembler.putIntoHistogram(histogram.get(0), bucket, new DateTime(time), new DateTime(time + 10), histogram.size(), filters);


        assertEquals("Should have filtered for 4 items:" + histogram.get(0).keys().toString(), 0, histogram.get(0).series.size());

        filters.clear();
        filters.add(new Contains("crap", Arrays.asList("XXX")));
        filters.add(new Not("crap", "part2", "XXX"));
        filters.add(new GreaterThan("GT", "part2", 2));
        assembler.putIntoHistogram(histogram.get(0), bucket, new DateTime(time), new DateTime(time + 10), histogram.size(), filters);
        assertEquals("Should have filtered for 3 items and got 1:" + histogram.get(0).keys().toString(), 1, histogram.get(0).series.size());

    }

    @Test
    public void shouldPutAvgDeltaCorrectly() throws Exception {
        long fromTime = 1000000;
        long toTime = 2000000;

        List<ClientHistoItem> histogram = assembler.newClientHistogram(new DateTime(fromTime), new DateTime(toTime), 10);

        FieldSet fieldSet = new FieldSet("(w) (w)", "key", "value");


        ArrayList<Function> functions1 = new ArrayList<Function>();
        AverageDelta function = new AverageDelta("avg", "key", "value");
        functions1.add(function.create());
        Bucket bucketONE = new Bucket(histogram.get(0).timeMs, histogram.get(0).timeMs + 10, functions1, 0, fieldSet.expression, "hp", "sub", "");
        bucketONE.increment(fieldSet, fieldSet.getFields("k1 50", -1, -1, timeMs), filenameOnly, "filename", histogram.get(0).timeMs, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucketONE.increment(fieldSet, fieldSet.getFields("k2 100", -1, -1, timeMs), filenameOnly, "filename", histogram.get(0).timeMs, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);

        ArrayList<Function> functions2 = new ArrayList<Function>();
        functions2.add(function.create());
        Bucket bucketTWO = new Bucket(histogram.get(1).timeMs, histogram.get(1).timeMs + 10, functions2, 0, fieldSet.expression, "hp", "sub", "");
        bucketTWO.increment(fieldSet, fieldSet.getFields("k1 150", -1, -1, timeMs), filenameOnly, "filename", histogram.get(1).timeMs, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucketTWO.increment(fieldSet, fieldSet.getFields("k2 50", -1, -1, timeMs), filenameOnly, "filename", histogram.get(1).timeMs, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);


        ArrayList<Function> aggFunctionForBucket = new ArrayList<Function>();
        aggFunctionForBucket.add(function.create());
        int pos = 0;
        assembler.pushIntoClientHistoItem(histogram.get(0), bucketONE, 10, aggFunctionForBucket, true, 0, pos++, sharedFunctionData, histogram.size(), filters, functionFilters, subscriber, fieldSets, requestStartTimeMs, request);
        assembler.pushIntoClientHistoItem(histogram.get(0), bucketTWO, 10, aggFunctionForBucket, true, 0, pos++, sharedFunctionData, histogram.size(), filters, functionFilters, subscriber, fieldSets, requestStartTimeMs, request);

        for (Function func : aggFunctionForBucket) {
            func.flushAggResultsForBucket(bucketONE.getQueryPos(), sharedFunctionData);
        }


        assertNotNull("Keys:" + histogram.get(0).keys(), histogram.get(0).getSeriesValue("avg!k1"));
        assertEquals(100.0, histogram.get(0).getSeriesValue("avg!k1").value, 0);

        assertNotNull("Keys:" + histogram.get(0).keys(), histogram.get(0).getSeriesValue("avg!k2"));
        assertEquals(-50.0, histogram.get(0).getSeriesValue("avg!k2").value, 0);
    }

    @Test
    public void shouldPutCountDeltaCorrectly() throws Exception {
        long fromTime = 1000000;
        long toTime = 2000000;

        List<ClientHistoItem> histogram = assembler.newClientHistogram(new DateTime(fromTime), new DateTime(toTime), 10);

        String thePattern = "(\\w+)";

//		MatchResult matchONE = RegExpUtil.matches(thePattern, "groupA");
        MatchResult matchTWO = RegExpUtil.matches(thePattern, "groupB");

        ArrayList<Function> functions1 = new ArrayList<Function>();
        functions1.add(new CountDelta("count", FieldSets.fieldName, FieldSets.fieldName));
        Bucket bucketONE = new Bucket(histogram.get(0).timeMs, histogram.get(0).timeMs + 10, functions1, 0, thePattern, "hp", "sub", "");
        bucketONE.increment(fieldSet, fieldSet.getFields("groupA", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucketONE.increment(fieldSet, fieldSet.getFields("groupA", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucketONE.increment(fieldSet, fieldSet.getFields("groupA", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);

        ArrayList<Function> functions2 = new ArrayList<Function>();
        functions2.add(new CountDelta("count", FieldSets.fieldName, FieldSets.fieldName));
        Bucket bucketTWO = new Bucket(histogram.get(1).timeMs, histogram.get(1).timeMs + 10, functions2, 0, thePattern, "hp", "sub", "");
        bucketTWO.increment(fieldSet, fieldSet.getFields("groupB", -1, -1, timeMs), filenameOnly, "one", histogram.get(1).timeMs, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucketTWO.increment(fieldSet, fieldSet.getFields("groupB", -1, -1, timeMs), filenameOnly, "one", histogram.get(1).timeMs, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucketTWO.increment(fieldSet, fieldSet.getFields("groupB", -1, -1, timeMs), filenameOnly, "one", histogram.get(1).timeMs, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);


        ArrayList<Function> aggFunctionForBucket = new ArrayList<Function>();
        aggFunctionForBucket.add(new CountDelta("count", FieldSets.fieldName, FieldSets.fieldName));
        int pos = 0;
        assembler.pushIntoClientHistoItem(histogram.get(0), bucketONE, 10, aggFunctionForBucket, true, 0, pos++, sharedFunctionData, histogram.size(), filters, functionFilters, subscriber, fieldSets, requestStartTimeMs, request);
        assembler.pushIntoClientHistoItem(histogram.get(1), bucketTWO, 10, aggFunctionForBucket, true, 0, pos++, sharedFunctionData, histogram.size(), filters, functionFilters, subscriber, fieldSets, requestStartTimeMs, request);

        for (Function func : aggFunctionForBucket) {
            func.flushAggResultsForBucket(bucketONE.getQueryPos(), sharedFunctionData);
        }

        assertNotNull("Keys:" + histogram.get(1).keys(), histogram.get(1).getSeriesValue("count!groupA"));
        assertEquals(-3.0, histogram.get(1).getSeriesValue("count!groupA").value, 0);

        assertNotNull("Keys:" + histogram.get(1).keys(), histogram.get(1).getSeriesValue("count!groupB"));
        assertEquals(3, histogram.get(1).getSeriesValue("count!groupB").value, 0);
    }


    @Test
    public void shouldPutCountUniqueCorrectly() throws Exception {
        long fromTime = 1000000;
        long toTime = 2000000;

        List<ClientHistoItem> histogram = assembler.newClientHistogram(new DateTime(fromTime), new DateTime(toTime), 10);

        String thePattern = "(\\w+)";
        ArrayList<Function> functions = new ArrayList<Function>();
        functions.add(new CountUnique("count", FieldSets.fieldName, FieldSets.fieldName));

        Bucket bucket = new Bucket(fromTime, fromTime + 10, functions, 0, thePattern, "hp", "sub", "");
        bucket.increment(fieldSet, fieldSet.getFields("groupA", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("groupA", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("groupB", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("groupB", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("groupB", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);


        ArrayList<Function> aggFunctionForBucket = new ArrayList<Function>();
        aggFunctionForBucket.add(new CountUnique("count", FieldSets.fieldName, FieldSets.fieldName));
        assembler.pushIntoClientHistoItem(histogram.get(0), bucket, 10, aggFunctionForBucket, true, 0, 0, sharedFunctionData, histogram.size(), filters, functionFilters, subscriber, fieldSets, requestStartTimeMs, request);

        final String key = "count";
        assertEquals(2.0, histogram.get(0).getSeriesValue(key).value, 2);
    }

    @Test
    public void testShouldPutGroupCountIntoHistogram() throws Exception {
        long fromTime = 1000000;
        long toTime = 2000000;

        List<ClientHistoItem> histogram = assembler.newClientHistogram(new DateTime(fromTime), new DateTime(toTime), 10);

        FieldSet fieldSet = new FieldSet("(w) (w)-(w)", "part0", "part1", "part2");

        ArrayList<Function> functions = new ArrayList<Function>();
        functions.add(new Count("count", "part1", "part2"));

        Bucket bucket = new Bucket(fromTime, fromTime + 10, functions, 0, "pattern", "hp", "sub", "");
        bucket.increment(fieldSet, fieldSet.getFields("hello foo-aaa", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);


        ArrayList<Function> functions2 = new ArrayList<Function>();
        functions2.add(new Count("count", "part1", "part2"));
        assembler.pushIntoClientHistoItem(histogram.get(0), bucket, 10, functions2, true, 0, 0, sharedFunctionData,histogram.size(), filters, functionFilters, subscriber, fieldSets, requestStartTimeMs, request);

        assertNotNull("Keys:" + histogram.get(0).keys(), histogram.get(0).getSeriesValue("count!foo" + SPLIT + "aaa"));
        assertEquals(1.0, histogram.get(0).getSeriesValue("count!foo" + SPLIT + "aaa").value, 0);
    }

    @Test
    public void testShouldPutGroupCountsIntoHistogram() throws Exception {
        long fromTime = 1000000;
        long toTime = 2000000;

        List<ClientHistoItem> histogram = assembler.newClientHistogram(new DateTime(fromTime), new DateTime(toTime), 10);

        FieldSet fieldSet = new FieldSet("(w) (w)-(w)", "part0", "part1", "part2");

        ArrayList<Function> functions = new ArrayList<Function>();
        functions.add(new Count("count", "part2", "part1"));

        Bucket bucket = new Bucket(fromTime, fromTime +10, functions, 0, "thePattern", "hp", "sub", "");
        bucket.increment(fieldSet, fieldSet.getFields("hello aaa-foo", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hello aaa-foo", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hello bbb-foo", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hello bbb-foo", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.convertFuncResults(true);

        ArrayList<Function> functions2 = new ArrayList<Function>();
        functions2.add(new Count("count", "part2", "part1"));
        assembler.pushIntoClientHistoItem(histogram.get(0), bucket, 10, functions2, true, 0, 0, sharedFunctionData,histogram.size(), filters, functionFilters, subscriber, fieldSets, requestStartTimeMs, request);

        // This may be wrong - i can't  see
        assertEquals(histogram.get(0).keys().toString(), 2.0, histogram.get(0).getSeriesValue("count!foo" + SPLIT + "aaa").value, 0);
        assertEquals(2.0, histogram.get(0).getSeriesValue("count!foo" + SPLIT + "bbb").value, 0);
    }
    @Test
    public void shouldAggCountWithGTFilter() throws Exception {
        long fromTime = 1000000;
        long toTime = 2000000;

        List<ClientHistoItem> histogram = assembler.newClientHistogram(new DateTime(fromTime), new DateTime(toTime), 10);

        FieldSet fieldSet = new FieldSet("(w) (w)-(w)", "part0", "part1", "part2");

        ArrayList<Function> functions = new ArrayList<Function>();
        functions.add(new Count("count", "part1", "part2"));

        Bucket bucket = new Bucket(fromTime, fromTime +10, functions, 0, "thePattern", "hp", "sub", "");
        bucket.increment(fieldSet, fieldSet.getFields("hello GRPhostAA-WARN", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hello GRPhostAA-WARN", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hello GRPhostAA-WARN", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hello GRPhostBB-WARN", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hello GRPhostBB-WARN", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.convertFuncResults(true);

        ArrayList<Function> functions2 = new ArrayList<Function>();
        functions2.add(new Count("count", "part1", "part2"));


        // TEST - filter > 2
        filters = new ArrayList<Filter>();
        filters.add(new GreaterThan("GT", "part2", 2));

        assembler.pushIntoClientHistoItem(histogram.get(0), bucket, 10, functions2, true, 0, 0, sharedFunctionData,histogram.size(), filters, functionFilters, subscriber, fieldSets, requestStartTimeMs, request);

        assertEquals(histogram.get(0).keys().toString(), 3.0, histogram.get(0).getSeriesValue("count!GRPhostAA" + SPLIT + "WARN").value, 0);
        assertTrue(histogram.get(0).getSeriesValue("count!GRPhostB!WARN") == null);
    }

    @Test
    public void testShouldPutGroupCountsWithRangeFilter() throws Exception {
        long fromTime = 1000000;
        long toTime = 2000000;

        List<ClientHistoItem> histogram = assembler.newClientHistogram(new DateTime(fromTime), new DateTime(toTime), 10);

        FieldSet fieldSet = new FieldSet("(w) (w)-(w)", "part0", "part1", "part2");


        ArrayList<Function> functions = new ArrayList<Function>();
        functions.add(new Count("count", "part1", "part2"));

        Bucket bucket = new Bucket(fromTime, fromTime +10, functions, 0, "thePattern", "hp", "sub", "");
        bucket.increment(fieldSet, fieldSet.getFields("hello aaa-foo", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hello aaa-foo", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hello aaa-foo", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hello bbb-foo", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hello bbb-foo", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.convertFuncResults(true);

        ArrayList<Function> functions2 = new ArrayList<Function>();
        functions2.add(new Count("count", "part1", "part2"));


        // TEST - filter > 2
        filters = new ArrayList<Filter>();
        filters.add(new RangeIncludes("rI", "part2", 3, 4));

        assembler.pushIntoClientHistoItem(histogram.get(0), bucket, 10, functions2, true, 0, 0, sharedFunctionData,histogram.size(), filters, functionFilters, subscriber, fieldSets, requestStartTimeMs, request);

        assertEquals(histogram.get(0).keys().toString(), 3.0, histogram.get(0).getSeriesValue("count!aaa" + SPLIT + "foo").value, 0);
        assertTrue(histogram.get(0).getSeriesValue("count!bbb" + SPLIT + "foo") == null);
    }

    @Test
    public void testShouldPutGroupAveragesIntoHistogramWhereGroupByNotProvided() throws Exception {
        long fromTime = 1000000;
        long toTime = 2000000;

        List<ClientHistoItem> histogram = assembler.newClientHistogram(new DateTime(fromTime), new DateTime(toTime), 10);

        ArrayList<Function> functions = new ArrayList<Function>();
        functions.add(new Average("RX_1", "", "rx1b"));

        fieldSet = new FieldSet("(*)\t(*\t)\t(*)\t(.*)\t(.*)\t(.*)", "batch", "time", "rx1a", "rx1b");

//		MatchResult matcherAAA = RegExpUtil.matches(thePattern, "2632	Thu Jun 25 11:09:06 BST 2009	1.0	1.0	1.0	1.0");
//		MatchResult matcherBBB = RegExpUtil.matches(thePattern, "2	Thu Jun 18 09:35:24 BST 2009	1.0	0.9999803983064137	1.0	0.9984619389034808");


        Bucket bucket = new Bucket(fromTime, fromTime +100, functions, 0, "thePattern", "hp", "sub", "");

        bucket.increment(fieldSet, fieldSet.getFields("2632	Thu Jun 25 11:09:06 BST 2009	1.0	1.0	1.0	1.0", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("2632	Thu Jun 25 11:09:06 BST 2009	1.0	1.0	1.0	1.0", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("2	Thu Jun 18 09:35:24 BST 2009	1.0	0.9999803983064137	1.0	0.9984619389034808", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs );
        bucket.increment(fieldSet, fieldSet.getFields("2	Thu Jun 18 09:35:24 BST 2009	1.0	0.9999803983064137	1.0	0.9984619389034808", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.convertFuncResults(true);
        ArrayList<Function> functions2 = new ArrayList<Function>();
        functions2.add(new Average("RX_1", "", "rx1b"));
        assembler.pushIntoClientHistoItem(histogram.get(0), bucket, 10, functions2, true, 0, 0, sharedFunctionData,histogram.size(), filters, functionFilters, subscriber, fieldSets, requestStartTimeMs, request);

        assertNotNull(histogram.get(0).getSeriesValue("RX_1"));
        double value = histogram.get(0).getSeriesValue("RX_1").value;
        assertTrue(value > 0.9);
        assertTrue("should be a bit less than 1", value < 1.0);
    }

    @Test
    public void testShouldPutGroupAveragesIntoHistogram() throws Exception {
        long fromTime = 1000000;
        long toTime = 2000000;

        List<ClientHistoItem> histogram = assembler.newClientHistogram(new DateTime(fromTime), new DateTime(toTime), 10);

        ArrayList<Function> functions = new ArrayList<Function>();
        functions.add(new Average("avg", "part1", "part2"));

        //String thePattern = "hello (\\w+)-(\\d+)";
        FieldSet fieldSet = new FieldSet("(w) (w)-(w)", "part0", "part1", "part2");
//		Pattern pattern = Pattern.compile(thePattern);

//		MatchResult matcherAAA = RegExpUtil.matches(thePattern, "hello aaa-10");
//		MatchResult matcherBBB = RegExpUtil.matches(thePattern, "hello bbb-10");


        Bucket bucket = new Bucket(fromTime, fromTime +100, functions, 0, "thePattern", "hp", "sub", "");

        bucket.increment(fieldSet, fieldSet.getFields("hello aaa-10", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hello aaa-10", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hello bbb-10", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs );
        bucket.increment(fieldSet, fieldSet.getFields("hello bbb-10", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.convertFuncResults(true);
        ArrayList<Function> functions2 = new ArrayList<Function>();
        functions2.add(new Average("avg", "part1", "part2"));
        assembler.pushIntoClientHistoItem(histogram.get(0), bucket, 10, functions2, true, 0, 0, sharedFunctionData,histogram.size(), filters, functionFilters, subscriber, fieldSets, requestStartTimeMs, request);

        assertEquals(10.0, histogram.get(0).getSeriesValue("avg!aaa").value, 0);
        assertEquals(10.0, histogram.get(0).getSeriesValue("avg!bbb").value, 0);
    }

    @Test
    public void testShouldPutGroupSumsIntoHistogram() throws Exception {
        long fromTime = 1000000;
        long toTime = 2000000;

        List<ClientHistoItem> histogram = assembler.newClientHistogram(new DateTime(fromTime), new DateTime(toTime), 10);

        //String thePattern = "hello (\\w+)-(\\d+)";
        FieldSet fieldSet = new FieldSet("(w) (w)-(w)", "part0", "part1", "part2");

//		MatchResult matcherAAA = RegExpUtil.matches(thePattern, "hello aaa-10");
//		MatchResult matcherBBB = RegExpUtil.matches(thePattern, "hello bbb-15");

        List<Function> functions = new ArrayList<Function>();
        functions.add(new Sum("sum", "part1", "part2"));
        Bucket bucket = new Bucket(fromTime, fromTime +100, functions, 0, "thePattern", "hp", "sub", "");
        bucket.increment(fieldSet, fieldSet.getFields("hello aaa-10", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hello aaa-10", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hello bbb-15", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hello bbb-15", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);

        ArrayList<Function> functions2 = new ArrayList<Function>();
        functions2.add(new Sum("sum", "part1",  "part2"));
        assembler.pushIntoClientHistoItem(histogram.get(0), bucket, 10, functions2, true, 0, 0, sharedFunctionData,histogram.size(), filters, functionFilters, subscriber, fieldSets, requestStartTimeMs, request);

        assertEquals(20.0, histogram.get(0).getSeriesValue("sum!aaa").value, 0);
        assertEquals(30.0, histogram.get(0).getSeriesValue("sum!bbb").value, 0);
    }
    @Test
    public void testShouldPutSumProperlyIntoHistogram() throws Exception {
        long fromTime = 1000000;
        long toTime = 2000000;

        List<ClientHistoItem> histogram = assembler.newClientHistogram(new DateTime(fromTime), new DateTime(toTime), 10);

        //String thePattern = "hello (\\w+)-(\\d+)";
        FieldSet fieldSet = new FieldSet("(w) (w)-(w)", "part0", "part1", "part2");


        List<Function> functions = new ArrayList<Function>();
        functions.add(new Sum("sum", "", "part2"));
        Bucket bucket = new Bucket(fromTime, fromTime +100, functions, 0, "thePattern", "hp", "sub", "");
        bucket.increment(fieldSet, fieldSet.getFields("hello aaa-1000", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hello aaa-100", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hello aaa-10", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hello aaa-1", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);

        ArrayList<Function> functions2 = new ArrayList<Function>();
        functions2.add(new Sum("sum", "", "part2"));
        assembler.pushIntoClientHistoItem(histogram.get(0), bucket, 10, functions2, true, 0, 0, sharedFunctionData,histogram.size(), filters, functionFilters, subscriber, fieldSets, requestStartTimeMs, request);

        assertNotNull("Got nothing for sum",histogram.get(0).getSeriesValue("sum"));
        assertEquals("Didnt get a result:" + histogram.toString(), 1111.0, histogram.get(0).getSeriesValue("sum").value, 0);
    }

    @Test
    public void testShouldPutGroupMaxIntoHistogram() throws Exception {
        long fromTime = 1000000;
        long toTime = 2000000;

        List<ClientHistoItem> histogram = assembler.newClientHistogram(new DateTime(fromTime), new DateTime(toTime), 10);

        //String thePattern = "hello (\\w+)-(\\d+)";
        FieldSet fieldSet = new FieldSet("(w) (w)-(w)", "part0", "part1", "part2");

        ArrayList<Function> arrayList = new ArrayList<Function>();
        Max max = new Max("mm", "part1", "part2");
        arrayList.add(max);

        Bucket bucket = new Bucket(fromTime, fromTime +100, arrayList, 0, "thePattern", "hp", "sub", "");
        bucket.increment(fieldSet, fieldSet.getFields("hello aaa-10", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hello aaa-15", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hello bbb-15", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hello bbb-30", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);

        ArrayList<Function> functions2 = new ArrayList<Function>();
        functions2.add(new Max("mm", "part1", "part2"));
        assembler.pushIntoClientHistoItem(histogram.get(0), bucket, 10, functions2, true, 0, 0, sharedFunctionData,histogram.size(), filters, functionFilters, subscriber, fieldSets, requestStartTimeMs, request);

        assertEquals(15.0, histogram.get(0).getSeriesValue("mm!aaa").value, 0);
        assertEquals(30.0, histogram.get(0).getSeriesValue("mm!bbb").value, 0);
    }

    @Test
    public void testShouldPutGroupMinIntoHistogram() throws Exception {
        long fromTime = 1000000;
        long toTime = 2000000;

        List<ClientHistoItem> histogram = assembler.newClientHistogram(new DateTime(fromTime), new DateTime(toTime), 10);

//		String thePattern = "hello (\\w+)-(\\d+)";
        FieldSet fieldSet = new FieldSet("(w) (w)-(w)", "part0", "part1", "part2");


        ArrayList<Function> arrayList = new ArrayList<Function>();
        arrayList.add(new Min("min", "part1", "part2"));

        Bucket bucket = new Bucket(fromTime, fromTime +100, arrayList, 0, "thePattern", "hp", "sub", "");
        bucket.increment(fieldSet, fieldSet.getFields("hello aaa-10", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hello aaa-15", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hello bbb-15", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hello bbb-30", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);

        ArrayList<Function> functions2 = new ArrayList<Function>();
        functions2.add(new Min("min", "part1",  "part2"));
        assembler.pushIntoClientHistoItem(histogram.get(0), bucket, 10, functions2, true, 0, 0, sharedFunctionData,histogram.size(), filters, functionFilters, subscriber, fieldSets, requestStartTimeMs, request);

        assertEquals(10.0, histogram.get(0).getSeriesValue("min!aaa").value, 0);
        assertEquals(15.0, histogram.get(0).getSeriesValue("min!bbb").value, 0);
    }

    @Test
    public void testShouldSupportTwoOfSameFunctionInHistogram() throws Exception {
        long fromTime = 1000000;
        long toTime = 2000000;

        List<ClientHistoItem> histogram = assembler.newClientHistogram(new DateTime(fromTime), new DateTime(toTime), 10);

        ArrayList<FunctionFactory> functions = new ArrayList<FunctionFactory>();
        functions.add(new Min("min1", "part1", "part2"));
        functions.add(new Min("min2", "part1", "part3"));
        //String thePattern = "hello (\\w+)-(\\d+)-(\\d+)";
        FieldSet fieldSet = new FieldSet("(w) (w)-(d)-(d)", new String[] { "part0", "part1", "part2","part3" });

        ArrayList<Function> bucketFunctions = new ArrayList<Function>();
        bucketFunctions.add(new Min("min1", "part1", "part2"));
        bucketFunctions.add(new Min("min2", "part1", "part3"));

        Bucket bucket = new Bucket(fromTime, fromTime +100, bucketFunctions, 0, "thePattern", "hp", "sub", "");
        bucket.increment(fieldSet, fieldSet.getFields("hello aaa-10-20", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hello aaa-15-5", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hello bbb-15-20", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);
        bucket.increment(fieldSet, fieldSet.getFields("hello bbb-30-50", -1, -1, timeMs), filenameOnly, "one", time, fileStartTime, fileEndTime, lineNumber++, lineData, matchResult, isSummary, requestStartTimeMs, requestEndMs);

        ArrayList<Function> functions2 = new ArrayList<Function>();
        functions2.add(new Min("min1", "part1", "part2"));
        functions2.add(new Min("min2", "part1", "part3"));
        assembler.pushIntoClientHistoItem(histogram.get(0), bucket, 10, functions2, true, 0, 0, sharedFunctionData,histogram.size(), filters, functionFilters, subscriber, fieldSets, requestStartTimeMs, request);

        assertEquals(10.0, histogram.get(0).getSeriesValue("min1!aaa").value, 0);
        assertEquals(15.0, histogram.get(0).getSeriesValue("min1!bbb").value, 0);
        assertEquals(5.0, histogram.get(0).getSeriesValue("min2!aaa").value, 0);
        assertEquals(20.0, histogram.get(0).getSeriesValue("min2!bbb").value, 0);
    }

    @Test
    public void testShouldConvertAnEventToHistogram() throws Exception {

        long fromTime = 1000000;
        long toTime = 2000000;

        List<ClientHistoItem> histogram = assembler.newClientHistogram(new DateTime(fromTime), new DateTime(toTime), 10);

        Bucket bucket = new Bucket(fromTime, fromTime +100, new ArrayList<Function>(), 0, "myPattern", "hp", "sub", "");
        bucket.increment();

        assembler.putIntoHistogram(histogram.get(0), bucket, new DateTime(fromTime), new DateTime(toTime), 10, filters);

        assertEquals(1.0, histogram.get(0).getSeriesValue("myPattern").value, 0);

    }

    @Test
    public void testShouldConvertTwoEventsToHistogramInSameBucket() throws Exception {

        long fromTime = 1000000;
        long toTime = 2000000;

        List<ClientHistoItem> histogram = assembler.newClientHistogram(new DateTime(fromTime), new DateTime(toTime), 10);

        Bucket bucket = new Bucket(fromTime, fromTime +10, new ArrayList<Function>(), 0, "myPattern", "source", "sub", "");
        bucket.increment();
        assembler.putIntoHistogram(histogram.get(0), bucket, new DateTime(fromTime), new DateTime(toTime), 10, filters);
        assembler.putIntoHistogram(histogram.get(0), bucket, new DateTime(fromTime), new DateTime(toTime), 10, filters);

        assertEquals(2.0, histogram.get(0).getSeriesValue("myPattern").value, 0);
    }
}
