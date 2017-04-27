package com.liquidlabs.log.space.agg;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.LogRequestBuilder;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.Query;
import com.liquidlabs.log.search.functions.*;
import com.liquidlabs.log.search.handlers.SummaryBucket;
import com.liquidlabs.log.space.LogRequest;
import com.liquidlabs.log.util.DateTimeExtractor;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class HistoManagerTest {
	
	HistoManager assember = new HistoManager();
	long fromTime = new DateTime().minusHours(1).getMillis();
	long toTime = new DateTime().getMillis();
	int size = 100;
	String filename = "file";
	String hostname = "host";
	private LogRequest request;
	String subscriber ="";
	String requestId ="";
	private String sourceURI;
	private boolean hitmapDisabled;
	private MatchResult matchResult;
	private String rawLineData;
	private long requestStartTimeMs;
    private boolean allowPostAggs = false;

    @Before
	public void setUp() throws Exception {
		request = new LogRequest("sub", fromTime, toTime);
	}
    @Test
    public void testShouldCreateHistoForSingleBucket() throws Exception {
        List<Long> bucketTimes = new HistoManager().getBucketTimes(new DateTime().minusHours(1), new DateTime(), 1, true);
        assertEquals(1, bucketTimes.size());


    }
    @Test
    public void testShouldGetGoodBucketsFromEvent() throws Exception {
        List<Long> times = Arrays.asList(1389701280000l, 1389701287000l, 1389701295000l, 1389701302000l, 1389701310000l, 1389701317000l, 1389701325000l, 1389701332000l, 1389701340000l, 1389701347000l, 1389701355000l, 1389701362000l, 1389701370000l, 1389701377000l, 1389701385000l, 1389701392000l, 1389701400000l);
        int bucketIndex = assember.getBucketIndex(times.get(times.size()-1), times);
        assertEquals("Should have got Last Item", bucketIndex, times.size()-1);

        bucketIndex = assember.getBucketIndex(times.get(0)-10, times);
        assertEquals("Should have BEFORE first", -1, bucketIndex);

        bucketIndex = assember.getBucketIndex(times.get(0), times);
        assertEquals("Should have first", 0, bucketIndex);

        bucketIndex = assember.getBucketIndex(times.get(0)+10, times);
        assertEquals("Should have first", 0, bucketIndex);



        bucketIndex = assember.getBucketIndex(times.get(times.size()-1)+10, times);
        assertEquals("Should have last Plus one", times.size(), bucketIndex);

    }
    @Test
    public void testShouldGetGoodBuckets() throws Exception {
        List<Long> bucketTimes = assember.getBucketTimes(new DateTime(1363080746160l), new DateTime(1363167146160l), 96, true);
        assertTrue(bucketTimes.get(0) != bucketTimes.get(1));

    }

    @Test
    public void shouldDoStuff() throws Exception {
        DateTime time = new DateTime();
        LogRequest request = new LogRequestBuilder().getLogRequest("111", Arrays.asList("XXX | bucketWidth(1m)"), "", time.minusHours(1).getMillis(), time.getMillis());
        request.setSearch(true);
        request.setSummaryRequired(true);
//        logRequest.setReplay(new Replay(ReplayType.END, maxReplayItems))
        List<Map<String, Bucket>> maps = new HistoManager().newHistogram(request);
        long firstTime = maps.get(0).values().iterator().next().getStart();
        long secondTime = maps.get(1).values().iterator().next().getStart();
        System.out.println(maps);
        assertTrue(firstTime != secondTime);
    }


	@Test
	public void shouldGetBucketItemSimple() throws Exception {
		long startTime = 0;
		long endTime = 10;
		List<Long> bucketTimes = assember.getBucketTimes(new DateTime(startTime), new DateTime(endTime), 5, false);
        long bucketWidth = bucketTimes.get(1) - bucketTimes.get(0);
        long[] startEndBucketTime = assember.getStartEndBucketTime(bucketTimes, 11, bucketWidth);
		
		System.out.println(startTime + " - " +  endTime);
		for (Long b : bucketTimes) {
			System.out.println("Histo:" + b);
			
		}
		System.out.println("FoundBucket:" + startEndBucketTime[0] + " - " + startEndBucketTime[1]);
		assertTrue(startEndBucketTime[0] > bucketTimes.get(0).longValue());
		assertTrue(startEndBucketTime[0] == bucketTimes.get(bucketTimes.size()-1).longValue() + bucketWidth);
	}

	@Test
	public void shouldGetBucketItem() throws Exception {
		DateTime baseTime = new DateTime();
		baseTime = baseTime.minusMillis(baseTime.getMillisOfSecond());
		baseTime = baseTime.minusSeconds(baseTime.getSecondOfMinute());
		
		DateTime startTime = baseTime.minusMinutes(11);
		DateTime endTime = baseTime.minusMinutes(1);
		List<Long> bucketTimes = assember.getBucketTimes(startTime, endTime, 5, false);
        long bucketWidth = bucketTimes.get(1) - bucketTimes.get(0);
        long[] startEndBucketTime = assember.getStartEndBucketTime(bucketTimes, new DateTime().getMillis(), bucketWidth);
		
		System.out.println(DateUtil.shortDateTimeFormat7.print(startTime) + " - " +  DateUtil.shortDateTimeFormat7.print(endTime));
		for (Long b : bucketTimes) {
			System.out.println("Histo:" + DateUtil.shortDateTimeFormat7.print(b));
			
		}
		System.out.println("FoundBucket:" + DateUtil.shortDateTimeFormat7.print(startEndBucketTime[0]) + " - " + DateUtil.shortDateTimeFormat7.print(startEndBucketTime[1]));
		assertTrue(startEndBucketTime[0] > bucketTimes.get(0).longValue());
		assertEquals(startEndBucketTime[0], bucketTimes.get(bucketTimes.size()-1).longValue() + bucketWidth);
	}

	
	@Test
	public void shouldCreateSmallBuckets() throws Exception {
		//LogRequest Sub:sysadmin_X_New_X_14-30-28.255-CL[LogCons4ED61046-6E61-7789-8E9E-81907B404A08] Q:[Query ptrn:'*' type:old_school sPos:0 pos:0 replay:true fun[Count _type  _type ] filter[]] Buckets:90 Times[28-01-13_1427.00=>28-01-13_1430.00] LIVE:false Includes:*,*logscape*schedule-all*,*logscape*schedule-all*
		LogRequest request = new LogRequest();
		request.setBucketCount(90);
		request.addQuery(new Query(0,0, "pattern", "pattern", true));
		DateTime endTime = new DateTime();
		request.setStartTimeMs(endTime.minusMinutes(3).getMillis());
		request.setEndTimeMs(endTime.getMillis());
		

		List<Map<String, Bucket>> histo = new HistoManager().newHistogram(request);
		
		int pos = 0;
		for (Map<String, Bucket> map : histo) {
			Bucket next = map.values().iterator().next();
			System.out.println(pos++ + ")" + next);
		}
		assertTrue(histo.get(0).values().iterator().next().getStart() != histo.get(1).values().iterator().next().getStart());

		
	}
	
	@Test
	public void testCanCreateDayBasedBuckets() throws Exception {
        final int days = 24;
        int buckets = days;
		LogRequest request = new LogRequest();
		request.setBucketCount(buckets);
		request.addQuery(new Query(0,0,  "pattern", "pattern", true));

        final DateTime endTime = new DateTime();
        DateTime startTime = endTime.minusDays(days);
		List<Map<String, Bucket>> histo = assember.newHistogram(startTime, endTime, buckets, request.queries(), subscriber, hostname, sourceURI);

		assertThat(histo.size(),is(Matchers.greaterThanOrEqualTo(days)));
		assertThat(histo.size(),is(Matchers.lessThanOrEqualTo(days+1)));
		assertNotNull(histo.get(0).get("0pattern"));
	}
	@Test
	public void testCanCreateHourBasedBuckets() throws Exception {
		int buckets = 24;
		LogRequest request = new LogRequest();
		request.setBucketCount(buckets);
		request.addQuery(new Query(0,0, "pattern", "pattern", true));
		
		DateTime startTime = new DateTime().minusDays(1);
		DateTime endTime = new DateTime();
		List<Map<String, Bucket>> histo = assember.newHistogram(startTime, endTime, buckets, request.queries(), subscriber, hostname, sourceURI);
		assertThat(histo.size(), is(Matchers.greaterThanOrEqualTo(buckets)));
		assertThat(histo.size(), is(Matchers.lessThanOrEqualTo(buckets+1)));
		assertNotNull(histo.get(0).get("0pattern"));
	}
	@Test
	public void testCanCreateMinuteBasedBucket() throws Exception {
		int buckets = 10;
		LogRequest request = new LogRequest();
		request.setBucketCount(buckets);
		request.addQuery(new Query(0,0, "pattern", "pattern", true));
		
		DateTime startTime = new DateTime().minusHours(1);
		DateTime endTime = new DateTime();
		List<Map<String, Bucket>> histo = assember.newHistogram(startTime, endTime, buckets, request.queries(), subscriber, hostname, sourceURI);
		
		for (Map<String, Bucket> map : histo) {
			Bucket next = map.values().iterator().next();
			System.out.println(next);
		}
		
		assertThat(histo.size(), is(Matchers.greaterThanOrEqualTo(buckets)));
		assertThat(histo.size(), is(Matchers.lessThanOrEqualTo(buckets+1)));
		assertNotNull(histo.get(0).get("0pattern"));
	}
	
	@Test
	public void shouldPushHostsFieldsTogether() throws Exception {
		long now = DateTimeUtils.currentTimeMillis();
		
		Bucket bucket1 = new SummaryBucket();
		FieldSet fieldSet1 = FieldSets.getLog4JFieldSet();
		fieldSet1.addDefaultFields(fieldSet1.id, "host1", "file1.log", "path", "tag", "agent", "", 1, false);
		String nextLine1 = "2008-11-11 17:02:21,367 INFO main (ResourceAgentImpl.java:395)	 - dashboard-1.0 has been deployed";
		String[] fields = fieldSet1.getFields(nextLine1); 
		bucket1.increment(fieldSet1, fields, "file1.log", "/file1.log", now, now, now, 1, nextLine1, null, true, requestStartTimeMs, 1);
		bucket1.convertFuncResults(true);
		
		Bucket bucket2 = new SummaryBucket();
		FieldSet fieldSet2 = FieldSets.getLog4JFieldSet();
		fieldSet2.addDefaultFields(fieldSet2.id, "host2", "file2.log", "path", "tag", "agent", "", 1, false);
		String nextLine2 = "2008-11-11 17:02:21,367 WARN main (AgentImpl.java:395)	 - dashboard-1.0 has been deployed";
		String[] fields2 = fieldSet2.getFields(nextLine2); 
		bucket2.increment(fieldSet2, fields2, "file2.log", "/file1.log", now, now, now, 1, nextLine2, null, true, requestStartTimeMs, 1);
		bucket2.convertFuncResults(true);
		
		String before =  bucket2.getAggregateResults().toString();
		System.out.println("BEFORE:" + before);
		System.out.println("BEF-FUNCS:" + bucket2.functions().toString());
		
		// now see if we can combine the summary buckets
		assember.handle(bucket2, bucket1, 1, true, true);
		bucket2.convertFuncResults(false);
		
		Map<String, Map> aggregateResults = bucket2.getAggregateResults();
		System.out.println("Bucket2:" + aggregateResults);
		assertTrue(aggregateResults.toString().contains("host1"));
		assertTrue(aggregateResults.toString().contains("host2"));
		
		
		
	}
	@Test
	public void shouldPushIntoOtherHisto() throws Exception {
			// bucket 1-- SUMM:{
			// host={alteredcarbon.local=13279}, 
			// level={DEBUG=748, ERROR=3283, WARN=815, INFO=22656},
			//thread={main=1337, New_I/O_server_worker_#1-1=1241, New_I/O_server_worker_#1-4=759, watch-visit-22-1=618, qtp0-2=1658}, 
			//package={(download.DownloadTask)=3601, (agg.ClientHistoAssembler)=1871, (download.Downloader)=2119, (proxy.ProxyClient)=4542, (proxy.PeerHandler)=1812}, 
			//filename={logserver.log=18, logspace.log=478, dashboard.log=3069, aggspace.log=918, agent.log=8829}, type={log4j=13198},
			// msg={LOGGER  ******** P[stcp://192.168.0.3:11040?serviceName=AggSpace&host=alteredcarbon.local/_startTime=29-Oct-10_21-38-45&udp=0] Tenors[[0.*]]=360, Bad invocation suppressed msg:Uploader=452, stcp://192.168.0.3:11050?serviceName=VSOMain&host=alteredcarbon.local**** SENDING ERROR BACK TO CALLER ****** :stcp://192.168.228.129:11050?serviceName=VSOMain&host=neil-vm=388, bucket::{}=360, Got EX from Bytes[]:4290=1328}} 
			// f:7
			List<Function> functions1 = new ArrayList<Function>();
			functions1.add(new Count("level","","level"));
			Bucket bucket = new Bucket(100, 200, functions1, 0, "*", "usr","sub", "");
			Map<String, Map> functionResults = new HashMap<String, Map>();
			Map countValues = new HashMap<String, Integer>();
			countValues.put("DEBUG", new IntValue(748));
			countValues.put("ERROR", new IntValue(3283));
			countValues.put("WARN", new IntValue(815));
			countValues.put("INFO", new IntValue(22656));
			functionResults.put(functions1.get(0).toStringId(), countValues );
			bucket.setFunctionResults(functionResults);
			bucket.increment();;
			
			// Bucket 2 (AGG Bucket	2-- SUMM-AGG:{} f:7
			List<Function> functions2 = new ArrayList<Function>();
			Bucket aggBucket = new Bucket(100, 200, functions2, 0, "*", "usr","sub", "");
			
			// now see if this data can be pushing to the aggBucket
			assember.handle(aggBucket, bucket, 1, true, true);
			aggBucket.convertFuncResults(false);
			
			assertNotNull(aggBucket.getAggregateResults().size());
			assertEquals(1, aggBucket.getAggregateResults().size());
			System.out.println("Results:" + aggBucket.getAggregateResults().get("level"));;

		
	}
	
	@Test
	public void testShouldCopyHisto() throws Exception {
		
		
		fromTime = new DateTime(2009,4,24,10,52,00,00).getMillis();
		
		// setup
		LogRequest request = new LogRequest();
		request.setBucketCount(10);
		Query query = new Query(0,0, "pattern", "pattern", true);
		query.addFunction(new Count("count","groupField", "applyField"));
		request.addQuery(query);
		
		int offset = 4;
		List<Map<String, Bucket>> dest = assember.newHistogram(new DateTime(fromTime + offset * DateUtil.MINUTE), new DateTime(fromTime + (10 + offset * DateUtil.MINUTE)), 10, request.queries(), subscriber, hostname, sourceURI);
		
		List<Map<String, Bucket>> source = assember.newHistogram(new DateTime(fromTime), new DateTime(fromTime + 10 * DateUtil.MINUTE), 10, request.queries(), subscriber, hostname, sourceURI);
		
		int p = 0;
		for (Map<String, Bucket> map : source) {
			Bucket bucket = new Bucket(fromTime + p * DateUtil.MINUTE, fromTime + (p + 1) * DateUtil.MINUTE, new ArrayList<Function>(), 0, "pattern", "sourceURI", "subscriber", "");
			bucket.increment();
			map.put(query.key(), bucket);
			System.out.println("Bucket:" + bucket.toStringTime() + " " + bucket.toString());
			p++;
		}
		
		assember.copy(dest, source, DateUtil.MINUTE);
		
		assertNotNull(dest.get(0).get(query.key()));
		// first copied bucket
		assertEquals(1, dest.get(0).get(query.key()).hits());
		// first non-copied bucket
		assertEquals(0, dest.get(7).get(query.key()).hits());
		
		int totalHits = 0;
		for (Map<String, Bucket> map : dest) {
			if (map.values().size() > 0) {
				totalHits += map.values().iterator().next().hits();
			}
		}
		assertEquals(5, totalHits);
	}
	
	@Test
	public void shouldScrollGood_BUG_FIX() throws Exception {
//		2012-11-06 09:35:00,745 WARN tailer-young-111-4 (agg.HistoManager)	Scroll this bucket really far:Bucket.-2502287e:13ad500760c:5803 qpos:0 p:* sub:sysadmA] hits:1[09:35:00.000 - 09:35:10.000]
		DateTimeExtractor extractor = new DateTimeExtractor("hh:mm:ss.SSS");
		Bucket hitBucket = new Bucket(extractor.getTime("09:35:00.000", 0).getTime(), extractor.getTime("09:35:10.000", 0).getTime(),null,0,"*", "","", "");
		hitBucket.increment();
		
		// 2012-11-06 09:35:00,745 WARN tailer-young-111-4 (agg.HistoManager)	>{0*=in_X_-7A] hits:0[08:33:11.000 - 08:33:21.000]}
		// 2012-11-06 09:35:00,899 WARN tailer-young-111-4 (agg.HistoManager)	>{0*=B4C87A] hits:0[09:34:44.000 - 09:34:55.000]}

		Query query = new Query(0,0,"*","*",true);
		List<Map<String, Bucket>> histo  = new ArrayList<Map<String, Bucket>>();
		long from = extractor.getTime("07:33:11.000", 0).getTime();
		long to = extractor.getTime("08:34:55.000", 0).getTime();
		long delta = (to - from)/360;
		long time = from;
		for (int i = 0; i < 360; i++) {
			HashMap<String, Bucket> histoItem = new HashMap<String, Bucket>();
			Bucket bucket = new Bucket(time, time + delta, query.functions(),
					query.position(), query.pattern(),
					sourceURI, subscriber, "");
			histoItem.put(query.key(), bucket);
			time += delta;
			histo.add(histoItem);
		}

        System.out.println("1 BucketSize:" + histo.size());
        System.out.println("1 LastTime:" + histo.get(histo.size()-1).values());
        // out of bounds bucket should scroll
		assember.handle(histo, hitBucket, request, size);
		System.out.println("done");
        System.out.println("2 BucketSize:" + histo.size());
        System.out.println("2 LastTime:" + histo.get(histo.size()-1).values());




    }
	
	@Test
	public void testShouldScroll() throws Exception {
		
		FieldSet fieldSet = new FieldSet("(*) (**)","filename","level");
		Count count = new Count("TAG", "filename", "level");
		count.execute(fieldSet, fieldSet.getFields("agent.log INFO"), "*", 1, matchResult, rawLineData, requestStartTimeMs, 1);
		Map results = count.getResults();
		

		List<Map<String, Bucket>> histo = makeHisto();
		
		Bucket firstBucket = histo.get(0).get("0pattern");
		firstBucket.functions().add(count);
		firstBucket.increment();
		
		Bucket lastBucket = histo.get(histo.size()-1).get("0pattern");
		long width = lastBucket.getEnd() - lastBucket.getStart();
		
		Bucket bucketToForceScrolling = new Bucket(lastBucket.getEnd()+1,lastBucket.getEnd() + width, null, 0, "pattern", "", "", "");
		bucketToForceScrolling.increment();
		
		long[] froms = new long[] { histo.get(0).values().iterator().next().getStart(), histo.get(1).values().iterator().next().getStart()};
		
		// out of bounds bucket should scroll
		assember.handle(histo, bucketToForceScrolling, request, size);
		
		long[] froms2 = new long[] { histo.get(0).values().iterator().next().getStart(), histo.get(1).values().iterator().next().getStart()};
		
		for (int i = 0; i < froms.length; i++) {
			System.out.println(i + ")  B:" + new DateTime(froms[i]) + "     A:" + new DateTime(froms2[i]));
		}
		
		assertEquals(new DateTime(froms[1]).toString(), new DateTime(froms2[0]).toString());
		
		Bucket newLast = histo.get(histo.size()-1).get("0pattern");
		assertEquals(1, newLast.hits());
		
		// now that the first bucket has scrolled to the end - the function results should be empty.
		assertEquals(0 ,newLast.functions().get(0).getResults().size());
		
		
		
		
	}
	
	@Test
	public void testShouldHandleHitAdd() throws Exception {

		List<Map<String, Bucket>> histo = makeHisto();
		
		Bucket bucket2 = histo.get(0).get("0pattern");
		
		Bucket bucket = new Bucket(bucket2.getStart(),bucket2.getEnd(), null, 0, "pattern", "", "", "");
		bucket.increment();
		
		assember.handle(histo, bucket, request, size);
		
		assertEquals(1, histo.get(0).get("0pattern").hits());
	}
	
	@Test
	public void testShouldHandleHitAddx2() throws Exception {
		
		List<Map<String, Bucket>> histo = makeHisto();
		
		Bucket bucket2 = histo.get(0).get("0pattern");
		
		Bucket bucket = new Bucket(bucket2.getStart(),bucket2.getEnd(), null, 0, "pattern", "", "", "");
		bucket.increment();
		
		assember.handle(histo, bucket, request, size);
		assember.handle(histo, bucket, request, size);
		
		assertEquals(2, histo.get(0).get("0pattern").hits());
	}


	
	
	@Test
	public void testShouldHandleBucketWithCountFunction() throws Exception {
		
		// setup
		LogRequest request = new LogRequest();
		request.setBucketCount(10);
		Query query = new Query(0, 0, "pattern", "pattern", false);
		query.addFunction(new Count("count","data", "data"));
		request.addQuery(query);
		List<Map<String, Bucket>> histo = assember.newHistogram(new DateTime(fromTime), new DateTime(toTime), request.getBucketCount(), request.queries(), subscriber, hostname, sourceURI);
		
		
		// incoming bucket
		ArrayList<Function> functions = new ArrayList<Function>();
		Count count = new Count("count", "data", "data");
		count.updateResult("stuff", 100);
		functions.add(count);
		Bucket bucket = new Bucket(fromTime, toTime, functions, 1, "pattern", "sourceURI", "sub", "");
		bucket.convertFuncResults(true);
		bucket.increment();
		
		
		// test
		Bucket first = histo.get(0).values().iterator().next();
		assember.aggregateGroupingFunctionForBucket(first, bucket, true, allowPostAggs);
		
		// prime results
		first.convertFuncResults(true);
		
		assertEquals(1, first.getAggregateResultKeys().size());
		assertEquals("[" + count.toStringId() +  "]", first.getAggregateResultKeys().toString());
		assertEquals("{stuff=100}", first.getAggregateResult(count.toStringId(), true).toString());
	}
	
	@Test
	public void testShouldHandleBucketWithAverageFunction() throws Exception {
		
		// setup
		LogRequest request = new LogRequest();
		request.setBucketCount(10);
		Query query = new Query(0, 0, "pattern", "pattern", false);
		query.addFunction(new Average("average","data", "data"));
		request.addQuery(query);
		List<Map<String, Bucket>> histo = assember.newHistogram(new DateTime(fromTime), new DateTime(toTime), request.getBucketCount(), request.queries(), subscriber, hostname, sourceURI);
		
		
		// incoming Bucket
		ArrayList<Function> functions = new ArrayList<Function>();
		Average average = new Average("average", "data", "data");
		average.updateResult("stuff", 100);
		functions.add(average);
		Bucket bucket = new Bucket(fromTime, toTime, functions, 1, "pattern", "sourceURI", "sub", "");
		bucket.convertFuncResults(true);
		bucket.increment();
		
		// do the work
		Bucket first = histo.get(0).values().iterator().next();
		assember.aggregateGroupingFunctionForBucket(first, bucket, true, allowPostAggs);
		
		// prime results
		first.convertFuncResults(true);
		
		assertEquals(1, first.getAggregateResultKeys().size());
		assertEquals("["+average.toStringId() + "]", first.getAggregateResultKeys().toString());
		assertTrue(first.getAggregateResult(average.toStringId(), true).toString().contains("t:100.0}"));
	}

    @Test
    public void shouldHandleBucketWithElapsedFunction() throws Exception {
        // setup
		LogRequest request = new LogRequest();
		request.setBucketCount(10);
		Query query = new Query(0, 0, "pattern", "pattern", false);
		query.addFunction(new Elapsed("foo","data", "data", "data", "m", "", false));
		request.addQuery(query);
		List<Map<String, Bucket>> histo = assember.newHistogram(new DateTime(fromTime), new DateTime(toTime), request.getBucketCount(), request.queries(), subscriber, hostname, sourceURI);


		// incoming bucket
		Map<String, Object> data  = new HashMap<String, Object>();
        data.put("elapsed", Collections.singletonList(new ElapsedInfo(0, 10, 1, "")));
        ArrayList<Function> functions = new ArrayList<Function>();
		Elapsed elapsed = new Elapsed("foo","data", "data", "data", "m", "", false);
		elapsed.updateResult("elapsed", data);
		functions.add(elapsed);
		Bucket bucket = new Bucket(fromTime, toTime, functions, 1, "pattern", "sourceURI", "sub", "");
		bucket.convertFuncResults(true);
		bucket.increment();


		// test
		Bucket first = histo.get(0).values().iterator().next();
		assember.aggregateGroupingFunctionForBucket(first, bucket, true, allowPostAggs);

		// prime results
		first.convertFuncResults(true);

        String stringId = elapsed.toStringId();

		assertEquals(1, first.getAggregateResultKeys().size());
		assertEquals("["+stringId +"]", first.getAggregateResultKeys().toString());

		boolean normalExpects = "{elapsed=[ElapsedInfo [] startTime:01-Jan-70 01:00:00, endTime:01-Jan-70 01:00:00\n]}".equals(first.getAggregateResult(stringId, true).toString());
		boolean tzDiffExpects = "{elapsed=[ElapsedInfo [] startTime:01-Jan-70 00:00, endTime:01-Jan-70 00:00\n]}".equals(first.getAggregateResult(stringId, true).toString());
		assertTrue(normalExpects || tzDiffExpects);//Equals("{elapsed=[ElapsedInfo [] startTime:1970-01-01T01:00:00.000+01:00, endTime:1970-01-01T01:00:00.010+01:00\n]}", first.getAggregateResult("foo", true).toString());
    }
	
	@Test
	public void testShouldHandleBucketWith2XAverageBuckets() throws Exception {
		
		// setup
		LogRequest request = new LogRequest();
		request.setBucketCount(10);
		Query query = new Query(0, 0, "pattern", "pattern", false);
		query.addFunction(new Average("average", "data", "data"));
		request.addQuery(query);
		List<Map<String, Bucket>> histo = assember.newHistogram(new DateTime(fromTime), new DateTime(toTime), request.getBucketCount(), request.queries(), subscriber, hostname, sourceURI);
		
		
		// incoming Bucket-1
		ArrayList<Function> functions = new ArrayList<Function>();
		Average average = new Average("average", "data", "data");
		average.updateResult("stuff", 100);
		functions.add(average);
		Bucket bucket1 = new Bucket(fromTime, toTime, functions, 1, "pattern", "sourceURI", "sub", "");
		bucket1.convertFuncResults(true);
		bucket1.increment();
		
		// incoming Bucket-1
		ArrayList<Function> functions2 = new ArrayList<Function>();
		Average average2 = new Average("average", "data", "data");
		average2.updateResult("stuff", 200);
		functions2.add(average2);
		Bucket bucket2 = new Bucket(fromTime, toTime, functions2, 1, "pattern", "sourceURI", "sub", "");
		bucket2.convertFuncResults(true);
		bucket2.increment();
		
		// do the work
		Bucket first = histo.get(0).values().iterator().next();
		assember.aggregateGroupingFunctionForBucket(first, bucket1, true, allowPostAggs);
		assember.aggregateGroupingFunctionForBucket(first, bucket2, true, allowPostAggs);
		
		// prime results
		first.convertFuncResults(true);
		
		assertEquals(1, first.getAggregateResultKeys().size());
		assertEquals("["+average2.toStringId() + "]", first.getAggregateResultKeys().toString());
		assertEquals("{stuff=Averager e:2 t:300.0}", first.getAggregateResult(average2.toStringId(), true).toString());
	}
	
	@Test
	public void testShouldDoSomething() throws Exception {
		FieldSet fieldSet = new FieldSet(".*FOO (.+)-(\\d+) FOO.*","p1","p2");
		LogRequest request = new LogRequest();
		request.setBucketCount(10);
		Query query = new Query(0, 0, ".*FOO (.+)-(\\d+) FOO.*", "orig", false);
		Average function = new Average("average","p1", "p2");
		System.out.println("Avg:" + function.toString());
		query.addFunction(function);
		request.addQuery(query);
		List<Map<String, Bucket>> histo = assember.newHistogram(new DateTime(fromTime), new DateTime(toTime), request.getBucketCount(), request.queries(), subscriber, hostname, sourceURI);

		ArrayList<Function> functions = new ArrayList<Function>();
		Average average = new Average("average", "p1", "p2");
		functions.add(average);
		average.execute(fieldSet, fieldSet.getFields("FOO hello-1234 FOO"), "pattern", 10000, matchResult, rawLineData, requestStartTimeMs, 1);

		Bucket bucket1 = new Bucket(fromTime, toTime, functions, 1, "*FOO (.+)-(\\d+) FOO.*", "sourceURI", "sub", "");
		bucket1.convertFuncResults(true);
		bucket1.increment();
		
		ArrayList<Function> functions2 = new ArrayList<Function>();
		Average average2 = new Average("average", "p1", "p2");
		functions2.add(average2);
		average2.execute(fieldSet, fieldSet.getFields("FOO world-3456 FOO"), "pattern", 10000, matchResult, rawLineData, requestStartTimeMs, 2);

		Bucket bucket2 = new Bucket(fromTime, toTime, functions2, 1, "*FOO (.+)-(\\d+) FOO.*", "sourceURI", "sub", "");
		bucket2.convertFuncResults(true);
		bucket2.increment();
		
		// do the work
		Bucket first = histo.get(0).values().iterator().next();
		assember.aggregateGroupingFunctionForBucket(first, bucket1,true, allowPostAggs);
		assember.aggregateGroupingFunctionForBucket(first, bucket2,true, allowPostAggs);
		
		// prime results
		first.convertFuncResults(true);

		assertEquals(1, first.getAggregateResultKeys().size());
		assertEquals("["+average2.toStringId() +"]", first.getAggregateResultKeys().toString());
		assertEquals("{hello=Averager e:1 t:1234.0, world=Averager e:1 t:3456.0}", first.getAggregateResult(average2.toStringId(),true).toString());

		
	}

	private List<Map<String, Bucket>> makeHisto() {
		LogRequest request = new LogRequest();
		request.setBucketCount(10);
		request.addQuery(new Query(0,0, "pattern", "pattern", true));
		List<Map<String, Bucket>> histo = assember.newHistogram(new DateTime(fromTime), new DateTime(toTime), request.getBucketCount(), request.queries(), subscriber, hostname, sourceURI);
		return histo;
	}
}
