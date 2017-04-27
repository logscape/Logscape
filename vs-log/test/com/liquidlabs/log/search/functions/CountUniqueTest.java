package com.liquidlabs.log.search.functions;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.file.raf.RAF;
import com.liquidlabs.common.file.raf.RafFactory;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.space.agg.HistoManager;
import com.liquidlabs.transport.serialization.Convertor;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CountUniqueTest {
	
	private String hostname;
	private String filenameOnly;
	private String filename;
	private String pattern;
	private long time;
	
	FieldSet fieldSet = new FieldSet("^(\\w+) (.*)", "group", "user");
	private MatchResult matchResult;
	private String rawLineData;
	private long requestStartTimeMs;
    private long timeMs = System.currentTimeMillis();


    //    @Test
    public void shouldFoo() throws IOException, ClassNotFoundException {
        FieldSet log4JFieldSet = FieldSets.getLog4JFieldSet();
        CountUniqueHyperLog hyperlog = new CountUniqueHyperLog("TAG", "thread");
        File file = new File("/Users/damian/development/java/logscape/trunk/LogScape/master/build/logscape/work/agent.log");
        System.out.println("CountUniqueHyperLog\n---------------------------");

        for(int i = 0; i < 5; i++) {
            runIt(log4JFieldSet, hyperlog, file);
        }
    }

    private void runIt(FieldSet log4JFieldSet, Counter countUniqueFast, File file) throws IOException, ClassNotFoundException {
//        System.gc();

        RAF raf = RafFactory.getRafSingleLine(file.getAbsolutePath());
        String line = null;
        int lineNo = 1;
        long start = System.currentTimeMillis();
        while((line = raf.readLine()) != null) {
            countUniqueFast.execute(log4JFieldSet, log4JFieldSet.getFields(line, -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, lineNo++);
        }
        raf.close();
        long elapsed = System.currentTimeMillis() - start;
        double seconds = elapsed/1000.0;
        long mbytes = file.length()/ FileUtil.MEGABYTES;
        System.out.println(String.format("Throughput:%f MB/sec: unique:%d", (mbytes/seconds), countUniqueFast.count()));
        Bucket bucket = new Bucket();
        HashMap hashMap = new HashMap();
        hashMap.put(countUniqueFast.getTag(), countUniqueFast.getResults());
        bucket.setFunctionResults(hashMap);
        Bucket other = (Bucket) Convertor.deserialize(Convertor.serialize(bucket));
        System.out.println(((HyperLogLog)other.getAggregateResult(countUniqueFast.toStringId(), false).get(countUniqueFast.getTag())).cardinality());
    }

    @Test
    public void shouldDoCountOnCountUniqueHyperLog() throws Exception {
        // level.countUnique(filename)
        // means agent.log == 3 f2.log ==2 etc
        FieldSet fieldSet = new FieldSet("(*) (*)", "level", "filename");
        CountUniqueHyperLog count2 = new CountUniqueHyperLog("TAG", "filename");
        count2.execute(fieldSet, fieldSet.getFields("INFO agent.log", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 1);
        count2.execute(fieldSet, fieldSet.getFields("WARN agent.log", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 2);
        count2.execute(fieldSet, fieldSet.getFields("INFO agent.log", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 3);
        count2.execute(fieldSet, fieldSet.getFields("INFO f2.log", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 4);
        count2.execute(fieldSet, fieldSet.getFields("INFO f2.log", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 5);

        Map results = count2.getResults();
        System.out.println("Results:" + results);
        assertEquals(1, results.size());
        HyperLogLog tag = (HyperLogLog) results.get("TAG");
        assertEquals(2, tag.cardinality());


        ArrayList<Function> funcs = new ArrayList<Function>();
        funcs.add(count2);

        Bucket summaryBucket = new Bucket(0l, System.currentTimeMillis(), funcs, 0, "", "","", "");
        summaryBucket.convertFuncResults(true);

        Bucket aggBucket = new Bucket();
        CountUniqueHyperLog aggFun = new CountUniqueHyperLog("TAG", "filename");
        aggBucket.functions().add(aggFun);

        new HistoManager().handle(aggBucket, summaryBucket, 1, false, true);
        aggBucket.convertFuncResults(false);

        HyperLogLog aggLog = (HyperLogLog) aggBucket.getAggregateResults().get(aggFun.toStringId()).get("TAG");


        System.out.println("RR:" + aggBucket.getAggregateResults());
        assertEquals(2, aggLog.cardinality());

    }

    @Test
    public void shouldDoCountOnCountUniqueFast() throws Exception {
        // level.countUnique(filename)
        // means agent.log == 3 f2.log ==2 etc
        FieldSet fieldSet = new FieldSet("(*) (*)", "level", "filename");
        CountUniqueHyperLog count2 = new CountUniqueHyperLog("TAG", "filename");
        count2.execute(fieldSet, fieldSet.getFields("INFO agent.log", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 1);
        count2.execute(fieldSet, fieldSet.getFields("WARN agent.log", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 2);
        count2.execute(fieldSet, fieldSet.getFields("INFO agent.log", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 3);
        count2.execute(fieldSet, fieldSet.getFields("INFO f2.log", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 4);
        count2.execute(fieldSet, fieldSet.getFields("INFO f2.log", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 5);

        Map results = count2.getResults();
        System.out.println("Results:" + results);
        assertEquals(1, results.size());
        assertEquals(2, ((ICardinality) results.get("TAG")).cardinality());


        CountUniqueHyperLog aggCount = new CountUniqueHyperLog("TAG", "filename");
        System.out.println("Results:" + results);
        Set<String> keySet = results.keySet();
        for (String key : keySet) {
            aggCount.updateResult(key, (HyperLogLog) count2.getResults().get(key));
        }

        assertEquals(aggCount.getResults().size(), count2.getResults().size());
        assertEquals(2, ((HyperLogLog) results.values().iterator().next()).cardinality());
        System.out.println("Agg:" + aggCount.getResults());
    }

	@Test
	public void shouldAggregateToAnotherBucket() throws Exception {
		CountUnique count2 = new CountUnique("TAG", "filename", "level");
		count2.execute(fieldSet, fieldSet.getFields("one user1", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 1);
		count2.execute(fieldSet, fieldSet.getFields("one user2", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 2);
		count2.execute(fieldSet, fieldSet.getFields("one user3", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 3);
		count2.execute(fieldSet, fieldSet.getFields("one user3", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 4);
		count2.execute(fieldSet, fieldSet.getFields("two user3", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 5);

		CountUnique aggCount = new CountUnique("TAG", "group", "user");
		Map results = count2.getResults();
		System.out.println("Results:" + results);
		Set<String> keySet = results.keySet();
		for (String key : keySet) {
			aggCount.updateResult(key, (HashSet) count2.getResults().get(key));
		}
		
		assertEquals(aggCount.getResults().size(), count2.getResults().size());
	}

//	@Test DodgyTest? Not sure what it is testing
	public void shouldDoCountOnGroup0() throws Exception {
		CountUnique count2 = new CountUnique("TAG", "", "");
		count2.execute(fieldSet, fieldSet.getFields("one user1", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 1);
		count2.execute(fieldSet, fieldSet.getFields("one user2", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 2);
		count2.execute(fieldSet, fieldSet.getFields("one user3", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 3);
		count2.execute(fieldSet, fieldSet.getFields("one user3", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 4);
		
		Map results = count2.getResults();
		assertEquals(1, results.size());
		assertEquals(3, ((ICardinality)results.get("TAG")).cardinality());
	}
//	@Test
//	public void shouldDoCountOnGroupSPLIT() throws Exception {
//		CountUnique count2 = new CountUnique("TAG", "user", "group");
//		count2.execute(fieldSet, fieldSet.getFields("one user1,user2"), hostname, filenameOnly, filename, pattern, time, matchResult, rawLineData);
//		count2.execute(fieldSet, fieldSet.getFields("one user2,user2"), hostname, filenameOnly, filename, pattern, time, matchResult, rawLineData);
//		count2.execute(fieldSet, fieldSet.getFields("one user3,user2"), hostname, filenameOnly, filename, pattern, time, matchResult, rawLineData);
//		count2.execute(fieldSet, fieldSet.getFields("one user3,user2"), hostname, filenameOnly, filename, pattern, time, matchResult, rawLineData);
//		count2.execute(fieldSet, fieldSet.getFields("two user3,user2"), hostname, filenameOnly, filename, pattern, time, matchResult, rawLineData);
//		
//		Map results = count2.getResults();
//		System.out.println("Results:" + results);
//		assertEquals(3, results.size());
//		assertEquals(3, ((HashSet) results.get("user3")).size());
//	}
	@Test
	public void shouldDoCountOnSPLIT() throws Exception {
		CountUnique count2 = new CountUnique("TAG", "", "user");
		count2.execute(fieldSet, fieldSet.getFields("one user1,user2", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 1);
		count2.execute(fieldSet, fieldSet.getFields("one user2,user2", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 2);
		count2.execute(fieldSet, fieldSet.getFields("one user3,user2", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 3);
		count2.execute(fieldSet, fieldSet.getFields("one user3,user2", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 4);
		
		Map results = count2.getResults();
		System.out.println("Results:" + results);
		assertEquals(1, results.size());
		assertNotNull("Didnt get Results!", results.get("TAG"));
		assertEquals(3, ((ICardinality)results.get("TAG")).cardinality());
	}
	@Test
	public void shouldDoCountOnGroup() throws Exception {
		CountUnique count2 = new CountUnique("TAG", "", "user");
		count2.execute(fieldSet, fieldSet.getFields("one user1", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 1);
		count2.execute(fieldSet, fieldSet.getFields("one user2", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 2);
		count2.execute(fieldSet, fieldSet.getFields("one user3", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 3);
		count2.execute(fieldSet, fieldSet.getFields("one user3", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 4);
		
		Map results = count2.getResults();
		System.out.println("Results:" + results);
		assertEquals(1, results.size());
		assertNotNull("Didnt get Results!", results.get("TAG"));
		assertEquals(3, ((ICardinality)results.get("TAG")).cardinality());
	}
	@Test
	public void shouldDoCountOnGroupAndApply() throws Exception {
		// user.countUnique(group)
		CountUnique count2 = new CountUnique("TAG", "group", "user");
		count2.execute(fieldSet, fieldSet.getFields("one user1", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 1);
		count2.execute(fieldSet, fieldSet.getFields("one user2", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 2);
		count2.execute(fieldSet, fieldSet.getFields("two user2", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 3);
		count2.execute(fieldSet, fieldSet.getFields("one user3", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 4);
		count2.execute(fieldSet, fieldSet.getFields("two user3", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 5);
		count2.execute(fieldSet, fieldSet.getFields("three user3", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 6);
		
		Map results = count2.getResults();
		System.out.println("Results:" + results);
		assertEquals(3, results.size());
		assertEquals(3, ((ICardinality) results.get("one")).cardinality());
		assertEquals(2, ((ICardinality) results.get("two")).cardinality());
		assertEquals(1, ((ICardinality) results.get("three")).cardinality());
	}
	@Test
	public void shouldDoCountOnGroupAndApply2() throws Exception {
		// level.countUnique(filename)
		// means agent.log == 3 f2.log ==2 etc
		FieldSet fieldSet = new FieldSet("(*) (*)", "level", "filename");
		CountUnique count2 = new CountUnique("TAG", "filename", "level");
		count2.execute(fieldSet, fieldSet.getFields("INFO agent.log", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 1);
		count2.execute(fieldSet, fieldSet.getFields("WARN agent.log", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 2);
		count2.execute(fieldSet, fieldSet.getFields("INFO agent.log", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 3);
		count2.execute(fieldSet, fieldSet.getFields("INFO f2.log", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 4);
		count2.execute(fieldSet, fieldSet.getFields("INFO f2.log", -1, -1, timeMs), pattern, time, matchResult, rawLineData, requestStartTimeMs, 5);
		
		Map results = count2.getResults();
		System.out.println("Results:" + results);
		assertEquals(2, results.size());
		assertEquals(2, ((ICardinality) results.get("agent.log")).cardinality());
		assertEquals(1, (((ICardinality) results.get("f2.log")).cardinality()));
	}

}
