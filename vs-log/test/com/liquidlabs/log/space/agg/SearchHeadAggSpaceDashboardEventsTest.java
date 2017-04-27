package com.liquidlabs.log.space.agg;

import com.liquidlabs.log.LogRequestBuilder;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.space.LogRequest;
import com.liquidlabs.transport.serialization.Convertor;
import com.thoughtworks.xstream.XStream;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class SearchHeadAggSpaceDashboardEventsTest {
	
	long fromTime = new DateTime().minusHours(1).getMillis();
	long toTime = new DateTime().getMillis();
	HistoManager assember = new HistoManager();

	String dashboardXML = "";
	@Test
	public void testShouldPassCountDeltasToDashboard() throws Exception {
		
		// setup
	    String query = "(*) | data.count() buckets(1) verbose(false)";
        final LogRequest request = new LogRequestBuilder().getLogRequest("subscriber", Arrays.asList(query), "", fromTime, toTime);
        
		
        final List<Map<String, Bucket>> aggSpaceHisto = assember.newHistogram(request);
		Bucket bucket2 = aggSpaceHisto.get(0).values().iterator().next();
		
		final LinkedBlockingQueue<Bucket> aggQueue = new LinkedBlockingQueue<Bucket>();
		final LinkedBlockingQueue<List<Map<String, Bucket>>> dbQueue = new LinkedBlockingQueue<List<Map<String, Bucket>>>();
		Thread aggThread = new Thread() {
			@Override
			public void run() {
				try {
					while (true) {
						Bucket take = aggQueue.take();
						// AGGSPACE
						assember.handle(aggSpaceHisto, take, request, 100);
						assember.updateFunctionResults(aggSpaceHisto, request.isVerbose());
						
						System.out.println("'\n\n===========================AGG HISTO:" + aggSpaceHisto.size());
						List<Map<String, Bucket>> clonedHisto = (List<Map<String, Bucket>>) Convertor.deserialize(Convertor.serialize(aggSpaceHisto));
						dbQueue.add(clonedHisto);
						assember.resetHisto(aggSpaceHisto);
					}
				} catch (Exception e) {
				}
			}
		};
		aggThread.setDaemon(true);
		aggThread.start();
		
		final ClientHistoAssembler clientHistoAssembler = new ClientHistoAssembler();
		
		final List<Map<String, Bucket>> dbAggHisto =  assember.newHistogram(request);
		final CountDownLatch countDownLatch = new CountDownLatch(2);
		
		Thread dbThread = new Thread() {
			int count = 0;
			public void run() {
				try {
					while (true) {
						List<Map<String, Bucket>> histo = dbQueue.take();
						assember.handle(dbAggHisto, histo, request);
						assember.updateFunctionResults(dbAggHisto, request.isVerbose());
						System.out.println("'\n\n===========================DB HISTO:" + dbAggHisto.size());
						
						HashMap<Integer,Set<String>> functionTags = new HashMap<Integer,Set<String>>();
						List<ClientHistoItem> finalResult = clientHistoAssembler.getClientHistoFromRawHisto(request,dbAggHisto, functionTags, Arrays.asList(FieldSets.getBasicFieldSet()));
						String xml = new XStream().toXML(finalResult);
						
						System.out.println(xml);
						dashboardXML = xml;
						System.out.println("==========================================================\n\n\n");
						count++;
						countDownLatch.countDown();
					}
				} catch (InterruptedException e) {
				}
			}
		};
		dbThread.setDaemon(true);
		dbThread.start();
		
		
		// SEARCH HEAD - puts stuff on the queue
		Bucket bucket = new Bucket(bucket2.getStart(),bucket2.getEnd(), request.queries().get(0).functions(), 0, "(*)", "", request.subscriber(), "");
		bucket.increment(FieldSets.getBasicFieldSet(), new String[] { "ERROR" }, "", "", bucket.getStart(), bucket.getStart(),bucket.getEnd(), 111, "ERROR", null, false, bucket.getStart(), bucket.getEnd());
		bucket.increment(FieldSets.getBasicFieldSet(), new String[] { "ERROR" }, "", "", bucket.getStart(), bucket.getStart(),bucket.getEnd(), 222, "ERROR", null, false, bucket.getStart(), bucket.getEnd());
		bucket.increment(FieldSets.getBasicFieldSet(), new String[] { "WARN" }, "", "", bucket.getStart(), bucket.getStart(),bucket.getEnd(), 333, "ERROR", null, false, bucket.getStart(), bucket.getEnd());
		// ROLL  up for wire sending - fake by putting it on the Q
		bucket.convertFuncResults(false);
		aggQueue.add(bucket);
		Thread.sleep(50);

		// Reset bucket state & SCAN & SEND next delta
		bucket.resetAll();
		bucket.increment(FieldSets.getBasicFieldSet(), new String[] { "WARN" }, "", "", bucket.getStart(), bucket.getStart(),bucket.getEnd(), 444, "ERROR", null, false, bucket.getStart(), bucket.getEnd());
		bucket.convertFuncResults(false);
		aggQueue.add(bucket);

		// WAIT for the Dashboard to complete
		countDownLatch.await(5, TimeUnit.SECONDS);
		
		String warnCount2 = "<label>data!WARN</label>\n" + 
				"          <value>2.0</value>";
		// after the 2 updates Warn should have been incremented from 1 to 2
		assertTrue("Second HISTO PASS Should have WARN == 2",dashboardXML.contains(warnCount2));
	}
}
