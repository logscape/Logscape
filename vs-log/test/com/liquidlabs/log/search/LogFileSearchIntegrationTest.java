package com.liquidlabs.log.search;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.concurrent.NamingThreadFactory;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.file.raf.BreakRuleUtil;
import com.liquidlabs.common.regex.SimpleQueryConvertor;
import com.liquidlabs.log.LogReader;
import com.liquidlabs.log.LogRequestBuilder;
import com.liquidlabs.log.NullAggSpace;
import com.liquidlabs.log.TailerImpl;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.FieldSetGuesser2;
import com.liquidlabs.log.fields.FieldSetUtil;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.indexer.persistit.PIIndexer;
import com.liquidlabs.log.reader.LocalIndexerIngestHandler;
import com.liquidlabs.log.reader.GenericIngester;
import com.liquidlabs.log.search.handlers.Handlers;
import com.liquidlabs.log.search.handlers.SummaryBucket;
import com.liquidlabs.log.search.handlers.SummaryBucketHandler;
import com.liquidlabs.log.space.AggSpace;
import com.liquidlabs.log.space.LogRequest;
import com.liquidlabs.log.space.WatchDirectory;
import com.liquidlabs.log.space.agg.HistoManager;
import com.liquidlabs.transport.Config;
import junit.framework.Assert;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class LogFileSearchIntegrationTest {
	{
        System.setProperty("test.mode","true");
        System.setProperty("search.cache.enabled","false");

	}

    //private String SEARCH_EXPR = "* | dstintf.equals(wan1) type.equals(traffic) subtype.equals(forward) _path.count()  buckets(10)";
    private String SEARCH_EXPR = "* ";

    String FILENAME = "logserver2.log";
    String DIR = "/Volumes/SSD2/logscape_trunk/LogScape/master/buildAEGON/logscape/work";

	private boolean isDiscovery = true;
	boolean doSummary = true;


	String TEST_FILE = DIR + "/" + FILENAME;
	private FieldSet fieldSet = FieldSets.getBasicFieldSet();

	
//	private FieldSet fieldSet = (FieldSet) new ObjectTranslator().getObjectFromFormat(FieldSet.class, FileUtil.readAsString("C:\\work\\logs\\FortigateFW\\logscape-fieldsets.config"));

	private NullAggSpace aggSpace = new NullAggSpace();



	private File log;
	private Indexer indexer;
	private LogReader writer;
	private TailerImpl tailer;

	private String fieldSetId;
	private LogFile logFile;


    @Before
	public void setUp() throws Exception {

		try {

            log = new File(TEST_FILE);
            if (!log.exists()) {
                System.err.println("ERROR File not found!:" + log.getAbsolutePath());
                return;
            }
            FileUtil.deleteDir(new File("work"));
			Config.initialise();
			// force DB rebuild
			System.setProperty("log.delete.db", "true");
			System.setProperty("test.debug.mode","true");
			System.setProperty("db.root","build/INT-TEST/work/DB");
			

            FileUtil.deleteDir(new File("build/Test"));
			indexer = new PIIndexer("build/Test");


            String newLineRule = BreakRuleUtil.getStandardNewLineRule(FileUtil.readLines(DIR + "/" + FILENAME, 10), "", "");
            WatchDirectory watch = new WatchDirectory("tag", DIR, FILENAME, "", "", 999, "", isDiscovery, newLineRule, "", false, false);
			
			// run the fieldSetGuesser
//			guessFieldSet();
            //fieldSet.addField("TEST","",true,-1,true,"_filename",".*?\\.(\\S+)","");
			
			indexer.addFieldSet(fieldSet);
			fieldSetId = fieldSet.id;
			
			System.out.println("Using FIELDS:" + fieldSet.fields());
			
			logFile = indexer.openLogFile(log.getAbsolutePath(), true, fieldSetId, "tag1");
			logFile.setNewLineRule(watch.getBreakRule());
			indexer.updateLogFile(logFile);

            LocalIndexerIngestHandler client = new LocalIndexerIngestHandler(indexer);
            writer = new GenericIngester(logFile, indexer, client,  "sourceURI", "hostname", watch);
			tailer = new TailerImpl(log, 0, 0, writer, watch, indexer);
			
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}

	
	@After
	public void tearDown() throws Exception {
		if (indexer != null) indexer.close();
	}

    @Test
	public void testShouldSearch() throws Exception {

		// cant run the test
		if (tailer == null) return;

		// import the file into the DB
		long startImport = DateTimeUtils.currentTimeMillis();
		tailer.call();


		LogFile logFile1 = indexer.openLogFile(log.getAbsolutePath());
		long fLine = logFile1.getLineCount() - 300;
        System.out.println("LINES::::::::::::: Lines:" + logFile1.getLineCount());
        if (fLine < 1) fLine = 1;
//		long pos = indexer.filePositionForLine(logFile1.getFileName(), fLine);
//
//		long pos1 = logFile1.getPos();
//		Assert.assertTrue("Not equals", pos != pos1);
//
		long endImport = DateTimeUtils.currentTimeMillis();

        System.out.println("LogFile:" + indexer.openLogFile(this.TEST_FILE));

        long lineStoreSize = indexer.getLineStoreSize();
        System.out.println("Size;" + lineStoreSize);


        System.out.println("IMPORTED(s):" + ((endImport - startImport))/1000);


        String requestString = SEARCH_EXPR;

        LogRequest request = getRequest(new DateTime().minusDays(60).getMillis(), new DateTime().getMillis(), requestString, doSummary);

		
		ArrayList<HistoEvent> histo = makeHisto(request, request.query(0), aggSpace, request.getBucketCount());
		List<Long> elapsed = new ArrayList<Long>();

        long[] longs = FileUtil.countLines(log);
        System.out.println("Lines:" + longs[1]);
        int limit = 100;

        for (int i = 0; i < limit; i++) {
            Scanner searcher = getScanner(request);
			long start = DateTimeUtils.currentTimeMillis();

            System.out.println(" -------------------------- SEARCH ------------------------------------:" + i);
            int hits = searcher.search(request, histo, new AtomicInteger());

			request = getRequest(new DateTime().minusDays(60).getMillis(), new DateTime().getMillis(), requestString.replace("write","read"), doSummary);

            long end = DateTimeUtils.currentTimeMillis();
			
			elapsed.add((end - start));
            //Thread.sleep(100);
			System.out.println(i + ") ******* Search hits:" + hits + " Scanned:" + searcher.eventsComplete() + " elapsedMs:" + (end - start) + " avg:" + (getAvg(elapsed)));

			request.setSubmittedTime(DateTimeUtils.currentTimeMillis());

			Bucket summaryBucket = aggSpace.getSummaryBucket();
			//System.out.println(summaryBucket.functions.keySet());

			SummaryBucket summaryBucket1 = (SummaryBucket) summaryBucket;
			Map oo = summaryBucket1.functionsMap.get("_filename").get("_filename").getResults();
			System.out.println(summaryBucket.hashCode() + " " + oo.values());


			printThroughputRate(log.getAbsolutePath(), getAvg(elapsed), searcher.eventsComplete());
            //if (doSummary) printBucket(aggSpace.summaryBucket);
            List<Map<String, Bucket>> aggregatedHistogram = aggSpace.getAggregatedHistogram(request);
            printBuckets(aggregatedHistogram);


            aggSpace.reset();

			request = getRequest(request.getStartTimeMs(), request.getEndTimeMs(), request.queries().get(0).sourceQuery(), doSummary);


            System.out.println("Finished...");

		}

	}

    private void printBuckets(List<Map<String, Bucket>> aggregatedHistogram) {
        int i = 0;
        for (Map.Entry<String, Bucket> bucket : aggSpace.buckets.entrySet()) {
//            for (Bucket b : bucket.value()) {
                System.out.println(i++ + ")");
                printBucket(bucket.getValue());
//            }

        }


//        for (Map<String, Bucket> stringBucketMap : aggregatedHistogram) {
//            for (Bucket bucket : stringBucketMap.values()) {
//                System.out.println(i++ + ")");
//                printBucket(bucket);
//            }
//        }

//        printBucket(aggregatedHistogram.get(0).values().iterator().next());
    }

    public static void main(String[] args) {
        System.out.println("Usage TEST data/filename.log EXPR \n ///// { EXPR = *&|&_filename.count()} ");
        LogFileSearchIntegrationTest test = new LogFileSearchIntegrationTest();
        File file = new File(args[0]);

//        String FILENAME = "agent.log";
//        String DIR = "/WORK/LOGSCAPE/TRUNK/LogScape/master/build-TEST/logscape/work";
//        String TEST_FILE = DIR + "/" + FILENAME;

        test.DIR = file.getParent();
        test.FILENAME = file.getName();
        test.TEST_FILE = file.getAbsolutePath();
        test.SEARCH_EXPR = "*";
        if (args.length > 1) test.SEARCH_EXPR = args[1].replace("&"," ");

        try {
            System.out.println("Searching File: " + file.getCanonicalPath() + " EXPR: " + test.SEARCH_EXPR);
            test.setUp();
            test.testShouldSearch();
            test.tearDown();
            System.exit(-1);
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }


    }


	private void printThroughputRate(String absolutePath, long elapsed, long eventsScanned) {
		long length = new File(absolutePath).length();
		double seconds = elapsed/1000.0;
		long mbytes = length/FileUtil.MEGABYTES;
		System.out.println(String.format("Throughput:%f MB/sec %f events/Sec", (mbytes/seconds), eventsScanned / (elapsed / 1000.0)));
	}
    public static com.sun.management.OperatingSystemMXBean os = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

	private Scanner getScanner(LogRequest request) {
        SummaryBucketHandler summaryHandler = null;

            System.out.println("Using Single-Core-Scanner cores:" + os.getAvailableProcessors());
            return new FileScanner(indexer, log.getAbsolutePath(), Handlers.getSearchHandlers(logFile, request,aggSpace), "", "", aggSpace);
	}
	
	private long getAvg(List<Long> elapsedS) {
        List<Long> elapsed = elapsedS;
        if (elapsed.size() > 6) {
            elapsed = elapsed.subList(elapsed.size()-4, elapsed.size());
        }
		long total = 0;
		for (Long long1 : elapsed) {
			total += long1;
		}
		return total/elapsed.size();
	}

	private LogRequest getRequest(long from, long to, String expression, boolean summary) {
		LogRequest logRequest = new LogRequestBuilder().getLogRequest("SUBSCRIBER", Arrays.asList(expression), "", from, to);
		if (summary) logRequest.createSummaryBucket(null);
		return logRequest;
	}

	private void printBucket(Bucket bucket) {


		bucket.convertFuncResults(false);

		Map<String, Map> results = bucket.getAggregateResults();
        System.out.println("BUCKET >>>>>>>>>> BUCKET:" + bucket.toStringTime() + " " +  results.size() + " hits:" + bucket.hits());
		for (String mapKey : results.keySet()) {
			Map map = results.get(mapKey);
			//System.out.println("key:" + mapKey);
			Set<String> keySet = map.keySet();
			for (String key2 : keySet) {
//                if (!key2.contains("_tag")) continue;
				Object object = map.get(key2);
                    if (object instanceof HyperLogLog) {
                        HyperLogLog hl = (HyperLogLog) object;
                        long cardinality = hl.cardinality();
                        System.out.println("\t" + key2 + "\t:" + cardinality);
                    } else {
                    System.out.println("\t" + key2 + "\t:" + object);
                    }

			}
		}
	}

	private ArrayList<HistoEvent> makeHisto(LogRequest request, Query query, AggSpace aggSpace, int bucketCount) {
		HistoManager histoAssembler2 = new HistoManager();
		List<Map<String, Bucket>> newHistogram = histoAssembler2.newHistogram(new DateTime(request.getStartTimeMs()), new DateTime(request.getEndTimeMs()), request.getBucketCount(), request.queries(), "SUB", "HOST", "SRC");
		ArrayList<HistoEvent> results = new ArrayList<HistoEvent>();
		for (Map<String,Bucket> map : newHistogram) {
			for (Bucket bucket : map.values()) {
				HistoEvent event = new HistoEvent(request, 0, bucket.getStart(), bucket.getEnd(), map.values().size(), "HOSTS", "","SUB", query, aggSpace, false);
				results.add(event);
				//System.out.println("Event:" + event);
			}
			
		}
		return results;
		
	}

	private void guessFieldSet() throws IOException {
		if (fieldSet == null) {
			fieldSet = new FieldSetGuesser2().guess(Arrays.toStringArray(FileUtil.readLines(log.getAbsolutePath(), 20)));
			fieldSet.id = "TEST-FS";
			fieldSet.addSynthField("rx", "data", "(d)"	, "avg()", true, false);
			System.out.println("FieldSetExpression:" + fieldSet.expression);
			fieldSet.priority = 100;
			System.out.println("REGEX:" + SimpleQueryConvertor.convertSimpleToRegExp(fieldSet.expression));
			String test = FieldSetUtil.test(fieldSet, Arrays.toStringArray(FileUtil.readLines(log.getAbsolutePath(), 2)));
			System.out.println("test:" + test);
		}
	}



}
