package com.liquidlabs.log.search;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.TestModeSetter;
import com.liquidlabs.common.UID;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.log.*;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.indexer.krati.KratiIndexer;
import com.liquidlabs.log.reader.LocalIndexerIngestHandler;
import com.liquidlabs.log.reader.GenericIngester;
import com.liquidlabs.log.search.filters.Contains;
import com.liquidlabs.log.search.filters.LessThan;
import com.liquidlabs.log.search.filters.Not;
import com.liquidlabs.log.search.handlers.SearchHistogramHandler;
import com.liquidlabs.log.space.LogRequest;
import com.liquidlabs.log.space.WatchDirectory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NonIndexedSearchRealTest {

    NullAggSpace aggSpace;
    NullLogSpace logSpace;
    private File log;
    PrintWriter pw;
    LogReader writer;
    TailerImpl tailer;
    private Thread writerThread;

    protected Indexer indexer;
    protected List<HistoEvent> histo;

    protected AtomicLong indexedDataSize = new AtomicLong();
    protected int limit = Integer.MAX_VALUE;
    protected AtomicInteger sent = new AtomicInteger();
    protected boolean dwEnabled;
    private FieldSet fieldSet;

    static volatile int testNum = 0;
    static ReentrantLock reentrantLock = new ReentrantLock();

    @Before
    public void setUp() throws Exception {
        TestModeSetter.setTestMode();
        reentrantLock.lock();

        try {
            logSpace = new NullLogSpace();
            aggSpace = new NullAggSpace();
            log = new File("build/NonIndexedSearchRealTestDefaultFile.log");
            log.delete();

            pw = new PrintWriter(log);
            setupIndexer();

            fieldSet = new FieldSet("(s28) (**)", "timestamp", "data");
            fieldSet.id = "testFieldSet";
            fieldSet.addSynthField("memAvail", "data", "AVAIL:(d)","count(*)", true, true);
            fieldSet.addSynthField("hostname", "data", "AGENT (*)","count(*)", true, true);
            indexer.addFieldSet(fieldSet);

            WatchDirectory watch = new WatchDirectory("tag", "./build","NonIndexedSearchRealTestDefaultFile.log","","", 999, "", false,"Default", null, false, false);
            LogFile logFile = indexer.openLogFile(log.getAbsolutePath(), true, fieldSet.id, "");
//			logFile.setNewLineRule(watch.getBreakRule());


            writerThread = createWriter(pw);
            writerThread.start();
            Thread.sleep(1000);

            writer = new GenericIngester(logFile, indexer, new LocalIndexerIngestHandler(indexer), "sourceURI", "hostname", watch);
            tailer = new TailerImpl(log,0, 0,writer, watch, indexer);

            histo = new ArrayList<HistoEvent>();
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    protected void setupIndexer() {
        indexer = new KratiIndexer("build/LogFileScannerRealTest" + testNum++);
        // Persistit and Lucene can be a PIG when testing
        //disco = new PIIndexer("build/LogFileScannerRealTest" + testNum++);
    }

    @After
    public void tearDown() throws Exception {
        writerThread.interrupt();
        writerThread.join(5000);
        if (writerThread.isAlive()) {
            System.out.println("Writer Thread still alive!");
            writerThread.interrupt();
        }
        indexer.close();
        reentrantLock.unlock();
    }

    @Test
    public void shouldMatchMultilineQuery() throws Exception {
        File f = new File("build/NonIndexedSearchRealTest-MultiLine.log");
        PrintWriter printWriter = new PrintWriter(f);
        for (int i=1 ; i<=10; i++) {
            printWriter.println(DateUtil.log4jFormat.print(DateTimeUtils.currentTimeMillis()) + " Line = " + i);
        }

        // write the multiline part
        printWriter.print(DateUtil.log4jFormat.print(DateTimeUtils.currentTimeMillis()) + " This line has some left overs and it is ");
        printWriter.println(" line = 11");
        printWriter.println(" line = 12");
        printWriter.println(" line = 13");
        printWriter.println(" line = 14");
        printWriter.println(DateUtil.log4jFormat.print(DateTimeUtils.currentTimeMillis()) + " Line = 15");
        printWriter.flush();

        fieldSet = new FieldSet("(s23) (**)", "timestamp", "data");
        fieldSet.id = "testFieldSet";
        indexer.addFieldSet(fieldSet);
        LogFile logFile = indexer.openLogFile(f.getAbsolutePath(), true, fieldSet.id, "");

        WatchDirectory watch = new WatchDirectory();
        watch.setBreakRule("Year");

        writer = new GenericIngester(logFile, indexer, new LocalIndexerIngestHandler(indexer), "sourceURI", "hostname", watch);

        TailerImpl theTailer = new TailerImpl(f, 0,0, writer, watch, indexer);

        theTailer.call();
        System.out.println("2:" + theTailer);

        LogRequest request = getRequest(new DateTime().minusMinutes(2).getMillis(),new DateTime().plusMinutes(2).getMillis(), "line = 12");
        createScanner(request, f, indexer).search(request, histo, sent);

        assertEquals("No AggEvents", 1, aggSpace.buckets.size());
        assertEquals("No Hits", 1, aggSpace.buckets.values().iterator().next().hits());
    }
    @Test
    public void shouldMatchRealMLineQuery() throws Exception {
//		LogProperties.setTestDebugMode();
        File f = new File("test-data/mline-smaller.log");

        fieldSet = new FieldSet("(s23) (**)", "timestamp", "data");
        fieldSet.id = "testFieldSet";
        indexer.addFieldSet(fieldSet);
        LogFile logFile = indexer.openLogFile(f.getAbsolutePath(), true, fieldSet.id, "");
        logFile.setNewLineRule("Year");
        indexer.updateLogFile(logFile);

        WatchDirectory watch = new WatchDirectory();
        watch.setMaxAge(9999);
        watch.setBreakRule("Year");
        writer = new GenericIngester(logFile, indexer, new LocalIndexerIngestHandler(indexer), "sourceURI", "hostname", watch);


        TailerImpl theTailer = new TailerImpl(f, 0,0, writer, watch, indexer);

        theTailer.call();
        LogFile openLogFile = indexer.openLogFile(f.getAbsolutePath());

        LogRequest request = getRequest(openLogFile.getStartTime(),openLogFile.getEndTime(), "Exception");
        createScanner(request, f, indexer).search(request, histo, sent);

        assertEquals("No AggEvents", 1, aggSpace.buckets.size());
        assertEquals("No Hits", 3, aggSpace.buckets.values().iterator().next().hits());
    }


    @Test
    public void testShouldHaveSameNumberHitsMatchingLinesInSameFileButDifferentFilter() throws Exception {
        Thread.sleep(1000);
        for (int i = 0; i < 5; i++) {
            tailer.call();
            assertMemLinePartsHitsEqual();
            Thread.sleep(1000);
        }
    }

    @Test
    public void testShouldHaveSameNumberOfCPUAndMemHits() throws Exception {
        Thread.sleep(1000);
        for (int i = 0; i < 5; i++) {
            tailer.call();
            assertCPUAndMemHitsEqual();
            Thread.sleep(1000);
        }
    }

    @Test
    public void testShouldObeyHitLimit() throws Exception {
        File f = new File("build/NonIndexedSearchRealTest-HitLimit.log");
        PrintWriter printWriter = new PrintWriter(f);
        for (int i=1 ; i<=50; i++) {
            printWriter.println(new Date() + " Line = " + i);
        }
        printWriter.flush();

        /**
         * Setup the field and watch etc.
         */
        FieldSet fieldSet = new FieldSet("(s28) (**)", "timestamp", "data");
        fieldSet.id = this.fieldSet.id;
        indexer.addFieldSet(fieldSet);
        LogFile logFile = indexer.openLogFile(f.getAbsolutePath(), true, fieldSet.id, "");

        WatchDirectory watch = new WatchDirectory();

        writer = new GenericIngester(logFile, indexer, new LocalIndexerIngestHandler(indexer), "sourceURI", "hostname", watch);

        TailerImpl theTailer = new TailerImpl(f, 0,0, writer, watch, indexer);
        theTailer.call();

//		LogRequest request = getRequest(0, System.currentTimeMillis(), "*");
        LogRequest request = new LogRequestBuilder().getLogRequest("sub", Arrays.asList("* | hitLimit(3)"), "", 0, System.currentTimeMillis());
        setupHisto(request);

        request.setVerbose(true);
        request.setBucketCount(10);

        // hitlimiters etc only get initialised when a copy is made
        request = request.copy();
        createScanner(request, f, indexer).search(request, histo, sent);

        assertEquals("hits:" + histo.get(0).hits(), 3, histo.get(0).hits());
    }

    /**
     * [NEIL - Log deleted?]
     * @throws Exception
     */
    @Test
    public void testShouldApplyLessThanWithResults() throws Exception {
        Thread.sleep(1000);
        tailer.call();
        String memPattern = ".*AGENT (.+)-\\d+-\\d+ MEM.+AVAIL:(\\d+).*";
        LogRequest request = new LogRequest("foo", 0, System.currentTimeMillis());
        Query query = new Query(0, 0, memPattern, memPattern, true);
        query.addFilter(new LessThan("AvailMemLessThan10", "memAvail", 90));
        request.addQuery(query);

        setupHisto(request);

        createScanner(request, log, indexer).search(request, histo, sent);
        assertTrue("Should have got Buckets, but size was:" + aggSpace.buckets.size(), aggSpace.buckets.size() > 0);
    }


    @Test
    public void testShouldTrySomethingElseToMakeItBreak() throws Exception {
        // allow some data to get written to the log
        Thread.sleep(1000);
        for (int i = 0; i < 5; i++) {
            tailer.call();
            String cpu = ".*AGENT.*CPU.*";
            String agentP = ".*AGENT.*";
            String memP = ".*MEM.*";

            LogRequest request = getRequest(0, System.currentTimeMillis(),  cpu, agentP, memP);
            createScanner(request, log, indexer).search(request, histo, sent);
            long agentCpu = getHitCount(0);
            long agent = getHitCount(1);
            long mem = getHitCount(2);
            System.out.printf("%s-> hits: %d\n", cpu, agentCpu);
            System.out.printf("%s-> hits: %d\n", agentP, agent);
            System.out.printf("%s-> hits: %d\n", memP, mem);
            assertEquals(agentCpu, mem);
            assertEquals(agentCpu * 2, agent);
            Thread.sleep(1000);
        }
        assertCPUAndMemHitsEqual();
    }

    @Test
    public void testShouldDoSomething() throws Exception {
        Thread.sleep(1000);
        tailer.call();
        aggSpace.buckets.clear();
        LogRequest request = getRequest(0, System.currentTimeMillis(),
                ".*AGENT.*CPU.*",
                ".*AGENT.*"
        );
        createScanner(request, log, indexer).search(request, histo, sent);
        long agentCpu = getHitCount(0);
        long agent = getHitCount(1);
        System.out.println("Got:" + agentCpu + " and " + agent);
        assertTrue(agent > 0);
        assertEquals(agentCpu * 2, agent);
    }
    @Test
    public void testShouldDoSomething2() throws Exception {
        Thread.sleep(5000);
        tailer.call();
        aggSpace.buckets.clear();
        LogRequest request = getRequest(0, System.currentTimeMillis(),
                ".*AGENT.*CPU.*",
                ".*AGENT.*"
        );

        createScanner(request, log, indexer).search(request, histo, sent);
        long agentCpu = getHitCount(0);
        System.out.println("HITS:" + agentCpu);
        int cpu = countEventsOfType(0);
        int mem = countEventsOfType(1);
        System.out.printf("cpu events = %d, mem events = %d\n", cpu, mem);
        assertEquals("Number of events should be equal", cpu, mem);
    }

    private int countEventsOfType(int index) {
        int count = 0;
        for(ReplayEvent event : aggSpace.replayEvents){
            if (event.getQuerySourceIndex() == index) {
                count++;
            }
        }
        return count;
    }
    @Test
    public void testShouldDoSomeRandomSearches() throws Exception {
        String [][] filters = {
                {".*AGENT.*"},
                {".*AGENT.*MEM.*"},
                {".*AGENT.*CPU.*"},
                {".*AGENT.*CPU.*", ".*AGENT.*MEM.*"},
                {".*AGENT.*CPU.*", ".*AGENT.*"},
                {".*AGENT.*CPU.*", ".*AGENT.*", ".*AGENT.*MEM.*"}
        };

        for (int i = 0; i < filters.length; i++) {
            tailer.call();
            LogRequest request = getRequest(0, System.currentTimeMillis(), filters[i]);
            createScanner(request, log, indexer).search(request, histo, sent);
            Thread.sleep(1000);
        }
        aggSpace.replayEvents.clear();
        tailer.call();
        LogRequest request = getRequest(0, System.currentTimeMillis(), filters[0] );
        createScanner(request, log, indexer).search(request, histo, sent);
        for (ReplayEvent event : aggSpace.replayEvents) {
            String line = event.getFieldValues(fieldSet)[1].split("=")[1];
            assertEquals("Event Line did not match for:" + event, event.getLineNumber(), Integer.valueOf(line));
        }
        assertCPUAndMemHitsEqual();
    }


    @Test
    public void testShouldApplyLessThanOnScan() throws Exception {
        Thread.sleep(1000);
        tailer.call();
//		AGENT alteredcarbon.local-12050-0 MEM MB MAX:92 COMMITED:32 USED:23 AVAIL:68 - line=
        String memPattern = ".*AGENT (.*) MEM.*AVAIL:(\\d+) .*";
        LogRequest request = new LogRequest("foo", new DateTime().minusMinutes(2).getMillis(), new DateTime().getMillis());
        Query query = new Query(0, 0, memPattern, memPattern, true);
        query.addFilter(new LessThan("AvailMemLessThan10", "memAvail", 100));
        request.addQuery(query);
        createScanner(request, log, indexer).search(request, histo, sent);
        assertEquals(1, aggSpace.buckets.size());
    }
    @Test
    public void testShouldApplyNot() throws Exception {
        Thread.sleep(1000);
        writerThread.interrupt();
        writerThread.join(5000);
        tailer.call();
        LogRequest request = new LogRequest("foo", 0, System.currentTimeMillis());
        request.setVerbose(true);
        //, memPattern);
        Query query = new Query(0,  "*");
        query.addFilter(new Not("tag", "hostname", "alteredcarbon"));
        request.addQuery(query);
        createScanner(request, log, indexer).search(request, histo, sent);
        // there will
        System.out.println("EngineLines" + engineLines);
        System.out.println("AgentLines:" + agentLines);

        assertTrue(aggSpace.buckets.size() > 0);
        assertEquals(engineLines, aggSpace.buckets.values().iterator().next().hits());
    }

    @Test
    public void testShouldHaveCorrectLineNumbers() throws Exception {

        File otherLog = new File("test-data/my.log");

        FieldSet fieldSet = new FieldSet("(**)","data");
        fieldSet.id = UID.getUUID();
        indexer.addFieldSet(fieldSet);
        LogFile openLogFile = indexer.openLogFile(otherLog.getAbsolutePath(), true, fieldSet.id, "");



        WatchDirectory watch = new WatchDirectory();
        watch.setMaxAge(99999);

        writer = new GenericIngester(openLogFile, indexer, new LocalIndexerIngestHandler(indexer), "sourceURI", "hostname", watch);

        TailerImpl tailer = new TailerImpl(otherLog, 0,0, writer, watch, indexer);
        tailer.call();


        LogRequest request = new LogRequest("foo", 0, System.currentTimeMillis());
        Query query = new Query(0, 0, "*", "*",true);
        query.addFilter(new Contains("","data",".*line.*"));
        request.addQuery(query);
        request.setSummaryRequired(true);
        request.createSummaryBucket(null);
        request.setVerbose(true);
        setupHisto(request);

        createScanner(request, otherLog, indexer).search(request, histo, sent);
        assertEquals(1000, getHitCount(0));
    }


    void assertMemLinePartsHitsEqual() throws IOException {
        aggSpace.buckets.clear();
        // String memPattern = ".*AGENT (.+)-\\d+-\\d+ MEM.+AVAIL:(\\d+).*";
        // pw.println("AGENT alteredcarbon.local-12050-0 MEM MB MAX:92 COMMITED:32 USED:23 AVAIL:68 - line=" + i++);
        String pattern1 = ".*AGENT (.+)-\\d+-\\d+ MEM.*COMMITED:(\\d+).*";
        String pattern2 = ".*AGENT (.+)-\\d+-\\d+ MEM.*USED:(\\d+).*";
        LogRequest request = getRequest(0, System.currentTimeMillis(), pattern1, pattern2);
        createScanner(request, log, indexer).search(request, histo, sent);
        long hitCount1 = getHitCount(0);
        long hitCount2 = getHitCount(1);
        System.out.printf(" %s-> hits: %d\n", pattern1, hitCount1);
        System.out.printf(" %s-> hits: %d\n", pattern2, hitCount2);
        assertEquals(hitCount1, hitCount2);
    }

    private void assertCPUAndMemHitsEqual() throws IOException {
        aggSpace.buckets.clear();
        String cpu = ".*AGENT.*CPU.*";
        String mem = ".*AGENT.*MEM.*";
        LogRequest request = getRequest(0, System.currentTimeMillis(), cpu, mem);
        createScanner(request, log, indexer).search(request, histo, sent);
        long cpuHits = getHitCount(0);
        long memHits = getHitCount(1);
        System.out.printf("%s-> hits: %d\n", cpu, cpuHits);
        System.out.printf("%s-> hits: %d\n", mem, memHits);
        assertEquals(cpuHits, memHits);
    }

    private long getHitCount(int matchIndex) {
        long hits = 0;
        for(Bucket bucket : aggSpace.buckets()) {
            if (bucket.getQueryPos() == matchIndex){
                hits += bucket.hits();
            }
        }
        return hits;
    }

    protected Scanner createScanner(LogRequest request, File file, Indexer indexer) throws IOException {
        setupHisto(request);
        return new FileScanner(indexer, file.getAbsolutePath(), new SearchHistogramHandler(null, new LogFile("filename",1, "basic","tag"), request), "","", aggSpace);
    }


    protected LogRequest getRequest(long from, long to, String... patterns) {
        LogRequest request = new LogRequest("sub", from, to);
        int i = 0;
        for (String filter : patterns) {
            Query query = new Query(i++, i, filter, filter, false);
            request.addQuery(query);
        }

        request.setVerbose(true);

        setupHisto(request);
        return request;
    }

    protected void setupHisto(LogRequest request) {
        this.histo = new ArrayList<HistoEvent>();
        for (Query query : request.queries()) {
            histo.add(new HistoEvent(request, 0, 0, DateTimeUtils.currentTimeMillis(), 1, "hostname", "endPoint", request.subscriber(), query, aggSpace, false));
        }
    }


    int agentLines = 0;
    int engineLines = 0;

    private Thread createWriter(final PrintWriter pw) {
        Thread writerThread = new Thread(new Runnable() {
            public void run() {
                agentLines = 0;
                engineLines = 0;
                boolean interrupted = false;
                int i = 1;
                int memAvail = 68;
                Random rand = new Random();
                while(!Thread.currentThread().isInterrupted() && !interrupted) {
                    if (i % 5 == 0) {
                        pw.print(new Date() + " Engine line = " + i++);
                        pw.println("\n\tEngine line = " + i++);
                        engineLines++;
                        pw.flush();
                    } else {
                        if (i % 2 == 0) {
                            memAvail = 0;
                        } else {
                            memAvail = 68;
                        }
                        pw.println(new Date() + String.format(" AGENT alteredcarbon.local-12050-0 MEM MB MAX:92 COMMITED:32 USED:23 AVAIL:%d - line=%d", memAvail, i++));
                        pw.println(new Date() + " AGENT alteredcarbon.local-12050 CPU:6.000000 - line=" + i);
                        pw.flush();
                        agentLines += 2;
                    }
                    try {
                        Thread.sleep(rand.nextInt(500));
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
                System.out.println("Writer Thread is Terminating term:" + interrupted);
            }});

        writerThread.setDaemon(true);
        return writerThread;
    }

}
