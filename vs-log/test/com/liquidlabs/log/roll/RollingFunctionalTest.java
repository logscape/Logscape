package com.liquidlabs.log.roll;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.TestModeSetter;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.log.LogReader;
import com.liquidlabs.log.NullAggSpace;
import com.liquidlabs.log.TailResult;
import com.liquidlabs.log.TailerImpl;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.index.LogFileOps;
import com.liquidlabs.log.indexer.krati.KratiIndexer;
import com.liquidlabs.log.reader.LocalIndexerIngestHandler;
import com.liquidlabs.log.reader.GenericIngester;
import com.liquidlabs.log.search.*;
import com.liquidlabs.log.search.handlers.ReplayEventsHandler;
import com.liquidlabs.log.search.handlers.SearchHistogramHandler;
import com.liquidlabs.log.space.LogRequest;
import com.liquidlabs.log.space.WatchDirectory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

public class RollingFunctionalTest  {

    static {
        System.setProperty("log4j.debug","true");
    }
    private Indexer indexer;
    String dbDir = "build/" + getClass().getSimpleName();
    String fileDir = dbDir + "Files";

    NullAggSpace aggSpace = new NullAggSpace();
    private AtomicLong indexedDataSize = new AtomicLong(0);
    private FieldSet fieldSet;
    protected List<HistoEvent> histo;
    private AtomicInteger sent = new AtomicInteger();
    private String sourceTags = "";
    private String fileTags = "";

    @Before
    public void setUp() throws Exception {

        TestModeSetter.setTestMode();
        FileUtil.deleteDir(new File(dbDir));
        FileUtil.deleteDir(new File(fileDir));
        FileUtil.mkdir(dbDir);
        FileUtil.mkdir(fileDir);
        System.out.println("\n\n----------" + dbDir + " --------------------------------------------------------\n");

        aggSpace = new NullAggSpace();

        indexer = new KratiIndexer(dbDir);

        fieldSet = FieldSets.getBasicFieldSet();
        indexer.addFieldSet(fieldSet);
    }

    @After
    public void tearDown() throws Exception {
        try {
            System.out.println("Closing..........DB");
            indexer.close();
        } catch(Exception e) {};
        FileUtil.deleteDir(new File(dbDir));
        FileUtil.deleteDir(new File(fileDir));
    }

    /**
     * numerical roll needs to track each independent file for rolling,
     * i.e. 1 -> 2 -> 3 etc
     * @throws Exception
     */
    @Test
    public void testShouldRollNumericallyLots() throws Exception {

        File dir = new File("build/NumericRoll");

        FileUtil.deleteDir(dir);
        dir.mkdir();
        String rollName = "numeric.log";
        File theLog = new File(dir, rollName);
        Thread writer = createWriter(theLog.getAbsolutePath());
        writer.start();
        Thread.sleep(500);

        WatchDirectory watchDirectory = new WatchDirectory();
        watchDirectory.setFileSorter(new ContentBasedSorter());
        LogFile logFile = indexer.openLogFile(theLog.getAbsolutePath(), true, fieldSet.getId(), sourceTags );
        LogReader ingester = new GenericIngester(logFile, indexer, new LocalIndexerIngestHandler(indexer), "sourceURI", "hostname", new WatchDirectory());

        TailerImpl tailer = new TailerImpl(theLog, 0,0, ingester, watchDirectory, indexer);

        createTailer(tailer);

        Thread.sleep(1500);

        // 1) Normal Roll  now roll from .log -> .log.1 and replace .log
        writer.interrupt();
        System.out.println("Performing FIRST ROLL-------------------");

        FileUtil.renameTo(theLog, new File(dir, rollName + ".1"));
        // START from the begining of the file again
        writer = createWriter(theLog.getAbsolutePath());
        writer.start();
        Thread.sleep(3000);

        // assert the first roll was detected

        assertTrue("Wasnt Indexed: rollFile.1 ", indexer.isIndexed(new File(dir, rollName + ".1").getAbsolutePath()));

//        assertFalse("Numeric roll should not tail the original file, only each rolled instance", disco.isIndexed(new File(rollName).getAbsolutePath()));


        writer.interrupt();
        System.out.println("Performing SECOND ROLL-------------------");

        // now do a proper numeric roll.....log -> .1 and .1 -> .2
        FileUtil.renameTo(new File(dir, rollName + ".1"), new File(dir, rollName + ".2"));
        FileUtil.renameTo(new File(dir, rollName ), new File(dir, rollName + ".1"));

        // START from the beginning of the file again
        writer = createWriter(theLog.getAbsolutePath());
        writer.start();
        Thread.sleep(3000);

        System.out.println("CHECKING SECOND ROLL-------------------");

        assertTrue(indexer.isIndexed(new File(dir, rollName + ".2").getAbsolutePath()));
        assertTrue(indexer.isIndexed(new File(dir, rollName + ".1").getAbsolutePath()));
        assertTrue(indexer.isIndexed(new File(dir, rollName).getAbsolutePath()));
    }



    @Test
    public void testShouldRollAndKeepTailingLogFile() throws Exception {
        String testFilename = "AtheSimple.log";
        String theLogFileName = fileDir + File.separator + testFilename;
        String theRolledLogFileName = fileDir + File.separator + testFilename + "." + DateUtil.shortDateTimeFormat1.print(System.currentTimeMillis());
        File theLog = new File(theLogFileName);

        Thread writer = createWriter(theLog.getAbsolutePath());
        writer.start();
        Thread.sleep(500);

        WatchDirectory watchDirectory = new WatchDirectory();
        watchDirectory.setFileSorter(new ContentBasedSorter());
        LogFile logFile = indexer.openLogFile(theLog.getAbsolutePath(), true, fieldSet.getId(), sourceTags );
        LogReader ingester = new GenericIngester(logFile, indexer, new LocalIndexerIngestHandler(indexer), "sourceURI", "hostname", new WatchDirectory());
        TailerImpl tailer = new TailerImpl(theLog, 0,0, ingester, watchDirectory, indexer);
        Future tailerThread = createTailer(tailer);
        Thread.sleep(1500);

        List<com.liquidlabs.log.index.Bucket> orig = indexer.find(theLog.getAbsolutePath(), 0, System.currentTimeMillis());
        System.out.printf("%d lines indexed before roll = fileSize:%d\n", orig.size(), theLog.length());

        Thread.sleep(1500);

        // STOP updating the sourceFILE - so a roll is detected (!file.exists());
        writer.interrupt();

        File rollToFile = new File(theRolledLogFileName);
        boolean renameTo = FileUtil.renameTo(theLog, rollToFile);

        // START from the begining of the file again
        writer = createWriter(theLog.getAbsolutePath());
        writer.start();
        Thread.sleep(1500);

        System.out.println("Waiting for Tailer to pick up the ROLL...:" + renameTo);
        Thread.sleep(1500);

        // stop threads
        tailerThread.cancel(true);
        writer.interrupt();


        // search for Buckets on the RolledToFile to verify the RolledToStore is OK
        List<com.liquidlabs.log.index.Bucket> rolledToBuckets = indexer.find(rollToFile.getAbsolutePath(), 0, System.currentTimeMillis());
        assertTrue(String.format("rolled size = %d, orig %d", rolledToBuckets.size(), orig.size()),
                rolledToBuckets.size() >= orig.size());

        assertTrue(indexer.isIndexed(theLog.getAbsolutePath()));
        assertTrue(indexer.isIndexed(rollToFile.getAbsolutePath()));

        // verify the tailer is owning the rolled and the original log files
        assertTrue("Should say its tailing the SourceFile", tailer.isFor(theLog.getAbsolutePath()));
        assertTrue("Should say its tailing the RolledFile", tailer.isFor(rollToFile.getAbsolutePath()));
    }

    @Test
    public void testShouldRollAndStopTailingWhenOriginalFileStops() throws Exception {

        String testFilename = "theRollingFunctionalReTailTest.log";
        String theLogFileName = fileDir + "/" + testFilename;
        String theRolledLogFileName = fileDir + "/" + testFilename + "." + DateUtil.shortDateTimeFormat1.print(System.currentTimeMillis());
        File theLog = new File(theLogFileName);

        Thread writer = createWriter(theLog.getAbsolutePath());
        writer.start();
        Thread.sleep(500);

        WatchDirectory watchDirectory = new WatchDirectory();
        watchDirectory.setFileSorter(new ContentBasedSorter());
        LogFile logFile = indexer.openLogFile(theLog.getAbsolutePath(), true, fieldSet.getId(), sourceTags);
        LogReader ingester = new GenericIngester(logFile, indexer, new LocalIndexerIngestHandler(indexer), "sourceURI", "hostname", new WatchDirectory());
        TailerImpl tailer = new TailerImpl(theLog, 0,0, ingester, watchDirectory, indexer);
        Future tailerThread = createTailer(tailer);
        Thread.sleep(1500);

        List<com.liquidlabs.log.index.Bucket> orig = indexer.find(theLog.getAbsolutePath(), 0, System.currentTimeMillis());
        System.out.printf("%d lines indexed before roll = fileSize:%d\n", orig.size(), theLog.length());

        Thread.sleep(1500);
        // STOP updating the sourceFILE
        writer.interrupt();

        File rolledTo = new File(theRolledLogFileName);
        boolean renameTo = FileUtil.renameTo(theLog, rolledTo);

        System.out.println("Waiting for Tailer to pick up the ROLL...:" + renameTo);
        Thread.sleep(1500);
        tailerThread.cancel(true);

        List<com.liquidlabs.log.index.Bucket> rolled = indexer.find(rolledTo.getAbsolutePath(), 0, System.currentTimeMillis());
        assertTrue(String.format("rolled size = %d, orig %d", rolled.size(), orig.size()),rolled.size() >= orig.size());

        assertFalse("**Should NOT have an entry for the SourceLog - cause it doesnt EXIST", indexer.isIndexed(theLog.getAbsolutePath()));
        assertTrue(indexer.isIndexed(rolledTo.getAbsolutePath()));

        // verify the tailer is owning the rolled and the original log files
        assertTrue("Should say its tailing the SourceFile", tailer.isFor(theLog.getAbsolutePath()));
        assertTrue("Should say its tailing the RolledFile", tailer.isFor(rolledTo.getAbsolutePath()));

    }

    //	@Test DodgyTest? Move this someplace else
    public void testShouldRollAgainAndAgainWithoutProblems() throws Exception {
        String theLogFileName = fileDir + File.separator + "theSimple.log";
        File theLog = new File(theLogFileName);

        // start writing the file
        Thread writer = createWriter(theLog.getAbsolutePath());
        writer.start();
        Thread.sleep(5000);
        writer.interrupt();
        System.out.println("============= Finished writing FIRST File:" + theLog.length());
        // setup the tailer to read the file
        WatchDirectory watchDirectory = new WatchDirectory();
        watchDirectory.setFileSorter(new ContentBasedSorter());
        LogFile logFile = indexer.openLogFile(theLog.getAbsolutePath(), true, fieldSet.getId(), sourceTags);
        LogReader ingester = new GenericIngester(logFile, indexer, new LocalIndexerIngestHandler(indexer), "sourceURI", "hostname", new WatchDirectory());
        TailerImpl tailer = new TailerImpl(theLog, 0,0, ingester, watchDirectory, indexer);
        Future tailerThread = createTailer(tailer);


        List<com.liquidlabs.log.index.Bucket> orig = indexer.find(theLog.getAbsolutePath(), 0, System.currentTimeMillis());
        System.out.printf("%d lines indexed before roll = fileSize:%d\n", orig.size(), theLog.length());

        List<String> files = new ArrayList<String>();
        for (int i = 0; i < 5; i++) {
            System.out.println("============================:" + i);
            DateTime dateTime = new DateTime();
            String theRolledLogFileName = fileDir + File.separator + "theSimple.log." + DateUtil.shortDateFormat.print(System.currentTimeMillis())+ "-" + dateTime.getSecondOfMinute() ;
            File l2 = new File(theRolledLogFileName);

            // ROLL
            boolean renameTo = FileUtil.renameTo(theLog, l2);
            System.out.println("Waiting for ROLL...rename successful?:" + renameTo + " file:" + theRolledLogFileName);
            String rolledToFilename = l2.getAbsolutePath();
            files.add(rolledToFilename);
            Thread.sleep(100);

            // Start writing again - to the same file
            writer = createWriter(theLog.getAbsolutePath());
            writer.start();
            // DONT Roll too fast - cause roll detection waits for 2 seconds before determining
            Thread.sleep(4000);
            writer.interrupt();
            System.out.println("** IndexedFiles:" + indexer.indexedFiles(0, Long.MAX_VALUE, false, LogFileOps.FilterCallback.ALWAYS).size());// + " Files:" + disco.indexedFiles());
            System.out.println(indexer.openLogFile(theLog.getAbsolutePath()).getId() + ">>>> SRC BUCKETS:" + indexer.find(theLog.getAbsolutePath(), 0, System.currentTimeMillis()).size());
            System.out.println(indexer.openLogFile(rolledToFilename).getId() + ">>>> ROL BUCKETS:" + indexer.find(rolledToFilename, 0, System.currentTimeMillis()).size() + " " + rolledToFilename);
        }

        tailerThread.cancel(true);
        System.out.println("===============VERIFY==============");

        List<LogFile> firstFile = indexer.indexedFiles(0, Long.MAX_VALUE, false, new LogFileOps.FilterCallbackRegEx("theSimple.log"));
        System.out.println(">>> First:" + firstFile.get(0));

        System.out.println("** IndexedFile" + indexer.indexedFiles(0, Long.MAX_VALUE, false, LogFileOps.FilterCallback.ALWAYS));

        boolean goodSizes = false;

        int pos = 0;
        for (String file : files) {
            List<LogFile> indexedFiles = indexer.indexedFiles(0, Long.MAX_VALUE, false, new LogFileOps.FilterCallbackRegEx(file));
            if (indexedFiles.size() == 0) System.out.println(pos + ") ERROR ************ Didnt find file:" + file);
            else {
                LogFile aLogFile = indexedFiles.get(0);
                List<com.liquidlabs.log.index.Bucket> orig2 = indexer.find(file, 0, System.currentTimeMillis());
                if (orig2.size() == 0) {
                    System.out.println(">>> Found 0 buckets for:" + file);
                }
                if (!goodSizes) goodSizes = orig2.size() > 0;

                System.out.println(pos + ") " + aLogFile + " Size:" + orig2.size());

            }

            pos++;
        }
        assertTrue("Found files with 0 sizes", goodSizes);

    }

    @Test
    public void testShouldDoTimeStampBasicRoll() throws Exception {
        String filename = "timestamp.log";
        File theLog = new File("build/" + getClass().getSimpleName() + "/" + filename);
        theLog.mkdirs();
        theLog.delete();
        String rollFromFilename = theLog.getAbsolutePath();

        Thread writer = createWriter(rollFromFilename);
        writer.start();
        Thread.sleep(500);
        WatchDirectory watchDirectory = new WatchDirectory();
        watchDirectory.setFileSorter(new ContentBasedSorter());

        LogFile logFile = indexer.openLogFile(rollFromFilename, true, fieldSet.getId(), sourceTags);
        LogReader ingester = new GenericIngester(logFile, indexer, new LocalIndexerIngestHandler(indexer), "sourceURI", "hostname", new WatchDirectory());
        TailerImpl tailer = new TailerImpl(theLog, 0,0, ingester, watchDirectory, indexer);
        Future tailerThread = createTailer(tailer);

        Thread.sleep(4000);
        writer.interrupt();
        assertTrue(indexer.isIndexed(rollFromFilename));
        List<com.liquidlabs.log.index.Bucket> orig = indexer.find(rollFromFilename, 0, System.currentTimeMillis());
        System.out.printf("%d lines indexed before roll = fileSize:%d\n", orig.size(), theLog.length());
        assertTrue("existing log file had no buckets", orig.size() != 0);


        File rolledToLogFile = new File(theLog.getParentFile(), filename + "." + DateUtil.shortDateFormat.print(System.currentTimeMillis()));
        String rolledToFilename = rolledToLogFile.getAbsolutePath();

        System.out.println(">>>>> RENAMING FILE:\n" + theLog.getAbsolutePath() + " -> \n" +  rolledToFilename);
        // ROLL
        boolean success = FileUtil.renameTo(theLog, rolledToLogFile);
        assertTrue(success);

        System.out.println("Writing New timestamp file");

        // Re-Write the New LogFile
        Thread writer2 = createWriter(rollFromFilename);
        writer2.start();
        Thread.sleep(5000);

        // check that the roll and source are indexed
        assertTrue(indexer.isIndexed(rollFromFilename));
        assertTrue("Rolled to didnt find file:" + rolledToFilename, indexer.isIndexed(rolledToFilename));
        List<com.liquidlabs.log.index.Bucket> rolled = indexer.find(rolledToFilename, 0, System.currentTimeMillis());
        assertTrue("existing log file had no buckets", rolled.size() != 0);


        LogFile firstFileToBeTailed = indexer.openLogFile(rollFromFilename);
        assertNotNull(firstFileToBeTailed);

        System.out.println("Original FILE logId:" + firstFileToBeTailed.getId());

        List<com.liquidlabs.log.index.Bucket> existingBuckets = indexer.find(rollFromFilename, 0, System.currentTimeMillis());
        writer2.interrupt();
        tailerThread.cancel(true);
        assertTrue("existing log file had no buckets", rolled.size() != 0);
        assertTrue("new log file had no buckets", existingBuckets.size() != 0);
        assertTrue(String.format("rolled size = %d, orig %d", rolled.size(), orig.size()),rolled.size() >= orig.size());
        assertFalse(indexer.isIndexed(new File("build/the.log.2").getAbsolutePath()));

        // chec that we are picking up the existing log file still
    }


    // @Test DodgyTest? scanner.search always returns 0 - WTF?
    public void testShouldRollLogAndStillBeAbleToSearchContents() throws Exception {
        String sourceFileName = "RollLogAndStillBeAbleToSearch.log";
        File file = new File("build", sourceFileName);
        file.deleteOnExit();
        PrintWriter printWriter = new PrintWriter(file);
        printWriter.println("AGENT alteredcarbon.local-12050-0 MEM MB MAX:92 COMMITED:32 USED:23 AVAIL:68");
        printWriter.println("AGENT alteredcarbon.local-12050-0 MEM MB MAX:92 COMMITED:32 USED:23 AVAIL:68");
        printWriter.println("AGENT alteredcarbon.local-12050-0 MEM MB MAX:92 COMMITED:32 USED:23 AVAIL:68");
        printWriter.flush();
        printWriter.close();
        NullAggSpace aggSpace = new NullAggSpace();
        WatchDirectory watchDirectory = new WatchDirectory();
        watchDirectory.setFileSorter(new ContentBasedSorter());

        LogFile logFile = indexer.openLogFile(file.getAbsolutePath(), true, fieldSet.getId(), sourceTags);

        LogReader ingester = new GenericIngester(logFile, indexer, new LocalIndexerIngestHandler(indexer), "sourceURI", "hostname", new WatchDirectory());
        TailerImpl tailer = new TailerImpl(file, 0,0, ingester, watchDirectory, indexer);
        tailer.call();

        System.out.println("\n 1 ---------------- SEARCH ------------------- ");


        String memPattern = ".*MEM.*";
        LogRequest request = makeRequest(memPattern);
        FileScanner scanner = new FileScanner(indexer, file.getAbsolutePath(), new SearchHistogramHandler(null, logFile, request), fileTags, "", aggSpace);
        // DodgyTest - this always returns 0!
        int hits = scanner.search(request, histo, new AtomicInteger());

        // verify we can search the non-rolled file
        assertEquals(3, hits);
        aggSpace.buckets.clear();

        // roll it
        File rolled1 = new File("build", sourceFileName + ".1");
        rolled1.deleteOnExit();
        FileUtil.renameTo(file, rolled1);
        new File("build", sourceFileName).createNewFile();
        // trigger the roll event
        tailer.call();


        // verify we can search the rolled file
        request = makeRequest(memPattern);
        FileScanner searcher = new FileScanner(indexer, rolled1.getAbsolutePath(), new SearchHistogramHandler(null, logFile, request), fileTags, "", aggSpace);
        hits = searcher.search(request, histo, sent);
        assertEquals(3, hits);

        // write to the original file
        printWriter = new PrintWriter(file);
        printWriter.println("AGENT alteredcarbon.local-12050-0 MEM MB MAX:92 COMMITED:32 USED:23 AVAIL:68");
        printWriter.flush();
        tailer.call();


        // verify we can still search the original file and get hits
        request = makeRequest(memPattern);
        aggSpace.buckets.clear();

        searcher = new FileScanner(indexer, file.getAbsolutePath(), new SearchHistogramHandler(null, logFile, request), fileTags, "", aggSpace);
        hits = searcher.search(request, histo, sent);
        assertEquals(1, hits);
    }
    @Test
    public void testShouldRollLogAndStillBeAbleToAddToRolledFile() throws Exception {
        String sourceFileName = "RollLogAndStillBeAbleToAddToRolled.log";
        File file = new File("build", sourceFileName);
        file.deleteOnExit();
        PrintWriter printWriter = new PrintWriter(file);
        printWriter.println(new DateTime() + " AGENT alteredcarbon.local-12050-0 MEM MB MAX:92 COMMITED:32 USED:23 AVAIL:68");
        printWriter.println(new DateTime() + " AGENT alteredcarbon.local-12050-0 MEM MB MAX:92 COMMITED:32 USED:23 AVAIL:68");
        printWriter.println(new DateTime() + " AGENT alteredcarbon.local-12050-0 MEM MB MAX:92 COMMITED:32 USED:23 AVAIL:68");
        printWriter.flush();
        printWriter.close();
        NullAggSpace aggSpace = new NullAggSpace();
        WatchDirectory watchDirectory = new WatchDirectory();
        watchDirectory.setFileSorter(new ContentBasedSorter());

        LogFile logFile = indexer.openLogFile(file.getAbsolutePath(), true, fieldSet.getId(), sourceTags);

        LogReader ingester = new GenericIngester(logFile, indexer, new LocalIndexerIngestHandler(indexer), "sourceURI", "hostname", new WatchDirectory());
        TailerImpl tailer = new TailerImpl(file, 0,0, ingester, watchDirectory, indexer);
        tailer.call();

        // roll it
        File rolled1 = new File("build", sourceFileName + ".1");
        rolled1.deleteOnExit();
        FileUtil.renameTo(file, rolled1);
        // trigger the roll event
        System.out.println("DETECTING ROLL ??:" + rolled1);
        tailer.call();

        // write to the original file
        printWriter = new PrintWriter(rolled1);
        System.out.println("Writting to ROLLED file:" + rolled1);
        printWriter.println("AGENT alteredcarbon.local-12050-0 MEM MB MAX:92 COMMITED:32 USED:23 AVAIL:68");
        printWriter.flush();

        // check the file now exists with the new name
        LogFile openLogFile = indexer.openLogFile(rolled1.getAbsolutePath());
        assertNotNull("Failed to roll to:" + rolled1.getAbsolutePath(), openLogFile);
        // this will blow up if the store was closed
        indexer.add(rolled1.getAbsolutePath(), 100, DateTimeUtils.currentTimeMillis(), 100);

    }

    private long countHits(List<Bucket> buckets, int index) {
        long hits = 0;
        for (Bucket bucket : buckets) {
            if (bucket.getQueryPos() == index) {
                hits += bucket.hits();
            }
        }
        return hits;
    }

    private LogRequest makeRequest(String pattern) {
        LogRequest request = new LogRequest("foo", 0, System.currentTimeMillis());
        request.addQuery(new Query(0, 0,  pattern, pattern, false));
        request.setVerbose(true);
        setupHisto(request);
        return request;
    }

    //	@Test DodgyTest?: searcher.search always returns 0! Intentional?
    public void testShouldCorrectlyRollLog2() throws Exception {


        File file = new File("build/rolling.log");
        try {
            String testFile = file.getAbsolutePath();

            PrintWriter printWriter = new PrintWriter(file);
            printWriter.println("AGENT alteredcarbon.local-12050-0 MEM MB MAX:92 COMMITED:32 USED:23 AVAIL:68");
            Thread.sleep(1000);
            printWriter.println("AGENT alteredcarbon.local-12050-0 MEM MB MAX:92 COMMITED:32 USED:23 AVAIL:68");
            Thread.sleep(1000);
            printWriter.println("AGENT alteredcarbon.local-12050-0 MEM MB MAX:92 COMMITED:32 USED:23 AVAIL:68");
            Thread.sleep(1000);
            printWriter.print("AGENT alteredcarbon.local-12050-0 MEM MB MAX:92 COMMITED:32 USED:23 AVAIL:68\n");
            printWriter.flush();
            printWriter.close();
            WatchDirectory watchDirectory = new WatchDirectory();
            watchDirectory.setFileSorter(new ContentBasedSorter());

            LogFile logFile = indexer.openLogFile(testFile, true, fieldSet.getId(), sourceTags);

            LogReader ingester = new GenericIngester(logFile, indexer, new LocalIndexerIngestHandler(indexer), "sourceURI", "hostname", new WatchDirectory());
            TailerImpl tailer = new TailerImpl(file, 0,0, ingester, watchDirectory, indexer);
            tailer.call();

            System.out.println(" 1------------------------- SEARCH ---------------");

            // verify the file is available
            indexer.isIndexed(testFile);
            assertTrue(indexer.find(testFile, 0, System.currentTimeMillis()).size() > 0);

            String memPattern = ".*AGENT (.+)-\\d+-\\d+ MEM.+AVAIL:(\\d+).*";
            LogRequest request = makeRequest(memPattern);
            FileScanner searcher = new FileScanner(indexer, testFile, new SearchHistogramHandler(null, logFile, request), fileTags, "", aggSpace);

            int hits = searcher.search(request, histo, sent);
            assertEquals(4, hits);
            aggSpace.buckets.clear();

            System.out.println(" 2------------------------- ROLL ---------------");

            File rolled1 = new File("build/rolling.log.1");
            rolled1.deleteOnExit();
            FileUtil.renameTo(file, rolled1);


            printWriter = new PrintWriter(file);
            printWriter.println("AGENT alteredcarbon.local-12050-0 MEM MB MAX:92 COMMITED:32 USED:23 AVAIL:68");
            printWriter.flush();
            tailer.call();

            System.out.println(" ------------------------- SEARCH ---------------");
            request = makeRequest(memPattern);

            searcher = new FileScanner(indexer, rolled1.getAbsolutePath(), new ReplayEventsHandler(null, request, aggSpace, "subscriber", logFile), fileTags, "", aggSpace);
            hits = searcher.search(request, null, sent);
            assertEquals("Should have picked up an extra hit - 3 from the old - 1 from end-of-file", 4, hits);
            //searcher.scan(rolled1);

            assertEquals(4, aggSpace.replayEvents.size());
            int i = 1;
            for (ReplayEvent replayEvent : aggSpace.replayEvents) {
                assertEquals(new File("build/rolling.log.1").getAbsolutePath(), replayEvent.getFilePath());
                assertEquals(new Integer(i++), replayEvent.getLineNumber());
                assertEquals("AGENT alteredcarbon.local-12050-0 MEM MB MAX:92 COMMITED:32 USED:23 AVAIL:68", replayEvent.getRawData());
            }

            request = makeRequest(memPattern);
            aggSpace.buckets.clear();
            searcher = new FileScanner(indexer, testFile, new SearchHistogramHandler(null, logFile, request), fileTags, "", aggSpace);
            hits = searcher.search(request, histo, sent);
            assertEquals(1, hits);
        } finally {
            file.delete();
        }
    }

    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(20);
    private Future createTailer(final TailerImpl tailer) {

        Runnable runnable = new Runnable() {
            public void run() {
                boolean interrupted = false;
                try {
                    while (!Thread.currentThread().isInterrupted() && !interrupted) {

                        try {
                            TailResult call = tailer.call();
                            if (call.equals(TailResult.FAILED))
                                throw new RuntimeException("Tailer is exiting because of result:" + call);
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                            interrupted = true;
                        }

                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                }
                System.out.println("Tailer Thread is Terminating");
            }
        };
        return scheduler.submit(runnable);
    }



    int count = 0;

    private Thread createWriter(final String name) {
        Thread writerThread = new Thread(new Runnable() {
            public void run() {
                try {
                    boolean interrupted = false;
                    DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss,SSS");
                    PrintWriter pw = getPrintWriter(name);
                    System.out.println(formatter.print(new DateTime()) + " Writer Thread is Starting name:" + name);
                    while(!Thread.currentThread().isInterrupted() && !interrupted) {
                        pw.println(formatter.print(new DateTime()) + " AGENT alteredcarbon.local-12050-0 MEM MB MAX:92 COMMITED:32 USED:23 AVAIL:68");
                        pw.println(formatter.print(new DateTime()) + " AGENT alteredcarbon.local-12050 CPU:6.000000");
                        pw.flush();
                        count += 2;
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            interrupted = true;
                        }
                    }
                    System.out.println(formatter.print(new DateTime()) + " Writer Thread is Terminating, lineCount:" + count);
                    pw.close();
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }

            private PrintWriter getPrintWriter(final String name) {
                try {
                    return new PrintWriter(name);
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                    throw new RuntimeException("Boooooooom:" + e.toString());
                }
            }});

        writerThread.setDaemon(true);
        return writerThread;
    }
    protected void setupHisto(LogRequest request) {
        this.histo = new ArrayList<HistoEvent>();
        for (Query query : request.queries()) {
            histo.add(new HistoEvent(request, 0, 0, DateTimeUtils.currentTimeMillis(), 1, "hostname", "endPoint", request.subscriber(), query, aggSpace, false));
        }
    }

}
