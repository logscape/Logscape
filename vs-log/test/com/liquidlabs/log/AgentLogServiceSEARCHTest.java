package com.liquidlabs.log;

import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.indexer.krati.KratiIndexer;
import com.liquidlabs.log.reader.LogReaderFactoryForIngester;
import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.ReplayEvent;
import com.liquidlabs.log.space.*;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.space.VSpaceProperties;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.ServiceInfo;
import com.liquidlabs.vso.resource.ResourceSpace;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 */
//@RunWith(LogscapeTestRunner.class)
public class AgentLogServiceSEARCHTest {

    private  final String DB_DIR = "build/" + this.getClass().getSimpleName();

    Mockery context = new Mockery();

    private LookupSpace lookupSpace;
    private ORMapperFactory ormFactory;
    private LogSpace logSpace;
    private MyEventListener myEventListener;
    private FileOutputStream out;
    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(5);
    String format = "yyyy-MM-dd HH:mm:ss";
    DateTimeFormatter formatter = DateTimeFormat.forPattern(format);
    private Indexer indexer;
    private File myLog;
    private AggSpaceImpl aggSpace;
    List<Bucket> events = new ArrayList<Bucket>();
    private AgentLogServiceImpl agentLogService;
    private long fromTimeMs;
    private long toTimeMs2;

    private String hosts;

    private LogReaderFactoryForIngester logReaderFactory;

    private ResourceSpace resourceSpace;

    private int maxAgeDays = 9999;

    private String fileIncludes;


    @Before
    public void setUp() throws Exception {
        System.setProperty("test.mode", "true");
        LogProperties.setAddWatchFileDelay(1);
        System.setProperty("log.sync.data.delay.secs", "1");

        setupSoLogSpaceCanRunProperly();

        FileUtil.deleteDir(new File(DB_DIR));

        // need to tell LogSpcae to add data
        VSOProperties.setResourceType("Management");

        lookupSpace = context.mock(LookupSpace.class);
        context.checking(new Expectations(){{
            atLeast(1).of(lookupSpace).registerService(with(any(ServiceInfo.class)), with(any(long.class))); will(returnValue("lease"));
            atLeast(1).of(lookupSpace).unregisterService(with(any(ServiceInfo.class))); will(returnValue(true));
            atLeast(1).of(lookupSpace).renewLease(with(any(String.class)), with(any(int.class)));
        }}
        );

        ormFactory = new ORMapperFactory();
        setupAggSpace();
        logReaderFactory = new LogReaderFactoryForIngester(aggSpace, ormFactory.getProxyFactory());

        createLogSpace();
        myEventListener = new MyEventListener();

        aggSpace.registerEventListener(myEventListener, myEventListener.getId(), null, -1);

        myLog = new File("build/AGENT_LOG_SEARCH_my.log");
        out = new FileOutputStream(myLog);

        indexer = new KratiIndexer(DB_DIR);

        fromTimeMs = new DateTime().minusHours(1).getMillis();
        toTimeMs2 = new DateTime().plusMinutes(1).getMillis();

        logSpace.addWatch("", "build", ".*AGENT_LOG_SEARCH_my.log", format, "", hosts, maxAgeDays, "", false, "", null, false, true);

    }

    private void createLogSpace() {
        SpaceServiceImpl config = new SpaceServiceImpl(lookupSpace,ormFactory, "CONFIG", ormFactory.getScheduler(), false, false, false);
        SpaceServiceImpl log = new SpaceServiceImpl(lookupSpace,ormFactory, LogSpace.NAME, ormFactory.getScheduler(), false, false, true);
        logSpace = new LogSpaceImpl(config, log, null, aggSpace, null, new String[] { ".*filter1.*", ".*filter2.*"}, new String[0], resourceSpace, lookupSpace);
        logSpace.start();
    }

    private void setupAggSpace() {
        SpaceServiceImpl bucketService = new SpaceServiceImpl(lookupSpace,ormFactory, "AGGSPACE", ormFactory.getScheduler(), false, false, false);
        SpaceServiceImpl replayService = new SpaceServiceImpl(lookupSpace,ormFactory, "AGGSPACE", ormFactory.getScheduler(), false, false, false);
        SpaceServiceImpl logEventSpaceService = new SpaceServiceImpl(lookupSpace,ormFactory, "AGGSPACE", ormFactory.getScheduler(), false, false, true);

        aggSpace = new AggSpaceImpl("providerId", bucketService, replayService, logEventSpaceService, ormFactory.getScheduler());
        aggSpace.start();
    }

    @After
    public void tearDown() throws Exception {
        try {
            System.out.println("============================================ TEARDOWN >>>>>>>>>>>>>");
            executor.shutdown();
            logSpace.stop();
            ormFactory.stop();
            out.close();
            myLog.delete();
            indexer.close();
            if (agentLogService != null) agentLogService.stop();

            System.out.println("============================================ TEARDOWN <<<<<<<<<<<<<<<<");


        } catch(Throwable t) {
            t.printStackTrace();
        } finally {
            FileUtil.deleteDir(new File(DB_DIR));
        }
        // sleep cause shutdown hooks will fire...
        Thread.sleep(3 * 1000);
    }

    @Test
    public void test_FIELDBASED_ShouldReplaySecondAndThirdLine() throws Exception {

        LogProperties.setTestDebugMode();
        startLogService();
        writeStuffToOutFile();

        //TestFieldSet.fieldName = 'data' is used in by contains();

        LogRequest request = new LogRequestBuilder().getLogRequest("SUB-111111111", Arrays.asList("type='basic' | data.contains(second,third)"), "", fromTimeMs, toTimeMs2);
        request.setReplay(new Replay(ReplayType.START, 10));
        request.setSearch(true);
        request.setVerbose(true);

        System.out.println("\n\n========================= SEARCH 1 =====================================\n\n");

        MyReplayHandler replayHandler = new MyReplayHandler();

        aggSpace.search(request, replayHandler.getId(), replayHandler);
        logSpace.executeRequest(request);

        assertEquals("expected 2 replays" , 2, replayHandler.waitForReplayEvents(2));
        pause();
        assertEquals("expected 2 bucket hits", 2, replayHandler.bucketHits);

        System.out.println(replayHandler.replayEventsData);
        System.out.println("\n\n========================= SEARCH 2 =====================================\n\n");
        replayHandler.reset();

        LogRequest request2 = new LogRequestBuilder().getLogRequest("SUB-22222222", Arrays.asList("type='*' | data.contains(third, second)"), "", fromTimeMs, toTimeMs2);
        request2.setReplay(new Replay(ReplayType.START, 10));
        request2.setSearch(true);
        request2.setVerbose(true);


        aggSpace.replay(request2, replayHandler.getId(), replayHandler);
        logSpace.executeRequest(request2);

        pause();
        assertEquals(2, replayHandler.replayEvents);
        replayHandler.reset();

    }

    @Test
    public void test_HYBRID_ShouldReplaySecondAndThirdLine() throws Exception {
        startLogService();
        writeStuffToOutFile();
        pause();

        //TestFieldSet.fieldName = 'data' is used in by contains();

        LogRequest request = new LogRequestBuilder().getLogRequest("SUB-111111111", Arrays.asList("This | data.count()"), "", fromTimeMs, toTimeMs2);
        request.setReplay(new Replay(ReplayType.START, 10));
        request.setSearch(true);
        request.setVerbose(true);

        MyReplayHandler replayHandler = new MyReplayHandler();

        aggSpace.search(request, replayHandler.getId(), replayHandler);
        logSpace.executeRequest(request);

        pause();
        printHisto(replayHandler.histoBuckets);
        assertTrue(replayHandler.bucketHits > 4);

        System.out.println(replayHandler.replayEventsData);
        System.out.println("\n\n===================================================");
        replayHandler.reset();

        LogRequest request2 = new LogRequestBuilder().getLogRequest("SUB-22222222", Arrays.asList("This | data.contains(third, second)"), "", fromTimeMs, toTimeMs2);
        request2.setReplay(new Replay(ReplayType.START, 10));
        request2.setSearch(true);
        request2.setVerbose(true);


        aggSpace.replay(request2, replayHandler.getId(), replayHandler);
        logSpace.executeRequest(request2);

        pause();
        assertEquals("expected 2 replays" , 2, replayHandler.waitForReplayEvents(2));
        System.out.println(replayHandler.replayEventsData);
        System.out.println("\n\n===================================================");
        replayHandler.reset();

    }
    private void printHisto(List<Bucket> histo) {
        for (Bucket bucket : histo) {
            System.out.println("B:" + bucket);
        }
    }

    @Test
    public void test_OLDSCHOOL_ShouldReplaySecondAndThirdLine() throws Exception {

        startLogService();
        writeStuffToOutFile();
        pause();

        LogRequest request = new LogRequestBuilder().getLogRequest("OLD-111111111", Arrays.asList("* | contains(second,third)"), "", fromTimeMs, toTimeMs2);
        request.setReplay(new Replay(ReplayType.START, 10));
        request.setSearch(true);
        request.setVerbose(true);

        MyReplayHandler replayHandler = new MyReplayHandler(2);

        aggSpace.search(request, replayHandler.getId(), replayHandler);
        logSpace.executeRequest(request);

        pause();
        assertEquals(2, replayHandler.replayEvents);
        assertEquals(2, replayHandler.bucketHits);

        replayHandler.reset();

        LogRequest request2 = new LogRequestBuilder().getLogRequest("OLD-22222222", Arrays.asList("* | contains(third, second)"), "", fromTimeMs, toTimeMs2);
        request2.setReplay(new Replay(ReplayType.START, 10));
        request2.setSearch(true);
        request2.setVerbose(true);


        aggSpace.replay(request2, replayHandler.getId(), replayHandler);
        logSpace.executeRequest(request2);

        pause();
        assertEquals("expected 2 replays" , 2, replayHandler.waitForReplayEvents(2));
        replayHandler.reset();
    }


    private void startLogService() {
        Thread.UncaughtExceptionHandler exceptionHandler = new Thread.UncaughtExceptionHandler() {
            public void uncaughtException(Thread thread, Throwable throwable) {

            }
        };
        agentLogService = new AgentLogServiceImpl(logSpace, aggSpace, executor, ormFactory.getProxyFactory(), indexer, "resourceId", logReaderFactory, exceptionHandler, lookupSpace);
        agentLogService.start();
    }




    private void writeStuffToOutFile() throws Exception {
        out.write("This is the first line\n".getBytes());
        out.write("This is the second line\n".getBytes());
        out.write("This is the third line\n".getBytes());
        out.write("This is the fourth line\n".getBytes());
        out.write("This is the fifth line\n".getBytes());
        out.flush();

        int waitCount = 0;
        while (!indexer.isIndexed(myLog.getAbsolutePath())&& waitCount++ < 100) {
            Thread.sleep(500);
        }
    }

    private void pause() throws InterruptedException {
        Thread.sleep(2000);
    }

    public static class MyEventListener implements LogEventListener {
        int events;
        public String getId() {
            return "my-listener";
        }

        public void handle(LogEvent event) {
            System.out.println("MyEventListener:" + event.getFilename() + ":" + event.getMessage());
            events++;
        }
    }

    public static class MyReplayHandler implements LogReplayHandler {

        volatile int events;
        volatile int bucketHits;
        volatile int replayEvents;
        List<String> replayEventsData = new ArrayList<String>();
        int id =0;
        private int linenumber;
        List<Bucket> histoBuckets = new ArrayList<Bucket>();
        private List<Map<String, Bucket>> histo;
        private CountDownLatch replayLatch;

        public MyReplayHandler() {}

        public int waitForReplayEvents(int waitCount) {
            System.out.println("Waiting for Replays:" + waitCount);
            replayLatch = new CountDownLatch(waitCount);
            try {
                boolean awaitGood = replayLatch.await(10, TimeUnit.SECONDS);
                if (awaitGood) System.out.println("Got all ReplayEvents!");
                else System.out.println("Failed to get all ReplayEvents");
            } catch (InterruptedException e) {
            }
            return this.replayEvents;
        }

        public MyReplayHandler(int i) {
            id = i;
        }
        public String getId() {
            return "my-TEST-Handler"+id;
        }

        public void handle(ReplayEvent replayEvent) {
            try {
                if (replayLatch != null) replayLatch.countDown();
                events++;
                replayEvents++;
                linenumber = replayEvent.getLineNumber();
                replayEventsData.add(replayEvent.getLineNumber() + " " + replayEvent.getRawData() + "\n");
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
        public void reset() {
            replayEvents = 0;
            replayEventsData.clear();
        }

        public void handle(Bucket event) {
            events++;
            bucketHits+= event.hits();
            histoBuckets.add(event);
        }
        public int handle(String providerId, String subscriber, int size, Map<String, Object> map) {
            List<Map<String, Bucket>> histo = (List<Map<String, Bucket>>) map.get("histo");
            events++;
            for (Map<String, Bucket> map2 : histo) {
                Collection<Bucket> values = map2.values();
                for (Bucket bucket : values) {
                    bucketHits += bucket.hits();
                }
            }
            return 1;
        }

        public int handle(List<ReplayEvent> events) {
            for (ReplayEvent replayEvent : events) {
                handle(replayEvent);
            }
            return 100;
        }

        public String toString() {
            StringBuffer buffer = new StringBuffer();
            buffer.append("[MyReplayHandler:");
            buffer.append(" events:");
            buffer.append(events);
            buffer.append(" bucketHits:");
            buffer.append(bucketHits);
            buffer.append(" replayEvents:");
            buffer.append(replayEvents);
            buffer.append(" id:");
            buffer.append(id);
            buffer.append(" linenumber:");
            buffer.append(linenumber);
            buffer.append(" histo:");
            buffer.append(histo);
            buffer.append("]");
            return buffer.toString();
        }

        public int status(String provider, String subscriber, String msg) {
            return 1;
        }

        public void handleSummary(Bucket bucketToSend) {
        }

    }
    private void setupSoLogSpaceCanRunProperly() {
        VSOProperties.setResourceType("Management");
        new File(VSpaceProperties.baseSpaceDir(), "touch.file").delete();
    }


}
