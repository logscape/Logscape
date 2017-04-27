package com.liquidlabs.log;

import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.indexer.persistit.PIIndexer;
import com.liquidlabs.log.reader.LogReaderFactoryForIngester;
import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.Query;
import com.liquidlabs.log.search.ReplayEvent;
import com.liquidlabs.log.space.*;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.space.VSpaceProperties;
import com.liquidlabs.transport.proxy.ProxyFactoryImpl;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.ServiceInfo;
import com.liquidlabs.vso.resource.ResourceSpace;
import com.logscape.disco.indexer.persistit.PersisitDbFactory;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class AgentLogServiceTest {

    private String DB_DIR = "build/" + this.getClass().getSimpleName();

    Mockery context = new Mockery();

    private LookupSpace lookupSpace;
    private ORMapperFactory factory;
    private LogSpace logSpace;
    private MyEventListener myEventListener;
    private FileOutputStream out;
    private FileOutputStream out2;
    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(5);
    private ProxyFactoryImpl proxyFactory;
    String format = "yyyy-MM-dd HH:mm:ss";
    private Indexer indexer;
    private File myLog;
    private String timeFormat;
    private AggSpaceImpl aggSpace;
    List<Bucket> events = new ArrayList<Bucket>();
    private File myOtherLog;
    private AgentLogServiceImpl agentLogService;
    private long fromTimeMs;
    private long toTimeMs2;

    private String hosts;

    private LogReaderFactoryForIngester logReaderFactory;

    private ResourceSpace resourceSpace;

    private int maxAgeDays = 9999;

    private boolean isDW;

    private String breakRule;
    private Thread.UncaughtExceptionHandler uncaughtExceptionHandler;
    private boolean systemFieldsEnabled;


    @Before
    public void setUp() throws Exception {
        DB_DIR = "build/" + this.getClass().getSimpleName() + System.currentTimeMillis();
        com.liquidlabs.common.concurrent.ExecutorService.setTestMode();
        setupSoLogSpaceCanRunProperly();
        LogProperties.setAddWatchFileDelay(1);
        LogProperties.setTestDebugMode();
        System.setProperty("log.sync.data.delay.secs", "1");


        lookupSpace = context.mock(LookupSpace.class);
        context.checking(new Expectations() {{
            atLeast(1).of(lookupSpace).registerService(with(any(ServiceInfo.class)), with(any(long.class)));
            will(returnValue("lease"));
            atLeast(1).of(lookupSpace).unregisterService(with(any(ServiceInfo.class)));
            atLeast(0).of(lookupSpace).renewLease(with(any(String.class)), with(any(int.class)));
            will(returnValue(true));
        }}
        );

        factory = new ORMapperFactory();
        proxyFactory = factory.getProxyFactory();
        setupAggSpace();
        logReaderFactory = new LogReaderFactoryForIngester(aggSpace, proxyFactory);

        SpaceServiceImpl config = new SpaceServiceImpl(lookupSpace, factory, "CONFIG", proxyFactory.getScheduler(), false, false, false);
        SpaceServiceImpl log = new SpaceServiceImpl(lookupSpace, factory, LogSpace.NAME, proxyFactory.getScheduler(), false, false, true);
        logSpace = new LogSpaceImpl(config, log, null, aggSpace, null, new String[]{".*filter1.*", ".*filter2.*"}, new String[0], resourceSpace, lookupSpace);
        logSpace.start();

        removeAllDataSources();

        myEventListener = new MyEventListener();

        logSpace.registerWatchListener("foo", myEventListener, "myHost");
        aggSpace.registerEventListener(myEventListener, myEventListener.getId(), null, -1);
        new File("build/foo").mkdirs();
        myLog = new File("build/my.log");
        myLog.deleteOnExit();
        out = new FileOutputStream(myLog);
        myOtherLog = new File("build/foo/myother.log");
        myOtherLog.deleteOnExit();
        out2 = new FileOutputStream(myOtherLog);

        FileUtil.deleteDir(new File(DB_DIR));

        indexer = new PIIndexer(DB_DIR);

        fromTimeMs = new DateTime().minusHours(1).getMillis();
        toTimeMs2 = new DateTime().getMillis();
        uncaughtExceptionHandler = new Thread.UncaughtExceptionHandler() {
            public void uncaughtException(Thread thread, Throwable throwable) {

            }
        };

    }

    private void removeAllDataSources() {
        List<WatchDirectory> watchDirectoriesWithFilter =  logSpace.watchDirectories(null, "", false);
        for (WatchDirectory watchDirectory : watchDirectoriesWithFilter) {
            logSpace.removeWatch(watchDirectory.id());
        }
    }

    private void setupAggSpace() {
        SpaceServiceImpl bucketService = new SpaceServiceImpl(lookupSpace, factory, "AGGSPACE", proxyFactory.getScheduler(), false, false, false);
        SpaceServiceImpl replayService = new SpaceServiceImpl(lookupSpace, factory, "AGGSPACE", proxyFactory.getScheduler(), false, false, false);
        SpaceServiceImpl logEventSpaceService = new SpaceServiceImpl(lookupSpace, factory, "AGGSPACE", proxyFactory.getScheduler(), false, false, true);

        aggSpace = new AggSpaceImpl("providerId", bucketService, replayService, logEventSpaceService, proxyFactory.getScheduler());
        aggSpace.start();
    }

    @After
    public void tearDown() throws Exception {
        try {
            System.out.println("============================================ TEARDOWN >>>>>>>>>>>>>");
            indexer.close();


            executor.shutdown();
            logSpace.stop();
            factory.stop();
            out.close();
            out2.close();
            myLog.delete();
            myOtherLog.delete();
            if (agentLogService != null) agentLogService.stop();

            FileUtil.deleteDir(new File(DB_DIR));
            PersisitDbFactory.close();

            System.out.println("============================================ TEARDOWN <<<<<<<<<<<<<<<<");
            Thread.sleep(3 * 1000);

        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            FileUtil.deleteDir(new File(DB_DIR));
        }
    }

    @Test
    public void shouldSeeOldFilesAsOldFiles() throws Exception {
        try {
            startLogService();
//    	long lastModified = new File("/Volumes/Media/logs/weblogs/liquidlabs-cloud.com-Feb-2010").lastModified();
//    	DateTime dateTime = new DateTime(lastModified);
//    	System.out.println(dateTime);
//    	assertTrue(agentLogService.isAnOldFile(lastModified));
            assertTrue(LogProperties.isAnOldFile(new DateTime().minusDays(2).getMillis()));
            assertFalse(LogProperties.isAnOldFile(new DateTime().minusHours(1).getMillis()));
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }


    @Test
    public void testShouldRemoveWatch() throws Exception {
        File tempFile = File.createTempFile("watchThis", ".log");
        String name = tempFile.getName().split("\\.")[0];
        myEventListener.expectedEvents(1);
        logSpace.addWatch("", tempFile.getParent(), name, timeFormat, "", hosts, maxAgeDays, "", isDW, breakRule, "acb123", false, systemFieldsEnabled);
        logSpace.setLiveLogFilters(new LogFilters(new String[]{".*doMe.*"}, new String[0]));
        startLogService();

        assertTrue(myEventListener.await(10));
        myEventListener.expectedEvents(1);
        logSpace.removeWatch("acb123");
        assertTrue(myEventListener.await(10));
    }


    @Test
    public void testShouldLiveTailLogs() throws Exception {
        logSpace.addWatch("LIVE_TEST", "build", "*.log", format, "", hosts, maxAgeDays, "", isDW, breakRule, null, false, systemFieldsEnabled);
        startLogService();

        System.out.println("*********************************** \n\n");

        out.write("ShouldTailMe foo bar foo bar 1\n".getBytes());
        out2.write("ShouldTailMe THIS IS A MESSAGE 1\n".getBytes());

        pause();
        pause();
        pause();
        pause();

        logSpace.setLiveLogFilters(new LogFilters(new String[]{"ShouldTailMe"}, new String[0]));

        out.write("ShouldTailMe bar foo bar foo 2\n".getBytes());
        out2.write("ShouldTailMe THIS IS ANOTHER MESSAGE 2\n".getBytes());

        out.write("ShouldTailMe bar foo bar foo 3\n".getBytes());
        out2.write("ShouldTailMe THIS IS ANOTHER MESSAGE 3\n".getBytes());
        pause();
        pause();
        // the first line written to each file is ignored
        assertEquals(2, myEventListener.liveEvents);
    }

    private void startLogService() {
        agentLogService = new AgentLogServiceImpl(logSpace, aggSpace, executor, proxyFactory, indexer, "resourceId", logReaderFactory, uncaughtExceptionHandler, lookupSpace);
        agentLogService.start();
    }


    @Test
    public void testShouldAddWatchAndListenToLIVEEvents() throws Exception {
        System.out.println(new DateTime() + " Starting test:testShouldAddWatchAndListenToLIVEEvents");
        startLogService();
        LogProperties.setTestDebugMode();

        myEventListener.expectedEvents(2);
        logSpace.setLiveLogFilters(new LogFilters(new String[]{".*pick.*"}, new String[0]));
        File tempFile = File.createTempFile("watchThis", ".log");
        tempFile.deleteOnExit();
        logSpace.addWatch("", tempFile.getParent(), tempFile.getName(), timeFormat, "", hosts, maxAgeDays, "", isDW, breakRule, "watchId", false, systemFieldsEnabled);

        System.out.println(new DateTime() + " WATCH ADDED>>>>>>>>>>>>>>>>>>" + tempFile.getName());

        PrintWriter printWriter = new PrintWriter(tempFile);
        printWriter.println("pick nothing cause its the first line and part of the import stage");
        printWriter.flush();
        pause();
        printWriter.println("pick this stuff up please?");
        printWriter.println("And pick this up!");
        printWriter.flush();
        printWriter.close();

        assertTrue(new DateTime() + " Listener only got event count:" + myEventListener.liveEvents, myEventListener.await(10));

    }

    @Test
    public void testShouldNotRaiseAnyReplayEvents() throws Exception {
        logSpace.addWatch("", "build", ".*\\.log", format, "", hosts, maxAgeDays, "", isDW, breakRule, null, false, systemFieldsEnabled);
        writeStuffToOutFile();
        startLogService();

        LogRequest request = new LogRequest("subscriber", fromTimeMs, toTimeMs2);

        request.addQuery(new Query(0, ".*third.*"));
        MyReplayHandler replayHandler = new MyReplayHandler();
        aggSpace.replay(request, replayHandler.getId(), replayHandler);
        Thread.sleep(500);
        assertEquals(0, replayHandler.events);
    }


    private void writeStuffToOutFile() throws Exception {
        long start = toTimeMs2 - (30 * 60 * 1000);
        indexer.open(myLog.getAbsolutePath(), true, FieldSets.get().getId(), "sourceTags");

        indexer.add(myLog.getAbsolutePath(), 0, start, 0);
        Thread.sleep(20);
        byte[] bytes = "This is the first line\n".getBytes();
        out.write(bytes);
        indexer.add(myLog.getAbsolutePath(), 1, start + 60001, bytes.length);
        byte[] bytes2 = "This is the second line\n".getBytes();
        int length = bytes.length + bytes2.length;
        out.write(bytes2);
        indexer.add(myLog.getAbsolutePath(), 2, start + 120001, length);
        byte[] bytes3 = "This is the third line\n".getBytes();
        out.write(bytes3);
        length += bytes3.length;
        indexer.add(myLog.getAbsolutePath(), 3, start + 180001, length);
        byte[] bytes4 = "This is the fourth line\n".getBytes();
        out.write(bytes4);
        length += bytes4.length;
        indexer.add(myLog.getAbsolutePath(), 4, start + 240001, length);
        out.write("This is the fifth line\n".getBytes());
        out.flush();
    }

    private void pause() throws InterruptedException {
        Thread.sleep(2000);
    }

    public static class MyEventListener implements LogEventListener, LogConfigListener {
        int liveEvents;
        private CountDownLatch latch;

        public void expectedEvents(int count) {
            latch = new CountDownLatch(count);
        }

        public String getId() {
            return "my-listener";
        }

        public void setFilters(LogFilters filters) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        public void removeWatch(WatchDirectory watch) {
            if (latch !=null) {
                latch.countDown();
            }
        }

        public void addWatch(WatchDirectory watch) {
            if (latch != null) {
                latch.countDown();
            }

        }

        public void updateWatch(WatchDirectory watch) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        public void handle(LogEvent event) {
            System.out.println("\n" + new DateTime() + " MyEventListener:" + event.getFilename() + ":" + event.getMessage());
            liveEvents++;
            if (latch != null) {
                latch.countDown();
            }
        }

        public boolean await(int seconds) {
            try {
                return latch.await(seconds, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                return false;
            }
        }
    }

    public static class MyReplayHandler implements LogReplayHandler {

        int events;
        int bucketHits;
        int replayEvents;
        List<String> replayEventsData = new ArrayList<String>();
        int id = 0;
        private int linenumber;
        private List<Map<String, Bucket>> histo;

        public MyReplayHandler() {
        }

        public MyReplayHandler(int i) {
            id = i;
        }

        public String getId() {
            return "my-handler" + id;
        }

        public void handle(ReplayEvent replayEvent) {
            events++;
            replayEvents++;
            linenumber = replayEvent.getLineNumber();
            replayEventsData.add(replayEvent.getLineNumber() + " " + replayEvent.getRawData() + "\n");
        }

        public void reset() {
            replayEvents = 0;
            replayEventsData.clear();
        }

        public void handle(Bucket event) {
            events++;
            bucketHits += event.hits();
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
            // TODO Auto-generated method stub

        }

    }
    private void setupSoLogSpaceCanRunProperly() {
        VSOProperties.setResourceType("Management");
        new File(VSpaceProperties.baseSpaceDir(), "touch.file").delete();
    }

}
