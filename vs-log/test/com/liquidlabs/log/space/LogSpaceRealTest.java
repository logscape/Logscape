package com.liquidlabs.log.space;

import com.liquidlabs.common.UID;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.roll.ContentBasedSorter;
import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.Query;
import com.liquidlabs.log.search.ReplayEvent;
import com.liquidlabs.log.search.TimeUID;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.space.VSpaceProperties;
import com.liquidlabs.space.lease.Lease;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.resource.ResourceGroup;
import com.liquidlabs.vso.resource.ResourceSpace;
import org.jmock.Mock;
import org.jmock.MockObjectTestCase;
import org.joda.time.DateTimeUtils;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;


public class LogSpaceRealTest extends MockObjectTestCase {

    private LogEvent info;
    private LogEvent warning;
    private LogEvent error;
    private Mock lookupSpace;
    private LogSpaceImpl logSpace;
    private ORMapperFactory factory;
    private long toTimeMs = System.currentTimeMillis();
    private AggSpaceImpl aggSpace;
    private String hosts;
    private SpaceServiceImpl logEventSpace;
    private Mock resourceSpace;

    @Override
    protected void setUp() throws Exception {
        com.liquidlabs.common.concurrent.ExecutorService.setTestMode();
        new File(VSpaceProperties.baseSpaceDir(), "touch.file").delete();
        System.setProperty(Lease.PROPERTY, "1");
        System.setProperty("vso.resource.type", "Management");
        VSpaceProperties.setBaseSpaceDir("build/space");
        new File(VSpaceProperties.baseSpaceDir(), "touch.file").delete();

        info = new LogEvent("sourceURI", "INFO this is a message", "host", "file", 1, 1, "");
        warning = new LogEvent("sourceURI", "WARNING this is a warning message", "host", "file", 2, 2, "");
        error = new LogEvent("sourceURI", "ERROR this is an error message", "host", "file", 3, 3, "");
        lookupSpace = mock(LookupSpace.class);
        lookupSpace.stubs();
        resourceSpace = mock(ResourceSpace.class);
        resourceSpace.stubs();
        resourceSpace.stubs().method("expandGroupIntoHostnames").withAnyArguments().will(returnValue(new HashSet<String>()));

        lookupSpace.stubs().method("registerService").withAnyArguments().will(returnValue("xxxx"));
        lookupSpace.stubs().method("unregisterService").withAnyArguments().will(returnValue(true));
        factory = new ORMapperFactory();
        aggSpace = new AggSpaceImpl("providerId",
                new SpaceServiceImpl((LookupSpace) lookupSpace.proxy(), factory, "BUCKET" + LogSpace.NAME, factory.getProxyFactory().getScheduler(), false, false, false),
                new SpaceServiceImpl((LookupSpace) lookupSpace.proxy(), factory, "REPLAY" + LogSpace.NAME, factory.getProxyFactory().getScheduler(), false, false, false),
                new SpaceServiceImpl((LookupSpace) lookupSpace.proxy(), factory, "REPLAY" + LogSpace.NAME, factory.getProxyFactory().getScheduler(), false, false, true),
                factory.getProxyFactory().getScheduler());
        aggSpace.start();
        logEventSpace = new SpaceServiceImpl((LookupSpace) lookupSpace.proxy(), factory, LogSpace.NAME, factory.getProxyFactory().getScheduler(), false, false, true);
        logSpace = new LogSpaceImpl(logEventSpace, logEventSpace, null, aggSpace, null, new String[]{"ERROR"}, new String[]{"crap"}, (ResourceSpace) resourceSpace.proxy(), (LookupSpace) lookupSpace.proxy());
        logSpace.start();
    }

    @Override
    protected void tearDown() throws Exception {
        new File(VSpaceProperties.baseSpaceDir(), "touch.file").delete();
        System.setProperty("vso.resource.type", "Agent");
        logSpace.stop();
        aggSpace.stop();
        factory.stop();
    }
    List<WatchDirectory> addedWatches = new ArrayList<WatchDirectory>();

    public void testShouldupgradeOK() throws Exception {
        Search upgradedSearch = new Search("test", "test", Arrays.asList("type='log4j'"), "tag:tag1,tag:tag2,*.log", Arrays.asList(1), 100,"");
        logSpace.saveSearch(upgradedSearch, null);
        Search search = logSpace.getSearch("test", null);
        assertTrue(search.patternFilter.get(0).contains("_tag.equals(tag1,tag2)"));
    }

    @Test
    public void testShouldUpdateWatchAgainstGroup() throws Exception {

        logSpace.registerWatchListener("xx", new LogConfigListener() {
            public void addWatch(WatchDirectory watch) {
                addedWatches.add(watch);
            }
            public String getId() {
                return "xx";
            }
            public void removeWatch(WatchDirectory watch) {
            }

            public void setFilters(LogFilters filters) {
            }

            public void updateWatch(WatchDirectory watch) {
                addedWatches.add(watch);
            }
        }, "");
        logSpace.addWatch("x", "/", "*", "", ContentBasedSorter.class.getCanonicalName(), "group:abcde", 7, "", true, "", null, false, true);
        Thread.sleep(100);

        logSpace.resourceGroupUpdated(null, new ResourceGroup("abcde", "","",""));
        Thread.sleep(100);

        assertEquals(2, addedWatches.size());
    }

    @Test
    public void testShouldGetCorrectWatchRollConvention() throws Exception {
        logSpace.registerWatchListener("xx", new LogConfigListener() {
            public void addWatch(WatchDirectory watch) {
                addedWatches.add(watch);
            }
            public String getId() {
                return "xx";
            }
            public void removeWatch(WatchDirectory watch) {
            }

            public void setFilters(LogFilters filters) {
            }

            public void updateWatch(WatchDirectory watch) {
                addedWatches.add(watch);
            }
        }, "");
        logSpace.addWatch("x", "/", "*", "", ContentBasedSorter.class.getCanonicalName(), "", 7, "", true, "", null, false, true);
        Thread.sleep(100);

        assertEquals("Only expected 1 added Watch - but got:" + addedWatches.size() + " watches:" + this.addedWatches, 1, addedWatches.size());
        assertEquals(ContentBasedSorter.class.getCanonicalName(), addedWatches.get(0).getFileSorter().getClass().getCanonicalName());

    }

    @Test
    public void testFieldSetLastModifiedIsSet() throws Exception {
        FieldSet fs = FieldSets.getIIS6();
        fs.lastModified =  System.currentTimeMillis();
        logSpace.saveFieldSet(fs);
        FieldSet fieldSet = logSpace.getFieldSet(fs.getId());
        assertTrue(fieldSet.lastModified > 0);
    }


    @Test
    public void testShouldSaveFieldSetWithHatChar() {
        FieldSet basicFieldSet = FieldSets.getBasicFieldSet();
        basicFieldSet.addSynthField("hat", "data", "groovy-script: return data.split(\"^foo\")","count()", true, true);

        logSpace.saveFieldSet(basicFieldSet);

        FieldSet fieldSet = logSpace.getFieldSet(basicFieldSet.getId());
        assertEquals(2, fieldSet.getFields().size());
    }

    public void testShouldStoreFilters() throws Exception {
        LogFilters readLiveFilters = logSpace.readLiveFilters();
        assertNotNull(readLiveFilters);

    }


    public void testShouldOnlyRecieveErrorLevel() throws Exception {
        MyEventListener eventListener = writeEvents("message contains ERROR");
        Thread.sleep(100);
        assertEquals(1, eventListener.received.size());
        assertEquals(error.getId(), eventListener.received.get(0));
    }

    public void testShouldReceiveWarningMessages() throws Exception {
        MyEventListener eventListener = writeEvents("message contains ERROR");
        Thread.sleep(100);
        assertEquals(1, eventListener.received.size());
        assertTrue(eventListener.received.contains(error.getId()));
    }

    public void testShouldReceiveAllEvents() throws Exception {
        MyEventListener eventListener = writeEvents(null);
        Thread.sleep(100);
        assertEquals(3, eventListener.received.size());
        assertTrue(eventListener.received.contains(warning.getId()));
        assertTrue(eventListener.received.contains(error.getId()));
        assertTrue(eventListener.received.contains(info.getId()));
    }


    public void testShouldNotReceiveEventsWhenUnregistered() throws Exception {
        MyEventListener eventListener = new MyEventListener();
        aggSpace.registerEventListener(eventListener, eventListener.getId(), "message contains INFO", -1);
        aggSpace.unregisterEventListener(eventListener.getId());
        aggSpace.write(info);
        Thread.sleep(100);
        assertEquals(0, eventListener.received.size());
    }


    public void testShouldReceiveReplayEvents() throws Exception {
        MyReplayListener myReplayListener = new MyReplayListener();
        LogRequest replayRequest = makeRequest(".*INFO.*");
        replayRequest.setVerbose(true);
        aggSpace.replay(replayRequest, myReplayListener.getId(), myReplayListener);
        System.out.println("Writing REPLAY events:" + replayRequest.subscriber());
        writeReplayEvents(replayRequest.subscriber());
        Thread.sleep(2000);
        assertEquals(3, myReplayListener.received.size());
    }

    private LogRequest makeRequest(String pattern) {
        LogRequest replayRequest = new LogRequest("subscriber" + UID.getUUIDWithTime(), 0, toTimeMs);
        replayRequest.addQuery(new Query(0, pattern));
        return replayRequest;
    }

    public void testShouldNotReceiveReplayEventsForOtherRequests() throws Exception {
        MyReplayListener myReplayListener = new MyReplayListener();
        LogRequest replayRequest = makeRequest(".*INFO.*");
        aggSpace.replay(replayRequest, myReplayListener.getId(), myReplayListener);
        writeReplayEvents("8989898");
        Thread.sleep(100);
        assertEquals(0, myReplayListener.received.size());
    }

    public void testShouldOnlyStoreObjectsUntilLeaseExpiry() throws Exception {
        aggSpace.write(new LogEvent("uri", "messg", "host", "file", 8, 4, ""));
        aggSpace.write(new LogEvent("uri", "messg", "host", "file", 8, 4, ""));
        aggSpace.write(new LogEvent("uri", "messg", "host", "file", 8, 4, ""));
        aggSpace.write(new LogEvent("uri", "messg", "host", "file", 8, 4, ""));
        aggSpace.write(new LogEvent("uri", "messg", "host", "file", 8, 4, ""));
        Thread.sleep(10 * 1000);
        assertEquals(0, logSpace.size());

    }

    private void writeReplayEvents(String requestId) {
        aggSpace.write(new ReplayEvent("sourceURI", 0, 0, 0, requestId, DateTimeUtils.currentTimeMillis(), ""), false, "", 0,0);
        aggSpace.write(new ReplayEvent("sourceURI", 1, 1, 0, requestId, DateTimeUtils.currentTimeMillis(), ""), false, "", 0,0);
        aggSpace.write(new ReplayEvent("sourceURI", 2, 2, 0, requestId, DateTimeUtils.currentTimeMillis(), ""), false, "", 0,0);
    }

    public void testWatchDirIsNotRegexpd() throws Exception {
        logSpace.addWatch("", "dir", "*.log", "timeFormat", "rollClass", hosts, 1, "", true, "", null, false, true);
        LogConfiguration configuration = logSpace.getConfiguration("");
        List<WatchDirectory> watching = configuration.watching();
        for (WatchDirectory watchDirectory : watching) {
            System.out.println(watchDirectory.timeFormat);
            if (watchDirectory.getDirName().equals("dir")) {
                assertEquals("*.log", watchDirectory.filePattern);
            }
        }
    }

    private MyEventListener writeEvents(String filter) throws Exception {
        MyEventListener eventListener = new MyEventListener();
        aggSpace.registerEventListener(eventListener, eventListener.getId(), filter, -1);
        aggSpace.write(info);
        aggSpace.write(warning);
        aggSpace.write(error);
        return eventListener;
    }

    class MyReplayListener implements LogReplayHandler {
        private List<TimeUID> received = new ArrayList<TimeUID>();

        public String getId() {
            return "MyReplayHandler";
        }

        public void handle(ReplayEvent event) {
            received.add(event.getId());
        }

        public void handle(Bucket event) {
        }

        public int handle(String providerId, String subscriber, int size, Map<String, Object> histo) {
            return 1;
        }

        public int handle(List<ReplayEvent> events) {
            for (ReplayEvent replayEvent : events) {
                received.add(replayEvent.getId());
            }
            return 100;
        }

        public int status(String provider, String subscriber, String msg) {
           return 1;
        }

        public void handleSummary(Bucket bucketToSend) {
            // TODO Auto-generated method stub

        }
    }

    public static class MyEventListener implements LogEventListener {

        private List<String> received = new ArrayList<String>();

        public String getId() {
            return "MyEventListener";
        }

        public void handle(LogEvent event) {
            received.add(event.getId());
        }

    }

}
