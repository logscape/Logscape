package com.liquidlabs.log.alert;

import com.liquidlabs.admin.AdminSpace;
import com.liquidlabs.log.NullLogSpace;
import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.ReplayEvent;
import com.liquidlabs.log.space.AggSpace;
import com.liquidlabs.log.space.LogSpace;
import com.liquidlabs.vso.SpaceService;
import org.junit.Test;

import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 30/05/2014
 * Time: 16:58
 * To change this template use File | Settings | File Templates.
 */
public class LiveFeedTest {
    public static boolean wasCalled = false;
    public static boolean stopped = false;
    public static String[] params = null;

    static String simpleGroovy =
            "def start(String... params){\n" +
                "   com.liquidlabs.log.alert.LiveFeedTest.params = params\n" +
            "}\n" +
            "def stop(){\n" +
            "   com.liquidlabs.log.alert.LiveFeedTest.stopped = true\n" +
            "}\n" +
            "def handle(String alertName, String host, String file, String content, Map fields){\n" +
            "   println \"Host:\" + host + \" File:\" + file + \" Content:\" + content + \" Fields:\" + fields\n" +
            "   com.liquidlabs.log.alert.LiveFeedTest.wasCalled=true\n" +
            "}\n" +
            "this";

    @Test
    public void shouldPassInitParamsToTheScript() throws Exception {
        wasCalled = false;
        StubTrigger trigger = new StubTrigger();

        FileOutputStream fos = new FileOutputStream("build/liveGroovy.groovy");
        fos.write(simpleGroovy.getBytes());
        fos.close();;

        com.liquidlabs.log.alert.LiveFeedTest.params = null;
        LiveFeed liveFeed = new LiveFeed("myAlert", "build/liveGroovy.groovy 11000", trigger, null);

        assertTrue(com.liquidlabs.log.alert.LiveFeedTest.params != null);
        assertEquals(1, LiveFeedTest.params.length);
        assertEquals("11000", LiveFeedTest.params[0]);
    }


    @Test
    public void shouldPassWithoutParams() throws Exception {
        wasCalled = false;
        StubTrigger trigger = new StubTrigger();

        FileOutputStream fos = new FileOutputStream("build/liveGroovy.groovy");
        fos.write(simpleGroovy.getBytes());
        fos.close();;

        com.liquidlabs.log.alert.LiveFeedTest.params = null;
        LiveFeed liveFeed = new LiveFeed("alertName", "build/liveGroovy.groovy", trigger, null);

        assertTrue(com.liquidlabs.log.alert.LiveFeedTest.params == null);
    }

    @Test
    public void shouldPassThroughMultipleHandlers() throws Exception {
        wasCalled = false;
        StubTrigger trigger = new StubTrigger();

        LiveFeed liveFeed = new LiveFeed("alertName", simpleGroovy, trigger, new NullLogSpace());

        Trigger proxy = liveFeed.proxy();
        ReplayEvent event = new ReplayEvent();
        event.setTime(System.currentTimeMillis());
        event.setDefaultFieldValues("type", "host", "file", "path", "tag", "agent","url", "1");
        event.setRawData("some log msg");
        ArrayList<ReplayEvent> events = new ArrayList<ReplayEvent>();
        events.add(event);

        proxy.handle(events);
        assertEquals(1, trigger.events.size());
        assertTrue(wasCalled);

    }

    @Test
    public void shouldPassThroughWhenEchoingFeed() throws Exception {
        wasCalled = false;
        StubTrigger trigger = new StubTrigger();

        LiveFeed liveFeed = new LiveFeed("alertName", simpleGroovy, trigger, new NullLogSpace());

        Trigger proxy = liveFeed.proxy();
        ReplayEvent event = new ReplayEvent();
        event.setTime(System.currentTimeMillis());
        event.setDefaultFieldValues("type", "host", "file", "path", "tag", "agent","url", "1");
        event.setRawData("some log msg");
        proxy.handle(event);
        assertEquals(1, trigger.events.size());
        assertTrue(wasCalled);
    }


    @Test
    public void shouldPassThroughWhenNoFeed() throws Exception {
        StubTrigger trigger = new StubTrigger();
        LiveFeed liveFeed = new LiveFeed("alertName", "", trigger, new NullLogSpace());
        Trigger proxy = liveFeed.proxy();
        proxy.handle(new ReplayEvent());
        assertEquals(1, trigger.events.size());

    }



    public static class StubTrigger implements Trigger {
        ArrayList<ReplayEvent> events = new ArrayList<ReplayEvent>();

        public  StubTrigger(){


        }

        @Override
        public void attach(LogSpace logSpace, AggSpace aggSpace, SpaceService spaceService, AdminSpace adminSpace) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void fireTrigger(ReplayEvent replayEvent) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void stop() {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public boolean isReplayOnly() {
            return false;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public int getMaxReplays() {
            return 0;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void handle(ReplayEvent event) {
            this.events.add(event);
        }

        @Override
        public void handle(Bucket event) {
        }

        @Override
        public void handleSummary(Bucket bucketToSend) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public int handle(String providerId, String subscriber, int size, Map<String, Object> histo) {
            return 1;
        }

        @Override
        public int handle(List<ReplayEvent> events) {
            this.events.addAll(events);
            return 100;
        }

        @Override
        public int status(String provider, String subscriber, String msg) {
            return 1;
        }

        @Override
        public String getId() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }
    }
}
