package com.liquidlabs.log.space.agg;

import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.ReplayEvent;
import com.liquidlabs.log.space.LogReplayHandler;
import com.liquidlabs.log.space.LogRequest;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;


/**
 * Created by Neiil on 11/7/2015.
 */
public class ReplayAggregatorTest {

    String[] lines = new String[] {
            "2015-11-07 16:55:39,179 INFO worker-1-8 (work.WorkAllocator)    WORK_ALLOC W Notifying WorkListener:Chronos-11003 work:Chronos-11003-0:vs-socket-server-1.0:SocketServer ASSIGNED^M\n",
                    "2015-11-07 16:55:39,179 INFO long-runningSHARED-one-way-1-3 (agent.ResourceAgent)       AGENT WorkAllocator Starting: Chronos-11003-0:vs-socket-server-1.0:SocketServer ASSIGNED bg:true workDir:system-bundles/vs-socket-server-1.0^M\n",
                    "2015-11-07 16:55:39,180 INFO long-runningSHARED-one-way-1-3 (agent.ResourceAgent)       WORK ASSIGNMENT ID = Chronos-11003-0:vs-socket-server-1.0:SocketServer^M\n",
                    "2015-11-07 16:55:39,181 INFO long-runningSHARED-one-way-1-3 (agent.ResourceAgent)       Starting Forked:Chronos-11003-0:vs-socket-server-1.0:SocketServer^M\n",
                    "2015-11-07 16:55:39,181 INFO long-runningSHARED-one-way-1-3 (deployment.ScriptForker)   Running processMaker.java:Chronos-11003-0:vs-socket-server-1.0:SocketServer^M\n",
                    "2015-11-07 16:55:39,181 INFO long-runningSHARED-one-way-1-3 (process.ProcessMaker)      PROCESS WORKDIR=D:\\work\\LOGSCAPE\\logscape_trunk\\LogScape\\master\\build\\logscape\\system-bundles\\vs-socket-server-1.0^M\n",
                    "2015-11-07 16:55:39,249 INFO long-runningSHARED-one-way-1-3 (process.ProcessHandler)     Managing process:Chronos-11003-0:vs-socket-server-1.0:SocketServer - pid:4504^M"
    };

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testHandle() throws Exception {

        LogRequest request = new LogRequest("sub", new DateTime().minusHours(1).getMillis(), new DateTime().getMillis());
        LogReplayHandler replayHandler = mock(LogReplayHandler.class);

        ReplayAggregator agg = new NativeQueuedReplayAggregator(request, replayHandler, "id");
        ReplayEvent event = new ReplayEvent("source", 100, 1, 1, "sub", 0, "line data");
        agg.handle(event);
        Thread.sleep(100);
        verify(replayHandler).handle(any(List.class));
    }

    @Test
    public void testPerformance() throws Exception {

        System.out.println(System.getProperty("file.encoding"));

        LogRequest request = new LogRequest("sub", new DateTime().minusHours(1).getMillis(), new DateTime().getMillis());

        int maxItems = 10 * 100 * 1000;
        TestHandler replayHandler = new TestHandler(maxItems);

        ReplayAggregator agg = new NativeQueuedReplayAggregator(request, replayHandler, "id");
        //while (true) {
            long start = System.currentTimeMillis();

            for (int i = 1; i < maxItems; i++) {
                ReplayEvent event = new ReplayEvent("source", i, 1, 1, "sub", i, lines[i % lines.length]);
                agg.handle(event);
                if (i % 100000 == 0) System.out.println("Added: " + i + ":" + replayHandler.latch.getCount());
            }
            long wend = System.currentTimeMillis();

            replayHandler.latch.await(10, TimeUnit.SECONDS);
            System.out.println("Currently:" + replayHandler.latch);
            replayHandler.latch.await(10, TimeUnit.MINUTES);
            replayHandler.resetLatch();
            long end = System.currentTimeMillis();
            long elapsed = end - start;
            long welapsed = wend - start;
            double throughput = maxItems / (elapsed / 1000.0);
            double wthroughput = maxItems / (welapsed / 1000.0);
            System.out.println("Added: :" + replayHandler.latch.getCount() + " Elapsed:" + elapsed + " TPs:" + throughput + " WTPut:" + wthroughput);
        //}
    }
    private static class TestHandler implements LogReplayHandler {
        CountDownLatch latch;
        private int maxItems;

        public TestHandler(int maxItems) {
            this.maxItems = maxItems;
            latch = new CountDownLatch(maxItems-1);
        }

        @Override
        public void handle(ReplayEvent event) {

        }

        @Override
        public void handle(Bucket event) {

        }

        @Override
        public void handleSummary(Bucket bucketToSend) {

        }

        @Override
        public int handle(String providerId, String subscriber, int size, Map<String, Object> histo) {
              return 1;
        }

        @Override
        public int handle(List<ReplayEvent> events) {

            for (ReplayEvent event : events) {
                latch.countDown();
            }
            return 100;
        }

        @Override
        public int status(String provider, String subscriber, String msg) {
              return 1;
        }

        @Override
        public String getId() {
            return null;
        }

        public void resetLatch() {
            latch = new CountDownLatch(maxItems-1);
        }
    }



}