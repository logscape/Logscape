package com.liquidlabs.log;

import javax.management.*;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;
import java.lang.instrument.Instrumentation;
import java.lang.management.ManagementFactory;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class JavaAgent {

    public static void premain(String agentArgs, Instrumentation instrumentation) {
        System.out.println("Init logscape javaagent...");
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        try {
            scheduledExecutorService.scheduleWithFixedDelay(new GcInfoCollector(mBeanServer), 5, 5, TimeUnit.SECONDS);
        } catch (MalformedObjectNameException e) {
            System.err.println("Unable to start JavaAgent Due to: " + e);
        }
    }


    public static class GcInfoCollector implements Runnable {
        private static final String LAST_GC_INFO = "LastGcInfo";
        private static final Object[] GC_KEYS = new Object[]{new Object[]{"CMS Old Gen"}, new Object[]{"Par Eden Space"}, new Object[]{"Par Survivor Space"}};

        private Long currentCount = 0l;
        private final MBeanServer mBeanServer;
        private final ObjectName coherenceNode;
        private final ObjectName cmsCollector;
        private final ObjectName parNewCollector;

        public GcInfoCollector(MBeanServer mBeanServer) throws MalformedObjectNameException {
            this.mBeanServer = mBeanServer;
            coherenceNode = new ObjectName("Coherence:type=Cluster");
            cmsCollector = new ObjectName("java.lang:type=GarbageCollector,name=ConcurrentMarkSweep");
            parNewCollector = new ObjectName("java.lang:type=GarbageCollector,name=ParNew");
        }

        public void run() {
            try {
                Object localMemberId = mBeanServer.getAttribute(coherenceNode, "LocalMemberId");
                Long collectionCount = (Long) mBeanServer.getAttribute(cmsCollector, "CollectionCount");
                System.out.println(collectionCount);
                if (collectionCount > currentCount) {
                    currentCount = collectionCount;
                    CompositeData lastGcInfo = lastGcInfo();
                    if (lastGcInfo != null) {
                        TabularData tabularData = (TabularData) lastGcInfo.get("memoryUsageAfterGc");
                        long usedMem = 0;
                        for (Object key : GC_KEYS) {
                            CompositeData composite = tabularData.get((Object[]) key);
                            CompositeData memStats = (CompositeData) composite.get("value");
                            usedMem += (Long) memStats.get("used");
                        }
                        log(String.format("NodeId=%s, CollectionCount=%d, PostGcUsedMemoryMB %f\n", localMemberId, currentCount, (usedMem / 1024.0 / 1024)));
                    }
                }

            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        private void log(String data) {
            System.out.println(data);
        }


        private CompositeData lastGcInfo() throws MBeanException, AttributeNotFoundException, InstanceNotFoundException, ReflectionException {
            CompositeData lastGcInfo = (CompositeData) mBeanServer.getAttribute(cmsCollector, LAST_GC_INFO);
            if (lastGcInfo == null) {
                return (CompositeData) mBeanServer.getAttribute(parNewCollector, LAST_GC_INFO);
            }
            return lastGcInfo;
        }

    }


}
