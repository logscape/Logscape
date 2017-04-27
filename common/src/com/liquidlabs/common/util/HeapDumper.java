package com.liquidlabs.common.util;

import javax.management.MBeanServer;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;


public class HeapDumper {
    // This is the name of the HotSpot Diagnostic MBean
    private static final String HOTSPOT_BEAN_NAME =
            "com.sun.management:type=HotSpotDiagnostic";

    // field to store the hotspot diagnostic MBean

    /**
     * Call this method from your application whenever you 
     * want to dump the heap snapshot into a file.
     *
     * @param fileName name of the heap dump file
     * @param live flag that tells whether to dump
     *             only the live objects
     */
    public void dumpHeap(String fileName, boolean live) {
        try {
            getHotspotMBean().dumpHeap(fileName, live);
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception exp) {
            throw new RuntimeException(exp);
        }
    }

    // platform MBean server
    private com.sun.management.HotSpotDiagnosticMXBean getHotspotMBean() {
        try {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            com.sun.management.HotSpotDiagnosticMXBean bean =
                    ManagementFactory.newPlatformMXBeanProxy(server, HOTSPOT_BEAN_NAME, com.sun.management.HotSpotDiagnosticMXBean.class);
            return bean;
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception exp) {
            throw new RuntimeException(exp);
        }
    }

    public static void main(String[] args) {
        // default heap dump file name
        String fileName = "heap.bin";
        // by default dump only the live objects
        boolean live = true;

        // simple command line options
        switch (args.length) {
            case 2:
                live = args[1].equals("true");
            case 1:
                fileName = args[0];
        }

        // dump the heap
        new HeapDumper().dumpHeap(fileName, live);
    }

    public static String dumpHeapWithPid(boolean live) {
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        int pid = Integer.parseInt(runtimeMXBean.getName().split("@")[0]);
        String fileName = pid +"-heap.hprof";

        if (new File(fileName).exists()) new File(fileName).delete();

        new HeapDumper().dumpHeap(fileName, live);
        return fileName;


    }
}
