package com.liquidlabs.common.util;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 05/02/2014
 * Time: 15:43
 * To change this template use File | Settings | File Templates.
 */
public class OSUtils {
    public static double getHeapGB() {
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        long max = memoryMXBean.getHeapMemoryUsage().getMax();
        return Math.max(max / 1023.0 / 1023.0 / 1000.0, 1);
    }
    private static boolean isWindowsOS = System.getProperty("os.name").toUpperCase().contains("WINDOWS");

    public static boolean isWindows() {
        return isWindowsOS;
    }
}
