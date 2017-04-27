package com.liquidlabs.vso.agent.metrics;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 16/06/2015
 * Time: 09:11
 * To change this template use File | Settings | File Templates.
 */
public class OSGetters {

    private static OSGetter osGetter;

    public static OSGetter get(ScheduledExecutorService scheduler) {
        if (osGetter != null) return osGetter;
        synchronized (OSGetters.class) {
            if (osGetter == null) {
                if (WindowsOSGetter.isA()) {
                    if (scheduler == null) scheduler = Executors.newScheduledThreadPool(1);
                    osGetter  = new WindowsOSGetter(scheduler);
                } else if (OSXOSGetter.isA()) {
                    osGetter = new OSXOSGetter();
                } else if (LinuxOSGetter.isA()) {
                    osGetter = new LinuxOSGetter();
                } else if (SunOSGetter.isA()) {
                    osGetter = new SunOSGetter();
                } else osGetter = new DefaultOSGetter();
            }
        }

        return osGetter;
    }}
