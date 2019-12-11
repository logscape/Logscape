package com.liquidlabs.common;

import com.liquidlabs.common.concurrent.ExecutorService;

/**
 * Used by tests to prevent sticky recovery resources from trying to execute outage or recovery mechanisms
 */
public class TestModeSetter {
    private static String TEST_MODE = "test.mode";

    public static void setTestMode() {
        System.setProperty(TEST_MODE, "true");
        System.out.println("Setting TEST_MODE");
        ExecutorService.setTestMode();
    }

    public static void resetTestMode() {
        System.setProperty(TEST_MODE,"false");
    }
    public static boolean isTestMode() {
        return Boolean.getBoolean(TEST_MODE);
    }

}
