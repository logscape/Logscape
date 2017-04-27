package com.liquidlabs.common;

import java.lang.management.ManagementFactory;

public class PIDGetter {

    private static String pid = ManagementFactory.getRuntimeMXBean().getName();
	
	public static String getPID() {
		return pid;
	}

}
