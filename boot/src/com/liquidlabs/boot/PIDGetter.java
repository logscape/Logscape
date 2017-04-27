package com.liquidlabs.boot;

import java.lang.management.ManagementFactory;

public class PIDGetter {
	
	static String getPID() {
		return ManagementFactory.getRuntimeMXBean().getName();		
	}

}
