package com.liquidlabs.log.index;


public class LogFileSeed {
	
	public static final String LOG_SEED = "LOG_SEED";
	
	String id = LOG_SEED;
	public int value;

	public LogFileSeed() {
	}
	public LogFileSeed(int value) {
		this.value = value;
	}
	synchronized public int  increment() {
		return value++;
	}
}
