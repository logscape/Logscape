package com.liquidlabs.common.collection.cache;

import org.joda.time.DateTimeUtils;

public class SimpleCache<T> {
	long calls;
	long last = -1;
	long threshold = 20 * 1000;
	T previousResult = null;
	SimpleAction<T> action;
	
	boolean verbose = false;
	
	
	public SimpleCache(int cacheTimeSeconds ) {
		threshold = cacheTimeSeconds * 1000;
	}
	
	public SimpleCache(int cacheTimeSeconds, SimpleAction<T> action) {
		this(cacheTimeSeconds);
		this.action = action;
	}

	public T execute(SimpleAction<T> action) throws InterruptedException  {
		long now = DateTimeUtils.currentTimeMillis();
		if (now - last > threshold) {
			if (verbose) System.out.println("EXECUTE: delta:" + (now - last));
			previousResult = (T) action.execute();
			last = now;
			calls++;
		} else {
			if (verbose) System.out.println("CACHED: delta:" + (now - last) + " t:" + threshold);
		}
		return previousResult;
	}
	public T execute() throws InterruptedException {
		return execute(action);
	}
	

}
