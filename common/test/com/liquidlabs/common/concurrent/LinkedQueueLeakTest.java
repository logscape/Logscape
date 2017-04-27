package com.liquidlabs.common.concurrent;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import junit.framework.TestCase;

import org.joda.time.DateTimeUtils;

public class LinkedQueueLeakTest extends TestCase {
	
	public void testQShufflePerformanceCrap() throws Exception {
//		int capacity = 100000;
		int capacity = 1000;
		long start = DateTimeUtils.currentTimeMillis();
		
		LinkedBlockingQueue<String> stuff = new LinkedBlockingQueue<String>();
//		CompactingConcurrentLinkedQueue<String> stuff = new CompactingConcurrentLinkedQueue<String>(1000);
		
		for(int i = 0; i < capacity; i++) {
			stuff.add(Integer.toString(i));
		}
		for(int i = 0; i < capacity; i++) {
			if (i % 2 == 0) {
				stuff.remove(Integer.toString(i));
			}
		}
		System.err.println("Elapsed:" + (DateTimeUtils.currentTimeMillis() - start));
		
	}
	
	public void testBlockingLinkedQueueCrap() throws Exception {
		LinkedBlockingQueue<String> stuff = new LinkedBlockingQueue<String>();
		stuff.add("1");
		stuff.add("2");
		stuff.add("3");
		stuff.add("4");
		stuff.add("5");
		
		stuff.remove("2");
		stuff.remove("4");
		// now inspect stuff use the ide - empty links will exist when using a concurrentLinkedQueue
		ConcurrentLinkedQueue<String> stuff2 = new ConcurrentLinkedQueue<String>(stuff);
		assertTrue(stuff2.size() == stuff.size());
	}
	

}
