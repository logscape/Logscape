package com.liquidlabs.common.collection;

import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

public class TimedDelayPriorityQueueTest extends TestCase {
	
	private TimedDelayPriorityQueue<Long> delayQueue;

	protected void setUp() throws Exception {
		delayQueue = new TimedDelayPriorityQueue<Long>(100, TimeUnit.MILLISECONDS);
	}
	protected void tearDown() throws Exception {
		delayQueue.clear();
	}
	
	public void testShouldPutAndTakeSingleItem() throws Exception {
		delayQueue.put(100, 100L);
		assertTrue(100L == delayQueue.getLatest());		
	}
	public void testShouldReportItemIsQueued() throws Exception {
		delayQueue.put(100, 100L);
		assertTrue(!delayQueue.isEmpty());		
	}
	
	public void testShouldPushMultipleItemsThroughInCorrectOrder() throws Exception {
		delayQueue.put(104, 104L);
		delayQueue.put(101, 101L);
		delayQueue.put(103, 103L);
		delayQueue.put(102, 102L);
		delayQueue.put(100, 100L);
		assertTrue(100L == delayQueue.getLatest());		
		assertTrue(101L == delayQueue.getLatest());		
		assertTrue(102L == delayQueue.getLatest());		
		assertTrue(103L == delayQueue.getLatest());		
		assertTrue(104L == delayQueue.getLatest());		
	}
	
	public void testShouldReleaseIncorrectItemAfterTimeOut() throws Exception {
		delayQueue.put(101, 101L);
		delayQueue.put(102, 102L);
		
		delayQueue.put(104, 104L);
		assertTrue(101L == delayQueue.getLatest());		
		assertTrue(delayQueue.isExperiencedDelay());
		assertTrue(102L == delayQueue.getLatest());		
		assertFalse(delayQueue.isExperiencedDelay());
		assertTrue(104L == delayQueue.getLatest());
		assertTrue(delayQueue.isExperiencedDelay());
		
	}

}
