package com.liquidlabs.common.collection;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class PriotrityQueueTest {
	

	@Test
	public void testShouldReturnHighPriorityTimestampWhenLowIsFull() {
		Queue priorityQueue = new PriorityQueue(1,1);
		long now = System.currentTimeMillis();
		Queue queue = priorityQueue.put(new Object(), now - 25 * 60 * 60 *1000);
		assertTrue(queue instanceof High);
	}
	
	@Test
	public void testShouldReturnLowPriorityTimestampWhenLowIsNotFull() {
		Queue priorityQueue = new PriorityQueue(1,1);
		Queue queue = priorityQueue.put(new Object(), System.currentTimeMillis());
		assertTrue(queue instanceof Low);
	}
	
	@Test
	public void testname() throws Exception {
		Queue priorityQueue = new PriorityQueue(5,1);
		priorityQueue.put("hello", System.currentTimeMillis()).put("hello", System.currentTimeMillis() - 25 * 60 * 60 * 1000);
		assertEquals(1, priorityQueue.size());
	}
	
	@Test
	public void testShouldAlwaysGetHighPriorityIfOneAvailable() throws Exception {
		Queue<String> priorityQueue = new PriorityQueue<String>(10, 10);
		priorityQueue.put("low", System.currentTimeMillis() - 25 * 60 * 60 * 1000);
		priorityQueue.put("low1", System.currentTimeMillis() - 25 * 60 * 60 * 1000);
		priorityQueue.put("high", System.currentTimeMillis());
		priorityQueue.put("high1", System.currentTimeMillis());
		
		String r1 = priorityQueue.take();
		String r2 = priorityQueue.take();
		String r3 = priorityQueue.take();
		
		assertEquals("high", r1);
		assertEquals("high1", r2);
		assertEquals("low", r3);
	}
}
