package com.liquidlabs.space.impl;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class SpacePerformanceFunctionalTest extends SpaceBaseFunctionalTest {

	private long timeout = 9999;
	private long currentTimeMillis;

	@Before
	public void setUp() throws Exception {
		super.setUp();
	}

	@After 
	public void tearDown() throws Exception {
		super.tearDown();
	}
	
	@Test
	public void testUpdateThroughput() throws Exception {

		int amount = 100;
		spaceA.write("A", "-someValueInitial", timeout);
		for (int i = 0; i < amount; i++){
			spaceA.update("A", "replace:" + i + "-someValue", timeout);
		}
		for (int i = 0; i < 5; i++) {
			String string = spaceB.read("A");
			System.out.println(i + "  =" + string);
			Thread.sleep(100);
		}
		long endTimeMillis = System.currentTimeMillis();
		double seconds = (endTimeMillis - currentTimeMillis)/1000.0;
		double rate = amount/seconds;
		System.err.println("TestUpdateThroughput" + "ElapsedTime:" + seconds + "secs  rate:" + rate +  "msg/sec");
	}
	
	@Test
	public void testWriteThroughput() throws Exception {
		int amount = 100;
		spaceA.write("A", "-someValueInitial", timeout);
		for (int i = 0; i < amount; i++){
			spaceA.write("A", "replace:" + i + "-someValue", -1);
		}
		for (int i = 0; i < 5; i++) {
			String string = spaceB.read("A");
			System.out.println(i + "  =" + string);
			Thread.sleep(100);
		}
		long endTimeMillis = System.currentTimeMillis();
		double seconds = (endTimeMillis - currentTimeMillis)/1000.0;
		double rate = amount/seconds;
		System.err.println("ElapsedTime:" + seconds + "secs  rate:" + rate +  "msg/sec");
	}
	
}
