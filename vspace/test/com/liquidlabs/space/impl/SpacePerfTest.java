package com.liquidlabs.space.impl;

import org.joda.time.DateTimeUtils;

import com.liquidlabs.space.lease.Lease;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;


public class SpacePerfTest extends SpaceBaseFunctionalTest {

	long timeout = 300;
	@Before
	public void setUp() throws Exception {
		System.setProperty(Lease.PROPERTY, "1");
		super.setUp();
	}
	int count = 0;
//	int work = 5000;
	int work = 10; // 872
	
	static long r1 = 1;
	static long r2 = 1 ;
	static long r3 = 1 ;

	@After
	public void tearDown() throws Exception {
		super.tearDown();
	}
	
	@Test
	public void testThroughputOn0SecondLease() throws Exception {
		
		long before = DateTimeUtils.currentTimeMillis();
		for (int i = 0; i < work; i++) {
			spaceA.write("key"+i, "value", 0);
		}
		r1 = (DateTimeUtils.currentTimeMillis() - before);
		System.out.println("TPon0Sec" + " ************** elapsed:" + r1);
	}
	
	@Test
	public void testThroughputOn1SecondLease() throws Exception {
		
		long before = DateTimeUtils.currentTimeMillis();
		for (int i = 0; i < work; i++) {
			spaceA.write("key"+i, "value", 1);
		}
		r2 = DateTimeUtils.currentTimeMillis() - before;
		System.out.println("TP-1Sec" + " *************** elapsed:" + r2 + " [ Compared to r1 = " + r2/r1 + "]");
	}
	
	@Test
	public void XXNeedToLookAtPerformanceOfLeasePurgingtestThroughputOn10SecondLease() throws Exception {
		
		long before = DateTimeUtils.currentTimeMillis();
		for (int i = 0; i < work; i++) {
			spaceA.write("key"+i, "value", 10);
		}
		r3 = DateTimeUtils.currentTimeMillis() - before;
		System.out.println("10SecondLease" + " ***** " + spaceA.size() + "********** elapsed:" + r3 + " [ Compared to r1 = " + r3/r1 + "]");
	}
}
