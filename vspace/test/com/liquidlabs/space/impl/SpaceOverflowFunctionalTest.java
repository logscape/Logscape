package com.liquidlabs.space.impl;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.liquidlabs.space.lease.Lease;

public class SpaceOverflowFunctionalTest extends SpaceBaseFunctionalTest {

	@Before
	public void setUp() throws Exception {
		System.setProperty(Lease.PROPERTY, "1");
		super.setUp();
	}

	@After
	public void tearDown() throws Exception {
		super.tearDown();
	}
	
	@Test
	public void testOverflowDoesntFail() throws Exception {
		// check the syserr for messages about unexpected write events...
		for (int i = 0; i < 5000; i ++){
			spaceA.write("A"+i, "A|B|C"+i, expires);
			Thread.yield();
		}
	}
}
