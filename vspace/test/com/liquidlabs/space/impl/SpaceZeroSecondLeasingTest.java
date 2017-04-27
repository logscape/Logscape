package com.liquidlabs.space.impl;

import com.liquidlabs.space.Space;
import com.liquidlabs.space.lease.Lease;
import com.liquidlabs.transport.proxy.events.DefaultEventListener;
import com.liquidlabs.transport.proxy.events.Event;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class SpaceZeroSecondLeasingTest extends SpaceBaseFunctionalTest {

	private String KEY = "someKeyA";
	@Before
	public void setUp() throws Exception {
		
		System.setProperty(Lease.PROPERTY, "1");
		super.setUp();
		KEY = "ZeroSecondLeasingFunction";
		
	}

	@After
	public void tearDown() throws Exception {
		super.tearDown();
	}
	
	
	@Test
	public void testShouldNotify() throws Exception {
		
		Space space = spaceA;
		
		DefaultEventListener listener = new DefaultEventListener();
		space.notify(new String[0] , new String[] { "A||C".replaceAll("\\|", Space.DELIM) }, listener , new Event.Type[] { Event.Type.WRITE }, 10);
		
		
		// should be received
		for (int i = 0; i < 5; i++) {
			space.write("someKeyA" + i, "A|B|C".replaceAll("\\|", Space.DELIM), 0);
		}
		
		pause();
		assertEquals(5, listener.getEvents().size());
		
	}

//	@Test DodgyTest
	public void testUpdateLeaseRollsbackSeconds() throws Exception {
		
		Space space = spaceA;
		space.write(KEY, "originalValue", 0);
		Thread.sleep(100);
		
		assertNull("newValue", spaceA.read(KEY));
	}

}