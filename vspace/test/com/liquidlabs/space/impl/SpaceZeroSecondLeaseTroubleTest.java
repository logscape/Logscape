package com.liquidlabs.space.impl;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.liquidlabs.space.Space;
import com.liquidlabs.transport.proxy.events.DefaultEventListener;
import com.liquidlabs.transport.proxy.events.Event;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;


public class SpaceZeroSecondLeaseTroubleTest extends SpaceBaseFunctionalTest {

	@Before
	public void setUp() throws Exception {
		super.setUp();
	}

	@After
	public void tearDown() throws Exception {
		super.tearDown();
	}
	
	@Test
	public void testShouldWorkWithSlowNotify() throws Exception {
		
		Space space = spaceA;
		
		final int work = 1000;
		final CountDownLatch countDownLatch = new CountDownLatch(work);
		
		DefaultEventListener listener = new DefaultEventListener() {
			int received = 0;
			public void notify(Event event) {
				try {
					synchronized(this){
						Thread.sleep(10);
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				if (received++ > work-100) {
					System.out.println("<< **************** Received:" + event.getKey());
				}
				countDownLatch.countDown();
			//	Thread.dumpStack();
				super.notify(event);
			}
		};
		
		space.notify(new String[0] , new String[] { "A||C".replaceAll("\\|", Space.DELIM) }, listener , new Event.Type[] { Event.Type.WRITE }, -1);
		
		// should be received
		for (int i = 0; i < work; i++) {
			space.write("someKeyA" + i, "A|B|C".replaceAll("\\|", Space.DELIM), 0);
		}
		
		countDownLatch.await(30, TimeUnit.SECONDS);
		
		pause();
		assertEquals(work, listener.getEvents().size());
		
	}

}