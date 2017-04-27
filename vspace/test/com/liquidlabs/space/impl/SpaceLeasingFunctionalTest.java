package com.liquidlabs.space.impl;

import java.util.Arrays;

import com.liquidlabs.space.Space;
import com.liquidlabs.space.lease.Lease;
import com.liquidlabs.transport.proxy.events.DefaultEventListener;
import com.liquidlabs.transport.proxy.events.Event;
import com.liquidlabs.transport.proxy.events.EventListener;
import com.liquidlabs.transport.proxy.events.Event.Type;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class SpaceLeasingFunctionalTest extends SpaceBaseFunctionalTest {

	private String KEY = "someKeyA";
	private static final String A_B_C = "A|B|C".replaceAll("\\|", Space.DELIM);
	private String leaseId = "MyListenerId";
	private int count = 0;
	
	@Before
	public void setUp() throws Exception {
		System.setProperty(Lease.PROPERTY, "1");
		super.setUp();
		KEY = "SpaceLeasingFunctionTest";
	}

	@After
	public void tearDown() throws Exception {
		super.tearDown();
	}
	
	@Test
	public void testUpdateLeaseRollsbackSeconds() throws Exception {
		Space space = spaceA;
		space.write(KEY, "originalValue", 999);
		Thread.sleep(100);
		String update = space.update(KEY, "replace:newValue", 1);
		assertNotNull("Should have got an update lease but didnt", update);
		Thread.sleep(500);
		String newValue = space.read(KEY);
		Thread.sleep(1000);
		String originalValue = space.read(KEY);
		
		assertEquals("newValue", newValue);
		assertEquals("originalValue", originalValue);
	}

	@Test
	public void testShouldNotReceivedEventsAfterNotifyExpires() throws Exception {
		
		Space space = spaceA;
		
		DefaultEventListener listener = new DefaultEventListener();
		String leasedKey = space.notify(new String[0] , new String[] { "A||C".replaceAll("\\|", Space.DELIM) }, listener , new Event.Type[] { Event.Type.WRITE }, 1);
		
		
		// should be received
		for (int i = 0; i < 5; i++) {
			space.write("someKeyA" + i, "A|B|C".replaceAll("\\|", Space.DELIM), -1);
		}
		
		// lease expires
		Thread.sleep(10 * 1000);
	
		// should NOT be received
		for (int i = 6; i < 10; i++) {
			space.write("someKeyA" + i, "A|B|C".replaceAll("\\|", Space.DELIM), -1);
		}
	
		
		pause();
		assertEquals(5, listener.getEvents().size());
		
	}

	@Test
	public void testThatALeaseCanBePurged() throws Exception {
		
		Space space = spaceA;
		
		
		// do initial write = let is expire
		String writeLease = space.write(KEY, A_B_C, 2);
	
		// do update - with longer lease
		String updateLease = space.write(KEY, "UPDATE" + A_B_C, 3);
		
		// wait for a bit
		Thread.sleep(2 * 1000);
		
		// cancel the lease
		space.purge(KEY);

		// wait because a lease may roll back the data
		Thread.sleep(2 * 1000);
		
		// see if it still exists
		String read = space.read(KEY);
		assertNull("LeaseShouldHaveRolledBack", read);
//		assertNotNull("Update should have overwritten the WriteLease",read);
		
	}
	
	@Test
	public void testInitialWriteLeaseIsDroppedOnceUpdateLeaseIsTaken() throws Exception {
		
		Space space = spaceA;
		
		
		// do initial write = let is expire
		space.write(KEY, A_B_C, 6);
		
		// do update - with longer lease so that it is not null after the first lease expired
		space.write(KEY, "UPDATE" + A_B_C, 8);
		
		Thread.sleep(7 * 1000);
		
		String read = space.read(KEY);
		assertNotNull("Update should have overwritten the WriteLease but got:" + read,read);
		
	}

	@Test
	public void testReadUpdateRollsbackMultipleItems() throws Exception {
		
		Space space = spaceA;
		space.write(KEY, "a|started|value1,11".replaceAll("\\|", Space.DELIM), 999);
		space.write("someKeyB", "a|started|value2".replaceAll("\\|", Space.DELIM), 999);
		space.write("someKeyC", "a|started|value3".replaceAll("\\|", Space.DELIM), 999);
		Thread.sleep(100);
		String[] readAndUpdateMultiple = space.readAndUpdateMultiple(new String[] { "a" }, new String[] { "a|replace:pending".replaceAll("\\|", Space.DELIM) }, 1, 99, "xxxx");
		
		// did we get clean data
		assertNotNull(readAndUpdateMultiple);
		assertEquals(3, readAndUpdateMultiple.length);
		String stuff = Arrays.toString(readAndUpdateMultiple);
		assertTrue(stuff.contains("a|started|value1,11".replaceAll("\\|", Space.DELIM)));
		assertTrue(stuff.contains("a|started|value2".replaceAll("\\|", Space.DELIM)));
		assertTrue(stuff.contains("a|started|value3".replaceAll("\\|", Space.DELIM)));
		
		// verify the space values were updated
		String[] readMultiple = space.readMultiple(new String[] { "equals:a|equals:pending".replaceAll("\\|", Space.DELIM)}, 99);
		assertEquals(3, readMultiple.length);
		
		Thread.sleep(2 * 1000);
		
		// check the rollback happened
		String[] rolledback = space.readMultiple(new String[] { "equals:a|equals:started".replaceAll("\\|", Space.DELIM)}, 99);
		assertEquals(3, readAndUpdateMultiple.length);
		String stuff2 = Arrays.toString(rolledback);
		assertTrue(stuff2.contains("a|started|value1,11".replaceAll("\\|", Space.DELIM)));
		assertTrue(stuff2.contains("a|started|value2".replaceAll("\\|", Space.DELIM)));
		assertTrue(stuff2.contains("a|started|value3".replaceAll("\\|", Space.DELIM)));
		
	}
	
	@Test
	public void testLeaseExpiresIn6Seconds() throws Exception {
		
		Space space = spaceA;
		space.write(KEY, A_B_C, 6);
		String read1 = space.read(KEY);
		
		assertNotNull("Item should still exist", read1);
		
		Thread.sleep(7 * 1000);
		String read2 = space.read(KEY);
		
		assertNull("Item should have expired", read2);
	}

	
	@Test
	public void testLeaseCanBeRenewed() throws Exception {
		String KEY = "6SecondLEASE" + "KEY";
		String writeLease = spaceA.write(KEY, A_B_C, 6);
		spaceA.renewLease(writeLease, 60);
		Thread.sleep(7000);
		String read2 = spaceA.read(KEY);
	
		assertNotNull("Item should NOT have expired with the newLease: read2:" + read2 + " finalKeySet:"+spaceA.keySet(), read2);
	}

	
	@Test
	public void testLeaseExpireyRemovesNotification() throws Exception {
		EventListener listener = new EventListener(){
			public String getId() {
				return leaseId;
			}
			public void notify(Event event) {
				System.err.println("Test got event:" + event);
				count++;
			}
		};
		leaseId = spaceA.notify(new String[0], new String[] { "equals:A" }, listener, new Event.Type[] { Type.READ}, 2);
		System.out.print("Got Notify Lease Key:" + leaseId);
	
		spaceA.write(KEY, A_B_C, 20);
		String read1 = spaceA.read(KEY);
	
		assertNotNull("Item should still exist", read1);
		
		Thread.sleep(3 * 1000);
		spaceA.read(KEY);
		spaceA.read(KEY);
		spaceA.read(KEY);
	
		assertEquals("Should have only had 1 count", 1, count);
		
	}
	

	@Test
	public void testLeaseDoesNotExpireIn6Seconds() throws Exception {
		
		Space space = spaceA;
		space.write(KEY, A_B_C, 6);
		String read1 = space.read(KEY);
		assertNotNull("Item should still exist", read1);
		
		Thread.sleep(5 * 1000);
		String read2 = space.read(KEY);
	
		assertNotNull("Item should STILL exist", read2);
	}
}
