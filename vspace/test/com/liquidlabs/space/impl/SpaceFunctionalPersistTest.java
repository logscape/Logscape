package com.liquidlabs.space.impl;

import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import com.liquidlabs.space.Space;
import com.liquidlabs.space.lease.Lease;
import com.liquidlabs.transport.proxy.events.Event;
import com.liquidlabs.transport.proxy.events.EventListener;
import com.liquidlabs.transport.proxy.events.Event.Type;

public class SpaceFunctionalPersistTest extends SpaceBaseFunctionalTest {

	long timeout = 300;
	@Before
	public void setUp() throws Exception {
		System.setProperty(Lease.PROPERTY, "1");
		super.persistent = true;
		super.setUp();
	}
	int count = 0;
	private boolean reuseClusterPort = true;

	@After
	public void tearDown() throws Exception {
		super.tearDown();
	}
	
	@Test
	public void testNotifcationCallsBack() throws Exception {
		pause();
		EventListener listener = new EventListener(){
			public String getId() {
				return "listenerId";
			}
			public void notify(Event event) {
				System.out.println(count++ + " Got event:" + event);
				
			}
		};
		spaceA.write("aKey", "value", -1);
		spaceA.notify(new String[] { "aKey"}, new String[0], listener , new Event.Type[] { Type.READ}, -1);
		pause();
		spaceA.read("aKey");
		spaceB.read("aKey");
		spaceA.read("aKey");
		spaceB.read("aKey");
		spaceA.read("aKey");
		spaceB.read("aKey");
		pause();
		assertEquals("Count should have been incremented to 6", 6, count);
	}

	@Test
	public void testListRead() throws Exception {
		spaceA.write("aKey1", "aValue1", -1);
		spaceA.write("aKey2", "aValue2", -1);
		spaceA.write("aKey3", "aValue3", -1);
		pause();
		
		for (int i = 0; i < 100; i++) {
			String[] stuff = new String [] { "aKey1", "aKey2", "aKey3" };
			String[] results = spaceB.read(stuff);
			System.out.println(i + Arrays.toString(results));
		}
	}
	
	@Test
	public void testMultiGetTemplateSimple() throws Exception {
		spaceA.write("A", "A|B|C".replaceAll("\\|", Space.DELIM), expires);
		spaceA.write("B", "A|D|C".replaceAll("\\|", Space.DELIM), expires);
		spaceA.write("C", "A|D|CEEEEEEEEEE".replaceAll("\\|", Space.DELIM), expires);
		pause();
		
		assertEquals("Should have found 3 items", 3, spaceB.size());
		
		String[] string = spaceB.readMultiple(new String[] { "A||".replaceAll("\\|", Space.DELIM)}, 99);
		assertEquals(3, string.length);
		for (String matchingItem : string) {
			System.out.println(matchingItem);
		}
		assertNotNull(string);
		assertTrue(Arrays.toString(string).contains("A|B|C".replaceAll("\\|", Space.DELIM)));
	}
	
	@Test
	public void testMultiGetTemplateComplex() throws Exception {
		spaceA.write("A1", "A|B|C|1".replaceAll("\\|", Space.DELIM), expires);
		spaceA.write("A2", "A|B|C|2".replaceAll("\\|", Space.DELIM), expires);
		spaceA.write("A3", "A|B|C|3".replaceAll("\\|", Space.DELIM), expires);
		spaceA.write("A4", "A|B|C|4".replaceAll("\\|", Space.DELIM), expires);
		spaceA.write("B5", "A|D|C|5".replaceAll("\\|", Space.DELIM), expires);
		spaceA.write("C6", "A|D|CEEEEEEEEEE|6".replaceAll("\\|", Space.DELIM), expires);
		spaceA.write("C7", "A|D|DEEEEEEEEEE|7".replaceAll("\\|", Space.DELIM), expires);
		pause();
		String[] string = spaceB.readMultiple(new String[] { "||C|<:7".replaceAll("\\|", Space.DELIM) }, 99);
		assertEquals(6, string.length);
		for (String matchingItem : string) {
			System.out.println(matchingItem);
		}
		assertTrue(Arrays.toString(string).contains("A|B|C|1".replaceAll("\\|", Space.DELIM)));
//		assertEquals("A|B|C|1", string[0]);
	}
	
	@Test
	public void testLeaseExpires() throws Exception {
			
			Space space = spaceA;
			space.write("someKeyA", "A|B|C".replaceAll("\\|", Space.DELIM), 2);
			System.err.println("keys1:" + space.keySet());
			String read1 = space.read("someKeyA");
			
			Thread.sleep(10 * 1000);
			String read2 = space.read("someKeyA");
			System.err.println("keys2:" + space.keySet());
	
			System.err.println("final keyset:" + space.keySet());
			assertNotNull("Item should still existed", read1);
			assertNull("Item should have expired", read2);
		}

	
	@Test
	public void testSpacesClustersWriteReadData() throws Exception {
		Space spA = spacePeerA.createSpace("partitionA", true, reuseClusterPort );
		Space spB = spacePeerB.createSpace("partitionA", true, reuseClusterPort);
		pause();
		spA.write("aKey", "aValue", -1);
		pause();
		String read = spB.read("aKey");
		assertNotNull("Should have for item from spaceB, using partitionA", read);
	}
	
	@Test
	public void testSpacePartitionsArePartitioned() throws Exception {
		Space spA = spacePeerA.createSpace("partitionA", true, reuseClusterPort);
		Space spB = spacePeerB.createSpace("partitionB", true, reuseClusterPort);
		pause();
		spA.write("aKey", "aValue", -1);
		pause();
		String read = spB.read("aKey");
		assertNull("Should NOT have for item from spaceB, using partitionA", read);
	}
}
