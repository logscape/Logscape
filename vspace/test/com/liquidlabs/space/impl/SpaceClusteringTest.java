package com.liquidlabs.space.impl;

import com.liquidlabs.space.Space;
import com.liquidlabs.space.lease.Lease;
import com.liquidlabs.transport.proxy.events.Event;
import com.liquidlabs.transport.proxy.events.EventListener;
import com.liquidlabs.transport.proxy.events.Event.Type;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;


/**
 * validate operations go through the basic cluster
 *
 */
public class SpaceClusteringTest extends SpaceBaseFunctionalTest {

	long timeout = 300;
	@Before
	public void setUp() throws Exception {
		super.setUp();
	}
	volatile int count = 0;

	@After
	public void tearDown() throws Exception {
		super.tearDown();
	}
	@Test	
	public void testUpdatesMakeItToPeer() throws Exception {
		String key = "makesItToPeer" + "A";
		spaceA.write(key, "A|B|C".replaceAll("\\|", Space.DELIM), expires);
		pause();
		spaceA.update(key, "||replace:DDD".replaceAll("\\|", Space.DELIM), timeout);
		pause();
		String updatedValue = spaceB.read(key);
		assertEquals("A|B|DDD".replaceAll("\\|", Space.DELIM), updatedValue);
	}

	@Test
	public void testPeerSeesItemUsingTemplate() throws Exception {
		spaceA.write("A", "A|B|C".replaceAll("\\|", Space.DELIM), expires);
		pause();
		String[] string = spaceB.readMultiple(new String[] { "A||".replaceAll("\\|", Space.DELIM) }, 99);
		assertEquals(1, string.length);
		assertEquals("A|B|C".replaceAll("\\|", Space.DELIM), string[0]);
	}

	@Test
	public void testWritesGoInBothDirections() throws Exception {
		spaceA.write("A", "A|B|C".replaceAll("\\|", Space.DELIM), expires);
		pause();
		String existingValue = spaceB.read("A");
		System.out.println("ExistingValue:" + existingValue);
		spaceB.write("A", "A|B|DDDD".replaceAll("\\|", Space.DELIM), expires);
		pause();
		
		String updatedValueA = spaceA.read("A");
		assertEquals("A|B|DDDD".replaceAll("\\|", Space.DELIM), updatedValueA);
	}

	@Test
	public void testWriteIsSeenInPeer() throws Exception {
		
		spaceA.write("A", "someValue", expires);
		pause();
		String string = spaceB.read("A");
		assertEquals("someValue", string);
	}
	
	@Test
	public void testUpdateNotificationsToTwoSubscribersOverCluster() throws Exception {
		pause();
		EventListener listener = new EventListener(){
			public String getId() {
				return "myTestsListenerId-1";
			}
			public void notify(Event event) {
				System.out.println(count++ + " ************** Got event:" + event);
				
			}
		};
		spaceA.notify(new String[] { "aKey"}, new String[0], listener , new Event.Type[] { Type.WRITE, Type.UPDATE}, -1);
		
		EventListener listener2 = new EventListener(){
			public String getId() {
				return "myTestsListenerId-2";
			}
			public void notify(Event event) {
				System.out.println(count++ + " ************** Got event:" + event);
				
			}
		};
		spaceA.notify(new String[] { "aKey"}, new String[0], listener2 , new Event.Type[] { Type.WRITE, Type.UPDATE}, -1);
		pause();
		// Only the first is a write
		spaceB.write("aKey", "value1", -1);
		spaceB.write("aKey", "value2", -1);
		spaceB.write("aKey", "value3", -1);
		pause();
		assertEquals("Count should have been incremented to 6", 6, count);
	}
	@Test
	public void testUpdateNotificationsCallsOverCluster() throws Exception {
		pause();
		EventListener listener = new EventListener(){
			public String getId() {
				return "myTestsListenerId";
			}
			public void notify(Event event) {
				System.out.println(count++ + " ************** Got event:" + event);
				
			}
		};
		spaceA.notify(new String[] { "aKey"}, new String[0], listener , new Event.Type[] { Type.WRITE, Type.UPDATE}, -1);
		pause();
		// Only the first is a write
		System.out.println("         1111111111111111111         ");
		spaceB.write("aKey", "value1", -1);
		System.out.println("         2222222222222222222         ");
		spaceB.write("aKey", "value2", -1);
		System.out.println("         3333333333333333333         ");
		spaceB.write("aKey", "value3", -1);
		pause();
		assertEquals("Count should have been incremented to 3", 3, count);
	}
	
	/**
	 * READ NOTIFICATIONS DISABLED BY DEFAULT
	 * @throws Exception
	 */
	@Test
	public void testReadNotificationsCallsOverCluster() throws Exception {
		pause();
		
		EventListener listener = new EventListener(){
			public String getId() {
				return "myTestListenerId";
			}
			public void notify(Event event) {
				System.out.println(count++ + " *********** Got event:" + event);
				
			}
		};
		spaceA.write("aKey", "value", -1);
		spaceA.notify(new String[] { "aKey"}, new String[0], listener , new Event.Type[] { Type.READ}, -1);
		pause();
		System.out.println("===============================================");
		spaceB.read("aKey");
		spaceB.read("aKey");
		spaceB.read("aKey");
		pause();
		assertEquals("Count should have been incremented to 3", 3, count);
	}
	@Test
	public void testWriteUpdateNotificationsCallsOverCluster() throws Exception {
		pause();
		EventListener listener = new EventListener(){
			public String getId() {
				return "listenerId";
			}
			public void notify(Event event) {
				System.out.println(count++ + " ***** Got event:" + event);
				
			}
		};
		spaceA.notify(new String[] { "aKey"}, new String[0], listener , new Event.Type[] { Type.WRITE}, -1);
		pause();
		// Only the first is a write
		//spaceA.write("aKey", "value1", -1);
		spaceB.write("aKey", "value1", -1);
		spaceB.write("aKey", "value2", -1);
		spaceB.write("aKey", "value3", -1);
		pause();
		assertEquals("Count should have been incremented to 1", 1, count);
	}
}
