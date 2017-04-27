package com.liquidlabs.space.impl;

import com.liquidlabs.common.net.URI;
import com.liquidlabs.space.Space;
import com.liquidlabs.space.lease.Lease;
import com.liquidlabs.transport.proxy.events.Event;
import com.liquidlabs.transport.proxy.events.Event.Type;
import com.liquidlabs.transport.proxy.events.EventListener;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * Test a Space joining the cluster inherits notifications so it know to event them
 *
 */
public class SpaceClusterJoinerNotificationsTest extends SpaceBaseFunctionalTest {


	long timeout = 300;
	int count = 0;
	private SpacePeer spacePeerC;
	private Space spaceC;
	private URI uri;

	@Before
	public void setUp() throws Exception {
		System.setProperty(Lease.PROPERTY, "1");
		uri = new URI("tcp://localhost:16666");
		super.setUp();
	}
	@After
	public void tearDown() throws Exception {
		super.tearDown();
	}

	@Test
	public void testUpdateNotificationsCallsFromNewJoiner() throws Exception {
        final CountDownLatch countDownLatch = new CountDownLatch(3);
        EventListener listener1 = new EventListener(){
			public String getId() {
				return "myTestsListenerIdA";
			}
			public void notify(Event event) {
				countDownLatch.countDown();
				
			}
		};
		spaceA.notify(new String[] { "aKey"}, new String[0], listener1 , new Event.Type[] { Type.WRITE, Type.UPDATE}, -1);
		
		pause();
		
		System.out.println("---------------------------------------------------------------------");
		
		spacePeerC = new SpacePeer(uri);
		spaceC = spacePeerC.createSpace(SpacePeer.DEFAULT_SPACE, true, reuseClusterPort);
		spacePeerC.start();
		
		
		spaceC.addPeer(spaceA.getReplicationURI());
		spaceC.addPeer(spaceB.getReplicationURI());
		spaceA.addPeer(spaceC.getReplicationURI());
		spaceB.addPeer(spaceC.getReplicationURI());
		
		// wait for the sync to happen
		pause(1000);
		
		// only the first is a write - the rest are updates
		System.out.println("---------------------------------------------------------------------");
		spaceC.write("aKey", "A,BundleServiceAllocator532878912", -1);
		System.out.println("---------------------------------------------------------------------");
		spaceC.write("aKey", "B,BundleServiceAllocator532878912", -1);
		System.out.println("---------------------------------------------------------------------");
		spaceC.write("aKey", "C,BundleServiceAllocator532878912", -1);
		System.out.println("---------------------------------------------------------------------");
	    countDownLatch.await(10, TimeUnit.SECONDS);
		spacePeerC.stop();
		assertEquals(0, countDownLatch.getCount());
	}
	
	@Test
	public void testUpdateNotificationsCallsFromNewJoinerToMultipleReceivers() throws Exception {
        final CountDownLatch countDownLatch = new CountDownLatch(6);
		EventListener listener1 = new EventListener(){
			public String getId() {
				return "myTestsListenerIdA";
			}
			public void notify(Event event) {
				countDownLatch.countDown();
				
			}
		};
		spaceA.notify(new String[] { "aKey"}, new String[0], listener1 , new Event.Type[] { Type.WRITE, Type.UPDATE}, -1);
		
		EventListener listener2 = new EventListener(){
			public String getId() {
				return "myTestsListenerIdB";
			}
			public void notify(Event event) {
				countDownLatch.countDown();
				
			}
		};
		spaceA.notify(new String[] { "aKey"}, new String[0], listener2 , new Event.Type[] { Type.WRITE, Type.UPDATE}, -1);
		
		pause();
		
		spacePeerC = new SpacePeer(uri);
		spaceC = spacePeerC.createSpace(SpacePeer.DEFAULT_SPACE, true, reuseClusterPort);
		spacePeerC.start();
		
		
		spaceC.addPeer(spaceA.getReplicationURI());
		spaceC.addPeer(spaceB.getReplicationURI());
		spaceA.addPeer(spaceC.getReplicationURI());
		spaceB.addPeer(spaceC.getReplicationURI());
		
		// wait for the sync to happen
		pause();
		
		// Only the first is a write
		spaceC.write("aKey", "A,BundleServiceAllocator532878912", -1);
		spaceC.write("aKey", "B,BundleServiceAllocator532878912", -1);
		spaceC.write("aKey", "C,BundleServiceAllocator532878912", -1);
		countDownLatch.await(10, TimeUnit.SECONDS);
		spacePeerC.stop();
        spacePeerB.stop();
		assertEquals(0, countDownLatch.getCount());
	}
	
	@Test
	public void testUpdateNotificationsCallsToNewJoiner() throws Exception {
        final CountDownLatch countDownLatch = new CountDownLatch(8);
		System.out.println("===================================================== new space starting........");
		spacePeerC = new SpacePeer(uri);
		spaceC = spacePeerC.createSpace(SpacePeer.DEFAULT_SPACE, true, reuseClusterPort);
		spacePeerC.start();
		
		spaceC.addPeer(spaceA.getReplicationURI());
		spaceC.addPeer(spaceB.getReplicationURI());
		spaceA.addPeer(spaceC.getReplicationURI());
		spaceB.addPeer(spaceC.getReplicationURI());
		
		System.out.println("===================================================== new space started........");
		// wait for the sync to happen
		pause();
		
		EventListener listener = new EventListener(){
			public String getId() {
				return "myTestsListenerId";
			}
			public void notify(Event event) {
				countDownLatch.countDown();
				
			}
		};
		spaceA.notify(new String[] { "aKey"}, new String[0], listener , new Event.Type[] { Type.WRITE, Type.UPDATE}, -1);
		pause();
		
		
		// Only the first is a write
		
		System.out.println("--------------------------- 1 -------------------");
		spaceA.write("aKey", "value1", -1);
		System.out.println("--------------------------- 2 -------------------");
		spaceB.write("aKey", "value2", -1);
		System.out.println("--------------------------- 3 -------------------");
		spaceA.write("aKey", "value3", -1);
		System.out.println("--------------------------- 4 -------------------");
		spaceB.write("aKey", "value3", -1);
		System.out.println("--------------------------- 5 -------------------");
		spaceA.write("aKey", "value1", -1);
		System.out.println("--------------------------- 6 -------------------");
		spaceB.write("aKey", "value2", -1);
		System.out.println("--------------------------- 7 -------------------");
		spaceA.write("aKey", "value3", -1);
		System.out.println("--------------------------- 8 -------------------");
		spaceB.write("aKey", "value3", -1);
		countDownLatch.countDown();
		spacePeerC.stop();
		assertTrue(countDownLatch.await(5, TimeUnit.SECONDS));
	}
	
	@Test
	public void testUpdateMultipleNotificationsCallsFromNewJoiner() throws Exception {
        final CountDownLatch countDownLatch = new CountDownLatch(6);
		EventListener listener = new EventListener(){
			public String getId() {
				return "myTestsListenerId";
			}
			public void notify(Event event) {
				countDownLatch.countDown();
				
			}
		};
		spaceA.notify(new String[] { "aKey"}, new String[0], listener , new Event.Type[] { Type.WRITE, Type.UPDATE}, -1);
		spaceB.notify(new String[] { "aKey"}, new String[0], listener , new Event.Type[] { Type.WRITE, Type.UPDATE}, -1);
		
		
		pause();
		
		spacePeerC = new SpacePeer(uri);
		spaceC = spacePeerC.createSpace(SpacePeer.DEFAULT_SPACE, true, reuseClusterPort);
		spacePeerC.start();
		
		spaceC.addPeer(spaceA.getReplicationURI());
		spaceC.addPeer(spaceB.getReplicationURI());
		spaceA.addPeer(spaceC.getReplicationURI());
		spaceB.addPeer(spaceC.getReplicationURI());
		
		// wait for the sync to happen
		pause();
		
		// Only the first is a write
		spaceC.write("aKey", "value1   vvvvvvvvvvvvvvvvvvvvvvv", -1);
		spaceC.write("aKey", "value2", -1);
		spaceC.write("aKey", "value3", -1);
        countDownLatch.await(10, TimeUnit.SECONDS);
        spacePeerC.stop();
        assertEquals(0, countDownLatch.getCount());
	}
}
