package com.liquidlabs.space.impl;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Set;

import com.liquidlabs.space.Space;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.liquidlabs.common.net.URI;
import com.liquidlabs.space.lease.Lease;

public class SpacePersistClusterTest extends SpaceBaseFunctionalTest {

    long timeout = 300;

    @Before
	public void setUp() throws Exception {
		defaultSpaceSize = 20;
		System.setProperty(Lease.PROPERTY, "1");
		System.setProperty("vspace.snapshot.interval.secs", "3");
		System.setProperty("base.space.dir","./test-data/LogSpace-Persist");
		super.persistent = true;
		super.setUp();
	}
	int count = 0;

	@After
	public void tearDown() throws Exception {
        super.tearDown();
	}
	
//	@Test
//	public void shouldLoadFromDisk() throws Exception {
//		SpacePeer spacePeer = new SpacePeer(new URI("stcp://localhost:17010"));
//		Space space = spacePeer.createSpace("LogSpace-SPACE", 20 * 1024, true, persistent, reuseClusterPort);
//		spacePeer.start();
//		try {
//			Set<String> keySet = space.keySet();
//			System.out.println("Keys:" + keySet);
//		} finally {
//			try {
//				space.stop();
//				spacePeer.stop();
//			} catch (Exception e) {
//
//			}
//		}
//	}
	
	@Test
	public void testListRead() throws Exception {
        
		spaceA.write("aKey1", "aValue1", -1);

		// why oh why?
		spaceB.write("bKey1", "aValue1", -1);
		pause();
		
		String[] keys = new String [] { "aKey1", "bKey1"};
		String[] results = spaceB.read(keys);
		assertEquals(2, results.length);
		
		results = spaceA.read(keys);
		assertEquals(2, results.length);

		
		// wait for a snapshot to write
		pauseSecs(4);
		
		// now shutdown and then restart the cluster to see if they both have the data
		spaceA.stop();
		spacePeerA.stop();
		
		spaceB.stop();
		spacePeerB.stop();	
		
		pauseSecs(1);
		
		System.setProperty("base.space.dir","./build/spaceFunc/spaceA");
		spacePeerA = new SpacePeer(new URI("stcp://localhost:17010"));
		spaceA = spacePeerA.createSpace(SpacePeer.DEFAULT_SPACE, 20 * 1024, true, persistent, reuseClusterPort);
		spacePeerA.start();
		
		System.setProperty("base.space.dir","./build/spaceFunc/spaceB");
		spacePeerB = new SpacePeer(new URI("stcp://localhost:17020"));
		spaceB = spacePeerB.createSpace(SpacePeer.DEFAULT_SPACE, 20 * 1024, true, persistent, reuseClusterPort);
		spacePeerB.start();
		
		spaceA.addPeer(spaceB.getReplicationURI());
		spaceB.addPeer(spaceA.getReplicationURI());
		
		// wait for them to cluster
		pause(2);
		
		// TEST IT
		spaceA.write("aKey2", "aValue2", -1);
		spaceB.write("bKey2", "aValue2", -1);
		
		keys = new String [] { "aKey1", "aKey2", "bKey1", "bKey2"};
		results = spaceB.read(keys);
		assertEquals(4, results.length);
		
		results = spaceA.read(keys);
		assertEquals(4, results.length);
		
		String[] resultsA = spaceA.read(keys);
		assertEquals(4, resultsA.length);
		assertNotNull(resultsA[0]);
		
		String[] resultsB = spaceB.read(keys);
		assertEquals(4, resultsB.length);
		assertNotNull(resultsB[0]);
		
	}

	public void pauseSecs(int secs) throws InterruptedException {
		Thread.sleep(secs * 1000);
	}
}
