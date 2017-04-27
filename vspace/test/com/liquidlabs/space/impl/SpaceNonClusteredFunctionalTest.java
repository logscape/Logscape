package com.liquidlabs.space.impl;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.liquidlabs.common.net.URI;
import com.liquidlabs.space.Space;
import com.liquidlabs.space.lease.Lease;

public class SpaceNonClusteredFunctionalTest {

	protected int PAUSE = 500;
	protected int START_PAUSE = 3000;
	protected SpacePeer spacePeerA;
	protected SpacePeer spacePeerB;
	protected long expires = -1;
	protected long startTime;
	protected Space spaceA;
	protected Space spaceB;

	@Before
	public void setUp() throws Exception {
		System.out.println("================================== setup:" + "NonClusteredTest");
		System.setProperty(Lease.PROPERTY, "1");
		spacePeerA = new SpacePeer(new URI("stcp://localhost:15010"));
		spaceA = spacePeerA.createSpace(SpacePeer.DEFAULT_SPACE, false, true);
		spacePeerA.start();
		spacePeerB = new SpacePeer(new URI("stcp://localhost:15020"));
		spaceB = spacePeerB.createSpace(SpacePeer.DEFAULT_SPACE, false, true);
		spacePeerB.start();
		
		Thread.sleep(START_PAUSE);
		startTime = System.currentTimeMillis();
		System.out.println("================================= " + getClass().getName());
	}

	@After
	public void tearDown() throws Exception {
		System.out.println("================================= teardown " + getClass().getName());
		long endTime = System.currentTimeMillis();
		long elapseSeconds = (endTime - startTime)/1000;
		System.err.println(" ***** " +  getClass().getName() + " =" + elapseSeconds + "sec *****");
		spacePeerA.stop();
		spacePeerB.stop();
		pause(100);
	}
	
	
	@Test
	public void testReadShouldNotHappendBetweenClusters() throws Exception {
		spaceA.write("someKey", "value", -1);
		
		pause();
		
		String read = spaceB.read("someKey");
		
		Assert.assertNull(read);
	}
	

	public void pause() throws InterruptedException {
		Thread.sleep(PAUSE);
	}
	protected void pause(int i) {
		try {
			Thread.sleep(i);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
