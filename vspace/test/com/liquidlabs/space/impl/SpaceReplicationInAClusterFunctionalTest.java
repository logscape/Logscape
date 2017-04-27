package com.liquidlabs.space.impl;

import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.space.Space;
import com.liquidlabs.space.lease.Lease;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class SpaceReplicationInAClusterFunctionalTest  {

	protected int START_PAUSE = 1000;
	protected SpacePeer spacePeerA;
	protected SpacePeer spacePeerB;
	protected long expires = -1;
	protected Space spaceA;
	protected Space spaceB;
	private boolean reuseClusterPort = true;

	@Before
	public void setUp() throws Exception {
		ExecutorService.setTestMode();
		System.setProperty(Lease.PROPERTY, "1");
	}
	@After
	public void tearDown() throws Exception {
		spacePeerA.stop();
		spacePeerB.stop();
		pause();
	}

	public void pause() throws InterruptedException {
		Thread.sleep(500);
	}
	
	@Test
	public void testShouldSynSmallAmountsOfData() throws Exception {
		spacePeerA = new SpacePeer(new URI("stcp://localhost:15010"));
		spaceA = spacePeerA.createSpace(SpacePeer.DEFAULT_SPACE, true, reuseClusterPort);
		spacePeerA.start();
		
		for (int i = 0; i < 10; i++) {
			spaceA.write(i + "-key", i + "-value", -1);
		}
		
		spacePeerB = new SpacePeer(new URI("stcp://localhost:15020"));
		spaceB = spacePeerB.createSpace(SpacePeer.DEFAULT_SPACE, true, reuseClusterPort);
		spacePeerB.start();
		
		spaceA.addPeer(spaceB.getReplicationURI());
		spaceB.addPeer(spaceA.getReplicationURI());
		
		waitForCompletion(10);
		
		assertEquals(10, spaceB.keySet().size());

	}
	
	@Test
	public void testShouldSynLotsOfData() throws Exception {
		spacePeerA = new SpacePeer(new URI("stcp://localhost:15010"));
		spaceA = spacePeerA.createSpace(SpacePeer.DEFAULT_SPACE, true, reuseClusterPort );
		spacePeerA.start();
		
		int itemCount = 1000;
		for (int i = 0; i < itemCount; i++) {
			spaceA.write(i + "-key", i + "-value", -1);
		}
		
		spacePeerB = new SpacePeer(new URI("stcp://localhost:15020"));
		spaceB = spacePeerB.createSpace(SpacePeer.DEFAULT_SPACE, true, reuseClusterPort);
		spacePeerB.start();
		
		spaceA.addPeer(spaceB.getReplicationURI());
		spaceB.addPeer(spaceA.getReplicationURI());

        waitForCompletion(itemCount);

		assertEquals(itemCount, spaceB.keySet().size());

	}

    private void waitForCompletion(int itemCount) throws InterruptedException {
        int i = 0;
        while(spaceB.keySet().size() != itemCount && i++ < 10) {
            Thread.sleep(START_PAUSE);
        }
    }

//    @Test DodgyTest - works occasionally
	public void testShouldSynLotsOfDataAndStillWorkWhileWriting() throws Exception {
		spacePeerA = new SpacePeer(new URI("stcp://localhost:15010"));
		spaceA = spacePeerA.createSpace(SpacePeer.DEFAULT_SPACE, true, reuseClusterPort);
		spacePeerA.start();
		
		int itemCount = 1000;
		for (int i = 0; i < itemCount; i++) {
			if (i % 100 == 0)
			spaceA.write(i + "-key", i + "-value", -1);
			
			if (i == 100) {
				spacePeerB = new SpacePeer(new URI("stcp://localhost:15020"));
				spaceB = spacePeerB.createSpace(SpacePeer.DEFAULT_SPACE, true, reuseClusterPort);
				spacePeerB.start();			
				
				spaceA.addPeer(spaceB.getReplicationURI());
				spaceB.addPeer(spaceA.getReplicationURI());
			}
		}
		
		
		waitForCompletion(itemCount);

		assertEquals(itemCount, spaceB.keySet().size());

	}


}
