package com.liquidlabs.space.impl;

import java.util.Set;

import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.space.Space;
import com.liquidlabs.space.lease.Lease;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SpaceClusterFailoverFailbackTest  {

	protected int PAUSE = 200;
	protected int START_PAUSE = 1000;
	protected SpacePeer spacePeerA;
	protected SpacePeer spacePeerB;
	protected long expires = -1;
	protected long startTime;
	protected Space spaceA;
	protected Space spaceB;
	private URI spaceONE_URI;
	private URI spaceTWO_URI;
	private SpacePeer spacePeerC;
	private Space spaceC;

	@Before
	public void setUp() throws Exception {
        ExecutorService.setTestMode();
		System.setProperty(Lease.PROPERTY, "1");
		spaceONE_URI = new URI("stcp://localhost:15111");
		spaceTWO_URI = new URI("stcp://localhost:15222");
	}
	@After
	public void tearDown() throws Exception {
		long endTime = System.currentTimeMillis();
		long elapseSeconds = (endTime - startTime)/1000;
		System.err.println(" ***** " + getClass().getSimpleName() + " =" + elapseSeconds + "sec *****");
		spacePeerA.stop();
		spacePeerB.stop();
		pause();
	}


	@Test
	public void testShouldSynSmallAmountsOfData() throws Exception {
		System.setProperty("space.no.sync.gap", "5");
		int testAmount = 50;
		
		
		// Start SPACE_A with some data
		spacePeerA = new SpacePeer(spaceONE_URI);
		spaceA = spacePeerA.createSpace(SpacePeer.DEFAULT_SPACE, true, false);
		spacePeerA.start();
		
		for (int i = 0; i < testAmount; i++) {
			spaceA.write(i + "-key", i + "-value", -1);
		}
		
		// Start SPACE_B 
		spacePeerB = new SpacePeer(spaceTWO_URI);
		spaceB = spacePeerB.createSpace(SpacePeer.DEFAULT_SPACE, true, false);
		spacePeerB.start();
		
		// make them join and Cluster - copy data
		spaceA.addPeer(spaceB.getReplicationURI());
		spaceB.addPeer(spaceA.getReplicationURI());

        waitTillGotEvents(testAmount, spaceB);

		assertEquals(testAmount, spaceB.keySet().size());
		
		// Kill SpaceA
		
		System.out.println("\n\n" + new DateTime() + " ============== STOP SPACE-A ");
		spacePeerA.stop();
		spaceA.stop();
		
		// need to wait for the sync delay of 5 seconds to pass
		System.out.println("Waiting.....");
		Thread.sleep(6000);
		
		// write an event to SpaceB
		spaceB.write("99999-key", "99999-value", -1);

		spacePeerC = new SpacePeer(spaceONE_URI);
		spaceC = spacePeerC.createSpace(SpacePeer.DEFAULT_SPACE, true, false);
		spacePeerC.start();
		System.out.println("\n\n" + new DateTime() + " ============== STAR SPACE-C (A - Replacement) ");
		
		spaceC.addPeer(spaceB.getReplicationURI());

        waitTillGotEvents(testAmount +1, spaceC);

        Set<String> keySet = spaceB.keySet();
        System.out.println("\n\nB KEYS:" + keySet.size() + " values:" + keySet);
        
        Set<String> keySet2 = spaceC.keySet();
        System.out.println("\n\nC KEYS:" + keySet2.size() + " values:" + keySet2);
        
        assertEquals("Data did not replicate back to other instance, got:" + spaceC.keySet().size(), testAmount + 1, spaceC.keySet().size());
	}

    private void waitTillGotEvents(int testAmount, Space space) throws InterruptedException {
        int i =0;
        while(space.keySet().size() < testAmount && i++ < 10) {
            Thread.sleep(100);

        }
    }

	public void pause() throws InterruptedException {
		Thread.sleep(500);
	}


}
