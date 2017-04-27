package com.liquidlabs.space.impl;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import com.liquidlabs.common.concurrent.ExecutorService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.space.Space;
import com.liquidlabs.space.lease.Lease;

public class SpaceClusterDataReliabilityTest  {

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
	boolean persistent = false;
	private boolean reuseClusterPort = true;

	@Before
	public void setUp() throws Exception {
		ExecutorService.setTestMode();
		String spaceDir = "build/" + getClass().getSimpleName();
		System.setProperty("base.space.dir", spaceDir);
		
		System.setProperty(Lease.PROPERTY, "1");
		spaceONE_URI = new URI("stcp://localhost:15111");
		spaceTWO_URI = new URI("stcp://localhost:15222");
	}
	@After
	public void tearDown() throws Exception {
		long endTime = System.currentTimeMillis();
		long elapseSeconds = (endTime - startTime)/1000;
		System.err.println(" ***** " + getClass().getSimpleName() + " =" + elapseSeconds + "sec *****");
		try {
			spacePeerA.stop();
			spacePeerB.stop();
		} catch (Throwable t) {
			t.printStackTrace();
		}
		pause();
	}


	
	@Test
	public void shouldHandleLeasedDataProperly() throws Exception {
			
		long leasePeriod = 5;
		int testAmount = 0;
		startSpaceA(testAmount);
		startSpaceB(testAmount);
		String leaseKey = spaceA.write("-A", "-value", leasePeriod);
		
		// wait for replication
		pauseSecs(3);
		String readA0 = spaceA.read("-A");
		assertNotNull(readA0);
		String readB0 = spaceB.read("-A");
		assertNotNull(readB0);

		// TEST 1
		spaceA.renewLease(leaseKey, leasePeriod);
		
		// wait for the new lease period to start and see if the dara is still there
		pauseSecs(3);
		readA0 = spaceA.read("-A");
		assertNotNull("Lease failed", readA0);
		readB0 = spaceB.read("-A");
		assertNotNull("Lease failed", readB0);
		
		// TEST 2
		spaceA.renewLease(leaseKey, leasePeriod);
		
		// wait for the new lease period to start and see if the dara is still there
		pauseSecs(3);
		readA0 = spaceA.read("-A");
		assertNotNull("Lease failed", readA0);
		readB0 = spaceB.read("-A");
		assertNotNull("Lease failed", readB0);

		
		
		
	}
	
	@Test
	public void testWriteWhileStartup() throws Exception {
		final int testAmount = 10;
		
		new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					startSpaceA(testAmount);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (URISyntaxException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			
		}).start();
		
		
		startSpaceB(testAmount);
		
		waitTillGotEvents(testAmount * 2, spaceB);
        waitTillGotEvents(testAmount * 2, spaceA);

        System.out.println("SpaceA:" + spaceA.keySet());
        System.out.println("SpaceB:" + spaceB.keySet());
		assertEquals("B failed", testAmount *2, spaceB.keySet().size());
		assertEquals("A failed", testAmount *2, spaceA.keySet().size());
		
		
	}
	
	@Test
	public void testShouldSyncBackAndForth() throws Exception {
		
		int testAmount = 100;
		startSpaceA(testAmount);
		
		startSpaceB(testAmount);
		
        waitTillGotEvents(testAmount * 2, spaceB);
        waitTillGotEvents(testAmount * 2, spaceA);

		assertEquals(testAmount *2, spaceB.keySet().size());
		assertEquals(testAmount *2, spaceA.keySet().size());
		
		// This breaks the TEST - stop is Bad....
		spaceA.stop();

		writeToSpace(spaceB, "B2", testAmount);
		
		//spaceA.start();
		
		waitTillGotEvents(testAmount * 3, spaceA);
		assertEquals(testAmount *3, spaceA.keySet().size());
		
		spaceB.stop();
		
		writeToSpace(spaceA, "A'", testAmount);
		
		spaceB.start();
		
		waitTillGotEvents(testAmount * 4, spaceB);
		assertEquals(testAmount * 4, spaceB.keySet().size());
		
	}
	private void writeToSpace(Space spaceB, String prefix, int testAmount) {
		for(int i = 0; i < testAmount; i ++) {
			spaceB.write(prefix + "-" + i, String.valueOf(i), -1);
		}
		
	}
	private void startSpaceB(int testAmount) throws IOException, URISyntaxException {
		
		System.err.println("B ====================================BBB ");
		
		// Start SPACE_B 
		System.setProperty("base.space.dir", "build/nodeB");
		FileUtil.deleteDir(new File("build/nodeB"));

		spacePeerB = new SpacePeer(spaceTWO_URI);
		spaceB = spacePeerB.createSpace(SpacePeer.DEFAULT_SPACE, 20 * 1024, true, persistent, reuseClusterPort );
		spacePeerB.start();
		spaceB.take(Arrays.toStringArray(spaceB.keySet()));
		spaceB.addPeer(spaceONE_URI);
		for (int i = 0; i < testAmount; i++) {
			spaceB.write(i + "-B", i + "-value", -1);
		}
	}
	private void startSpaceA(int testAmount) throws IOException, URISyntaxException {

		System.err.println("A ====================================AAA ");
		// Start SPACE_A with some data
		System.setProperty("base.space.dir", "build/nodeA");
		FileUtil.deleteDir(new File("build/nodeA"));
		
		spacePeerA = new SpacePeer(spaceONE_URI);
		spaceA = spacePeerA.createSpace(SpacePeer.DEFAULT_SPACE, 20 * 1024, true, persistent, reuseClusterPort);
		spacePeerA.start();
		spaceA.take(Arrays.toStringArray(spaceA.keySet()));
		spaceA.addPeer(spaceTWO_URI);
		
		for (int i = 0; i < testAmount; i++) {
			spaceA.write(i + "-A", i + "-value", -1);
		}
	}

    private void waitTillGotEvents(int testAmount, Space space) throws InterruptedException {
        int i =0;
        while(space == null && i++ < 10 || 
        		space != null && space.keySet().size() < testAmount && i++ < 30) {
            Thread.sleep(500);
        }
    }
	public void pause() throws InterruptedException {
		Thread.sleep(500);
	}
	private void pauseSecs(int i) throws InterruptedException {
		Thread.sleep(i * 1000);		
	}

}
