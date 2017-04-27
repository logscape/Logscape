package com.liquidlabs.space.impl;

import java.io.File;

import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.space.Space;
import com.liquidlabs.space.lease.Lease;
import com.liquidlabs.transport.TransportProperties;

public abstract class SpaceBaseFunctionalTest {

	protected int PAUSE = 500;
	protected int START_PAUSE = 1000;
	protected SpacePeer spacePeerA;
	protected SpacePeer spacePeerB;
	protected long expires = -1;
	protected long startTime;
	protected Space spaceA;
	protected Space spaceB;
	protected boolean persistent = false;
	boolean reuseClusterPort = true;
	protected int defaultSpaceSize = 20 * 1024;

	protected void setUp() throws Exception {
		System.gc();
		System.setProperty("test.mode", "true");
		System.setProperty("allow.read.events","true");
		System.setProperty(Lease.PROPERTY, "1");

		ExecutorService.setTestMode();
		FileUtil.deleteDir(new File("./build/spaceFunc"));
		
		System.out.println("================================== setup:" + getName());
		TransportProperties.setMCastEnabled(true);
		TransportProperties.setMCastTTL(1);

		System.setProperty("base.space.dir","./build/spaceFunc/spaceA");
		spacePeerA = new SpacePeer(new URI("stcp://localhost:15010"));
		spaceA = spacePeerA.createSpace(SpacePeer.DEFAULT_SPACE, defaultSpaceSize, true, persistent, reuseClusterPort);
		spacePeerA.start();
		
		System.setProperty("base.space.dir","./build/spaceFunc/spaceB");
		spacePeerB = new SpacePeer(new URI("stcp://localhost:15020"));
		spaceB = spacePeerB.createSpace(SpacePeer.DEFAULT_SPACE, defaultSpaceSize, true, persistent, reuseClusterPort);
		spacePeerB.start();
		
		spaceA.addPeer(spaceB.getReplicationURI());
		spaceB.addPeer(spaceA.getReplicationURI());
		doCustomSetUp();
		
		Thread.sleep(START_PAUSE);
		startTime = System.currentTimeMillis();
		System.out.println("================================= " + getClass().getName() + "." + getName() );
	}
	private int defaultSpaceSize() {
		return 20 * 1024;
	}
	private String getName() {
		return "SpaceFunctionalTest";
	}
	protected void doCustomSetUp(){
	}

	protected void tearDown() throws Exception {
		try {
			System.out.println("================================= teardown " + getClass().getName() + "." + getName() );
			long endTime = System.currentTimeMillis();
			long elapseSeconds = (endTime - startTime)/1000;
			System.err.println(" ***** " + getName() + " =" + elapseSeconds + "sec *****");
			spacePeerA.stop();
			if(spacePeerB!=null) spacePeerB.stop();
			pause();
			Thread.sleep(5 * 1000);
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}
	

	public void pause() throws InterruptedException {
		Thread.sleep(PAUSE);
	}
	public void pause(int i) {
		try {
			Thread.sleep(i);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

}
