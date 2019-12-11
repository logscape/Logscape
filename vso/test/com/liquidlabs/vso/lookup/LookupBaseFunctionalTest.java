package com.liquidlabs.vso.lookup;

import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.common.net.URI;

import junit.framework.TestCase;

public class LookupBaseFunctionalTest extends TestCase {
	protected LookupSpaceImpl lookupSpaceA;
	protected LookupSpaceImpl lookupSpaceB;
	
	protected void setUp() throws Exception {
		com.liquidlabs.common.concurrent.ExecutorService.setTestMode();
		super.setUp();
		System.out.println("=======================SETUP===================" + getName() + "=============================");
		lookupSpaceA = new LookupSpaceImpl(11000, 15000);
		lookupSpaceB = new LookupSpaceImpl(12000, 25000);
		lookupSpaceA.start();
		lookupSpaceB.start();
		lookupSpaceA.addLookupPeer(new URI("stcp://localhost:25000"));
		lookupSpaceB.addLookupPeer(new URI("stcp://localhost:15000"));
		System.out.println("=======================STARTING===================" + getName() + "=============================");
		Thread.sleep(3 * 1000);
	}
	
	protected void tearDown() throws Exception {
		System.out.println("=======================TEARDOWN===================" + getName() + "=============================");
		lookupSpaceA.stop();
		lookupSpaceB.stop();
		Thread.sleep(500);
	}
	public void pause(){
		try {
			Thread.sleep(200);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
	public void testSweetFA() throws Exception {
	}
}
