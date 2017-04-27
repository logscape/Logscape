package com.liquidlabs.vso;

import static org.junit.Assert.*;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.net.URI;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.liquidlabs.orm.Id;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.transport.proxy.AddressUpdater;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.ServiceInfo;

/**
 * Starts Cluster, kills-A, check stuff on-B, writes stuff to B, start a new-A and read B items from it.
 * @author Neiil
 *
 */
public class SpaceServicePeerFailBackTest {

	Mockery mockery = new Mockery();
	private LookupSpace lookup;
	private ORMapperFactory mapperOne;
	private SpaceServiceImpl spaceOne;
	private SpaceServiceImpl spaceTwo;
	private ORMapperFactory mapperTwo;
	ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);

	@Before
	public void setUp() throws Exception {
		
		try {
			com.liquidlabs.common.concurrent.ExecutorService.setTestMode();
			lookup = mockery.mock(LookupSpace.class);
	
			mockery.checking(new Expectations() {
			{
					atLeast(1).of(lookup).registerService(with(any(ServiceInfo.class)), with(any(Long.class)));
	                atLeast(1).of(lookup).registerUpdateListener(with(any(String.class)), with(any(AddressUpdater.class)), with(any(String.class)), with(any(String.class)), with(any(String.class)), with(any(String.class)), with(any(Boolean.class)));
	                atLeast(1).of(lookup).unregisterService(with(any(ServiceInfo.class)));
				}
			});

            final int p1 = NetworkUtils.determinePort(11111);
            this.mapperOne = new ORMapperFactory(p1, "SERVICE", p1);
	
			spaceOne = new SpaceServiceImpl(lookup, mapperOne, "SERVICE", scheduler, true, false, true);
			spaceOne.start();
			spaceOne.start(this, "test-1.0");

            final int p2 = NetworkUtils.determinePort(22222);
            this.mapperTwo = new ORMapperFactory(p2, "SERVICE", p2);
			spaceTwo = new SpaceServiceImpl(lookup, mapperTwo, "SERVICE", scheduler, true, false, true);
			spaceTwo.start();
			spaceTwo.start(this, "test-1.0");
			
			spaceOne.addPeer(spaceTwo.getReplicationURI());
			spaceTwo.addPeer(spaceOne.getReplicationURI());
			Thread.sleep(3 * 1000);
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}

	@After
	public void tearDown() throws Exception {
		spaceOne.stop();
		spaceTwo.stop();
	}

	public int count = 0;

	
	@Test
	public void testShouldWriteInOneAndReadInTheOther() throws Exception {
		
		System.out.println("Writing HelloA:::::::::::::::::::::");
		spaceOne.store(new Stuff("helloAAAAAAAAAA"), -1);
		Thread.sleep(500);
		spaceOne.stop();
		mapperOne.stop();
		
		System.out.println("Reading HelloA:::::::::::::::::::::");

		
		Stuff findById = spaceTwo.findById(Stuff.class, "helloAAAAAAAAAA");
		assertNotNull("Should have found Stuff instance 'hello'",findById);
		
		Thread.sleep(3 * 1000);
		
	
		
		spaceTwo.store(new Stuff("helloBBBBBBBBBB"), -1);

		// Fire up new One
		System.out.println("\n\n********* Start NEW One:::::::::::::::::::::");
		
//		 final int p1 = NetworkUtils.determinePort(33333);
		final int p1 = NetworkUtils.determinePort(11111);
         this.mapperOne = new ORMapperFactory(p1, "SERVICE", p1);
		spaceOne = new SpaceServiceImpl(lookup, mapperOne, "SERVICE", Executors.newScheduledThreadPool(10), true, false, true);
		spaceOne.start();
		spaceOne.start(this, "test-1.0");
		Thread.sleep(500);
		spaceOne.addPeer(spaceTwo.getReplicationURI());
		spaceTwo.addPeer(new URI(spaceOne.getReplicationURI().toString() + "?NEWWWW"));
		Thread.sleep(5 * 1000);
		
		System.out.println("********** Reading HelloA:::::::::::::::::::::");
		
		Stuff findById2 = spaceOne.findById(Stuff.class, "helloAAAAAAAAAA");
		assertNotNull("Should have found Stuff instance 'hello'",findById2);
		
		
	}

	
	public static class Stuff {

		public Stuff() {
		}

		public Stuff(String string) {
			id = string;
		}

		@Id
		String id;
	}

}
