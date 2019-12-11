package com.liquidlabs.vso;

import static org.junit.Assert.assertNotNull;

import java.util.concurrent.Executors;

import com.liquidlabs.common.NetworkUtils;
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
 * Setup 2 different types (A & B) of Space types and let both pairs use the same ports (nodes One and Two)
 * @author neil
 *
 */
public class SpaceServicesShareProxyFactoryPortsTest {

	Mockery mockery = new Mockery();
	private LookupSpace lookup;
	private ORMapperFactory mapperFactory;
	private SpaceServiceImpl spaceAOne;
	private SpaceServiceImpl spaceBOne;
	
	private SpaceServiceImpl spaceATwo;
	private SpaceServiceImpl spaceBTwo;
	private ORMapperFactory mapperFactory2;

	@Before
	public void setUp() throws Exception {
		
		try {
		
			lookup = mockery.mock(LookupSpace.class);
	
			mockery.checking(new Expectations() {
			{
					atLeast(1).of(lookup).registerService(with(any(ServiceInfo.class)), with(any(Long.class)));
	                atLeast(1).of(lookup).registerUpdateListener(with(any(String.class)), with(any(AddressUpdater.class)), with(any(String.class)), with(any(String.class)), with(any(String.class)), with(any(String.class)), with(any(Boolean.class)));
	                atLeast(1).of(lookup).unregisterService(with(any(ServiceInfo.class)));
	                atLeast(1).of(lookup).renewLease(with(any(String.class)), with(any(int.class)));
				}
			});
	
			System.out.println(">>>>>>>>>> Starting Instance ONE");
            final int p1 = NetworkUtils.determinePort(10204);
            this.mapperFactory = new ORMapperFactory(p1, "SERVICE", p1);
	
			spaceAOne = new SpaceServiceImpl(lookup, mapperFactory, "SERVICE-A", mapperFactory.getScheduler(), true, false, false);
			spaceAOne.start(this, "test-1.0");
			
			spaceBOne = new SpaceServiceImpl(lookup, mapperFactory, "SERVICE-B", mapperFactory.getScheduler(), true, false, true);
			spaceBOne.start(this, "test-1.0");


            final int p2 = NetworkUtils.determinePort(11204);
            this.mapperFactory2 = new ORMapperFactory(p2, "SERVICE", p2);
			
			System.out.println(">>>>>>>>>> Starting Instance TWO");
			
			spaceATwo = new SpaceServiceImpl(lookup, mapperFactory2, "SERVICE-A", mapperFactory2.getScheduler(), true, false, false);
			spaceATwo.start(this, "test-1.0");
			
			spaceBTwo = new SpaceServiceImpl(lookup, mapperFactory2, "SERVICE-B", mapperFactory2.getScheduler(), true, false, true);
			spaceBTwo.start(this, "test-1.0");

			
			spaceAOne.addPeer(spaceATwo.getReplicationURI());
			spaceBOne.addPeer(spaceBTwo.getReplicationURI());
			
			spaceATwo.addPeer(spaceAOne.getReplicationURI());
			spaceBTwo.addPeer(spaceBOne.getReplicationURI());
			
			Thread.sleep(1 * 1000);
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}

	@After
	public void tearDown() throws Exception {
		try {
			mapperFactory.stop();
			mapperFactory2.stop();
			spaceAOne.stop();
			spaceBOne.stop();
			spaceATwo.stop();
			spaceBTwo.stop();
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}

	public int count = 0;

	
	@Test
	public void testShouldWriteInOneAndReadInTheOther() throws Exception {
		
		System.out.println("A Writting Hello:::::::::::::::::::::");
		spaceAOne.store(new Stuff("helloA"), -1);
		
		Thread.sleep(500);
		System.out.println("A Reading Hello:::::::::::::::::::::");

		Stuff findById = spaceATwo.findById(Stuff.class, "helloA");
		assertNotNull("Should have found Stuff instance 'hello'",findById);
		

		System.out.println("B Writting Hello:::::::::::::::::::::");
		spaceBOne.store(new Stuff("helloB"), -1);
		
		Thread.sleep(500);
		System.out.println("B Reading Hello:::::::::::::::::::::");

		Stuff findById2 = spaceBTwo.findById(Stuff.class, "helloB");
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
