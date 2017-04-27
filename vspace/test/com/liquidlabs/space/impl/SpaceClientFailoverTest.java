package com.liquidlabs.space.impl;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.liquidlabs.space.Space;
import com.liquidlabs.space.lease.Lease;
import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.TransportFactoryImpl;
import com.liquidlabs.transport.proxy.ProxyFactoryImpl;

/**
 * verify client redirects to the other space AND the other space has valid contents.
 *
 */
public class SpaceClientFailoverTest extends SpaceBaseFunctionalTest {

	long timeout = 300;
	
	TransportFactory transportFactory = new TransportFactoryImpl(Executors.newFixedThreadPool(10), "test");
	ExecutorService executor = Executors.newFixedThreadPool(5);

	private ProxyFactoryImpl proxyFactoryA;

	private Space spaceProxyA;
	
	@Before
	public void setUp() throws Exception {
		System.setProperty(Lease.PROPERTY, "1");
		super.setUp();
		
		
		proxyFactoryA = new ProxyFactoryImpl(transportFactory, TransportFactoryImpl.getDefaultProtocolURI("", "localhost",11111, "serviceName"), executor, "");
		proxyFactoryA.start();
		spaceProxyA = proxyFactoryA.getRemoteService(SpacePeer.DEFAULT_SPACE, Space.class, new String [] { spacePeerA.getClientAddress().toString(), spacePeerB.getClientAddress().toString() });
	}
	
	@After
	public void tearDown() throws Exception {
		super.tearDown();
	}
	
	@Test
	public void testWriteIsSeenInPeer() throws Exception {
		
		int work = 10;
		for (int i = 0; i < work; i++) {
			System.out.println("************* WRITE :" + i);
			spaceProxyA.write("A"+ i, "someValue", expires);
		}
		pause();
		
		// stop the first space to make the client failover and pickup clustered data from the second
		System.out.println("Stopping:" + spacePeerA);
		spacePeerA.stop();
		
		String[] string = spaceProxyA.readMultiple(new String[] { "all:" }, -1);
		assertEquals(work, string.length);
	}
	
	@Test
	public void testWriteLeaseExpires() throws Exception {
		System.out.println("\n\n =====================================================");
		spaceProxyA.write("A",  "someValue", 5);
		Thread.sleep(100);
		System.out.println("\n\n ====WAIT=================================================");
		spacePeerA.stop();
		
		Thread.sleep(1000);
		String[] string = spaceProxyA.readMultiple(new String[] { "all:" }, -1);
		assertEquals("Should have received an item", 1, string.length);
		Thread.sleep(6000);
		String[] string2 = spaceProxyA.readMultiple(new String[] { "all:" }, -1);
		assertEquals("Should have expired", 0, string2.length);
	}
	
}
