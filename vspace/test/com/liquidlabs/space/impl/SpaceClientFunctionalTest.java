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
import com.liquidlabs.transport.proxy.RemoteEventListener;
import com.liquidlabs.transport.proxy.events.Event;
import com.liquidlabs.transport.proxy.events.EventListener;
import com.liquidlabs.transport.proxy.events.Event.Type;

public class SpaceClientFunctionalTest extends SpaceBaseFunctionalTest {

	private ProxyFactoryImpl proxyFactoryA;
	private Space spaceProxyA;
	private Space spaceProxyB;
	private ProxyFactoryImpl proxyFactoryB;
	TransportFactory transportFactory = new TransportFactoryImpl(Executors.newFixedThreadPool(10), "test");
	ExecutorService executor = Executors.newFixedThreadPool(5);
	int count = 0;

	@Before
	public void setUp() throws Exception {
		System.setProperty(Lease.PROPERTY, "1");
		super.setUp();
		transportFactory.start();
		
		proxyFactoryA = new ProxyFactoryImpl(transportFactory, TransportFactoryImpl.getDefaultProtocolURI("", "localhost", 11111, "serviceName"), executor, "");
		proxyFactoryA.start();
		spaceProxyA = proxyFactoryA.getRemoteService(SpacePeer.DEFAULT_SPACE, Space.class, new String [] { spacePeerA.getClientAddress().toString() });

		proxyFactoryB = new ProxyFactoryImpl(transportFactory, TransportFactoryImpl.getDefaultProtocolURI("", "localhost", 22222, "serviceName"), executor, "");
		proxyFactoryB.start();
		spaceProxyB = proxyFactoryB.getRemoteService(SpacePeer.DEFAULT_SPACE, Space.class, new String[] { spacePeerB.getClientAddress().toString() });
	}

	@After
	public void tearDown() throws Exception {
		super.tearDown();
		transportFactory.stop();
		proxyFactoryA.stop();
		proxyFactoryB.stop();
	}
	
	@Test
	public void testNotificationCallsBack() throws Exception {
		pause();
		EventListener listener = new RemoteEventListener("listenerID"){
			public void notify(Event event) {
				System.err.println(count++ + " ********************* Got event:" + event);
			}
		};
		
		spaceA.write("aKey", "value", -1);
		spaceProxyA.notify(new String[] { "aKey"}, new String[0], listener , new Event.Type[] { Type.READ}, -1);
		pause();
		spaceA.read("aKey");
		spaceB.read("aKey");
		spaceB.read("aKey");
		spaceA.read("aKey");
		spaceB.read("aKey");
		pause();
		assertEquals("Count should have been incremented to 5", 5, count);
	}
	
	@Test
	public void testWriteIsSeenInSpace() throws Exception {
		
		spaceProxyA.write("A", "someValue", expires);
		pause();
		String string = spaceA.read("A");
		assertEquals("someValue", string);
	}
	
	@Test
	public void testReadWorks() throws Exception {
		spaceA.write("A", "someValue", expires);
		String string = spaceProxyA.read("A");
		pause();
		assertEquals("someValue", string);
	}
	


	@Test
	public void testListRead() throws Exception {
		spaceProxyA.write("aKey1", "aValue1", -1);
		spaceProxyA.write("aKey2", "aValue2", -1);
		spaceProxyA.write("aKey3", "aValue3", -1);
		pause();
		
		String[] stuff = new String [] { "aKey1", "aKey2", "aKey3" };
		String[] results = spaceA.read(stuff);
		assertEquals(3, results.length);
	}
}
