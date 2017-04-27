package com.liquidlabs.transport.proxy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.liquidlabs.common.PIDGetter;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.TransportFactoryImpl;
import com.liquidlabs.transport.proxy.DummyServiceImpl.UserType;
import com.liquidlabs.transport.proxy.events.Event;
import com.liquidlabs.transport.proxy.events.Event.Type;
import com.liquidlabs.transport.serialization.Convertor;

public class ProxyRemoteCallbackInvocationsTest {
	private ProxyFactoryImpl proxyFactoryA;
	boolean enableOutput = false;
	private ProxyFactoryImpl proxyFactoryB;
	private URI proxyBAddress;
	private DummyService remoteService;
	TransportFactory transportFactory ;
	ExecutorService executor = Executors.newFixedThreadPool(5);
	Convertor c = new Convertor();
	
	@Before
	public void setUp() throws Exception {
		System.setProperty("tcp.use.oio.server", "true");
		System.setProperty("tcp.use.oio.client", "true");
		System.out.println("PID:" + PIDGetter.getPID());
		transportFactory = new TransportFactoryImpl(Executors.newFixedThreadPool(5), "test");
		proxyFactoryA = new ProxyFactoryImpl(transportFactory,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", 9999, "testServiceA"), executor, "");
		proxyFactoryA.start();
		
		Thread.sleep(100);
		
		TransportFactoryImpl transportFactory2 = new TransportFactoryImpl(Executors.newFixedThreadPool(5), "test");
		proxyFactoryB = new ProxyFactoryImpl(transportFactory2,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", 22222, "testServiceB"), executor, "");
		proxyFactoryB.registerMethodReceiver("methodReceiver", new DummyServiceImpl());
		proxyFactoryB.start();
		

		Thread.sleep(100);
		proxyBAddress = proxyFactoryB.getAddress();
		DummyServiceImpl.callCount = 0;
		remoteService = proxyFactoryA.getRemoteService("methodReceiver", DummyService.class, new String[] { proxyBAddress.toString() } );
		
		DummyServiceImpl.callCount = 0;
		System.out.println("********************************* "+ "TEST" + " ***************************");
	}
	@After
	public void tearDown() throws Exception {
		transportFactory.stop();
		proxyFactoryA.stop();
		proxyFactoryB.stop();
		Thread.sleep(50);
	}
	
	
	@Test
	public void testStreamInvocationsWork() throws Exception {
		final CountDownLatch latch = new CountDownLatch(10);
		NotifyInterface notifyInterface = new NotifyInterface(){
			public void notify(String payload) {
			    latch.countDown();
            }

			public String getId() {
				return "stuff";
			}
		};
		ContinuousEventListener eventListener = new ContinuousEventListener("notifyId", notifyInterface);
		proxyFactoryA.registerContinuousEventListener("notifyId", eventListener );
		

		remoteService.makeCallbackHappend(proxyFactoryA.getAddress().toString());
		
		assertTrue(latch.await(10, TimeUnit.SECONDS));
	}
	
	@Test
	public void testShouldSerializeRemoteProxy() throws Exception {
		DummyService remoteable = proxyFactoryA.makeRemoteable(remoteService);
		byte[] serialize = c.serialize(remoteable);
		Object cc = c.deserialize(serialize);
	}
	
	@Test
	public void testMultipleRemoteCallbacksWork() throws Exception {
		
		System.out.println("Sending....................");
		RemoteEventListener remoteEventListener1 = new RemoteEventListener("myListenerId1", 1);
		RemoteEventListener remoteEventListener2 = new RemoteEventListener("myListenerId2", 1);

        remoteService.registerCallback(remoteEventListener1);
		remoteService.registerCallback(remoteEventListener2);

        System.out.println(">>>>>>>>>> ProxyClientCount:" + proxyFactoryA.clients.size());
		remoteService.registerCallback(remoteEventListener2);
		remoteService.registerCallback(remoteEventListener2);
		remoteService.registerCallback(remoteEventListener2);
		remoteService.registerCallback(remoteEventListener2);
		System.out.println(">>>>>>>>>> ProxyClientCount:" + proxyFactoryA.clients.size());
		remoteService.callback();
		assertTrue(remoteEventListener1.await(10));
		assertTrue(remoteEventListener2.await(10));
	}
	@Test
	public void testRemoteEventNotificationWorkLikeASpaceWould() throws Exception {
		
		System.out.println("Sending....................");
		RemoteEventListener remoteEventListener = new RemoteEventListener("myListenerId", 10);
		String result = remoteService.notify(new String[0], new String[] { "template"}, remoteEventListener, new Event.Type[] { Type.READ}, -1);
		assertEquals("blah", result);
		assertTrue(remoteEventListener.await(10));
	}
	
	@Test
	public void testCustomUserTypeBothWays() throws Exception {
		UserType userType = new DummyServiceImpl.UserType();
		userType.someInt = 99;
		userType.someValue = "someValue";
		UserType userTypeResult = remoteService.doCustomUserType(userType);
		assertNotNull(userTypeResult);
		assertEquals("someValue", userTypeResult.someValue);
		assertTrue(DummyServiceImpl.callCount == 1);				
	}

}
