package com.liquidlabs.transport.proxy;

import com.liquidlabs.common.net.URI;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import junit.framework.TestCase;

import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.TransportFactoryImpl;
import com.liquidlabs.transport.serialization.Convertor;

public class RemoteClientCallsbackTest extends TestCase {
	private ProxyFactoryImpl proxyFactoryA;
	private ProxyFactoryImpl proxyFactoryB;
	private URI proxyBAddress;
	
	private DummyService remoteService;
	TransportFactory transportFactory = new TransportFactoryImpl(Executors.newFixedThreadPool(5), "test");
	ExecutorService executor = Executors.newFixedThreadPool(5);
	Convertor c = new Convertor();
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		transportFactory.start();
		
		proxyFactoryA = new ProxyFactoryImpl(transportFactory,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", 11111, "testService"), executor, "");
		proxyFactoryA.start();
		
		Thread.sleep(100);
		
		proxyFactoryB = new ProxyFactoryImpl(transportFactory,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", 22222, "testService"), executor, "");
		proxyFactoryB.registerMethodReceiver("methodReceiver", new DummyServiceImpl());
		proxyFactoryB.start();
		
		Thread.sleep(100);
		proxyBAddress = proxyFactoryB.getAddress();
		DummyServiceImpl.callCount = 0;
		remoteService = proxyFactoryA.getRemoteService("methodReceiver", DummyService.class, new String[] { proxyBAddress.toString() } );
	}
	@Override
	protected void tearDown() throws Exception {
		transportFactory.stop();
		proxyFactoryA.stop();
		proxyFactoryB.stop();
		Thread.sleep(50);
	}
	
	public void testRemotableClientShouldPassInvocations() throws Exception {
		RemoteClientCallbackImpl myClientObject = new RemoteClientCallbackImpl("CLIENT_ID", proxyFactoryA.getEndPoint());

		// prime the sockets
		remoteService.twoWay("");
		
		remoteService.callbackOnMe(myClientObject, "payload");
		Thread.sleep(1000);
		
		assertEquals(1, myClientObject.callCount);
		assertEquals("payload", myClientObject.payloads.get(0));
	}
	
	public void testProxyFactoryMakesRemoteableObject() throws Exception {
		
		MyDumbClient myDumbClient = new MyDumbClient();
		
		remoteService.callbackOnMe(myDumbClient, "payload");
		Thread.sleep(1000);
		
		assertTrue(myDumbClient.payloads.size() > 0);
		assertEquals("payload", myDumbClient.payloads.get(0));
	}
	public static class MyDumbClient implements RemoteClientCallback {
		public ArrayList<String> payloads = new ArrayList<String>();

		public void callback(String payload) {
			payloads.add(payload);
		}

		public String getRemoteId() {
			return null;
		}

		public String[] getRemoteURIs() {
			return null;
		}
		public String getId() {
			return "id";
		}
		
	}
	

}
