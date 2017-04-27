package com.liquidlabs.transport.proxy;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.TransportFactoryImpl;


/**
 * 
 * Tests invocations that a broadcast or roundRobin(factor)
 *
 */
public class PeerBroadcastTest  {
	TransportFactory transportFactory = new TransportFactoryImpl(Executors.newFixedThreadPool(5), "test");
	ExecutorService executor = Executors.newFixedThreadPool(5);
	
	private ProxyFactoryImpl proxyFactoryA;
	
	
	private ProxyFactoryImpl remoteOneFactory;
	private PeerFancyDummyService clientService;
	private PeerFancyDummyServiceImpl serviceOne;
	
	private ProxyFactoryImpl remoteTwoFactory;
	private PeerFancyDummyServiceImpl serviceTwo;
	
	public ProxyClient<?> client;
	
	@Before
	public void setUp() throws Exception {
		transportFactory.start();
		
		proxyFactoryA = new ProxyFactoryImpl(transportFactory,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", 10000, "testServiceA"), executor, "");
		proxyFactoryA.start();
		
		Thread.sleep(100);
		
		remoteOneFactory = new ProxyFactoryImpl(transportFactory,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", 10001, "testServiceB"), executor, "");
		serviceOne = new PeerFancyDummyServiceImpl("AAAAAAAAAAA:");
		remoteOneFactory.registerMethodReceiver("methodReceiver", serviceOne);
		remoteOneFactory.start();
		
		remoteTwoFactory = new ProxyFactoryImpl(transportFactory,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", 10002, "testServiceC"), executor, "");
		serviceTwo = new PeerFancyDummyServiceImpl("BBBBBBBBBBB:");
		remoteTwoFactory.registerMethodReceiver("methodReceiver", serviceTwo);
		remoteTwoFactory.start();
		
		Thread.sleep(100);
		AddressUpdater updater = new AddressUpdater(){
			public String getId() {
				return "someClientId";
			}
			public void setProxyClient(ProxyClient<?> aClient) {
				client = aClient;
			}
			public void removeEndPoint(String address, String replicationAddress) {
			}
			public void syncEndPoints(String[] addresses, String[] replicationLocations) {
			}
			public void updateEndpoint(String address, String replicationAddress) {
				client.refreshAddresses(address);
			}
			public void setId(String clientId) {
				// TODO Auto-generated method stub
				
			}
		};
		clientService = proxyFactoryA.getRemoteService("methodReceiver", PeerFancyDummyService.class, new String[] { remoteOneFactory.getAddress().toString(), remoteTwoFactory.getAddress().toString() }, updater);
	}
	@After
	public void tearDown() throws Exception {
		transportFactory.stop();
		proxyFactoryA.stop();
		
		remoteOneFactory.stop();
		remoteTwoFactory.stop();
		Thread.sleep(50);
	}
	
	@Test
	public void shouldSeeAnnotationToBroadcastInvocation() throws Exception {
		
		clientService.broadcast("stuff");
		
		assertEquals(1, serviceOne.callCount);
		assertEquals(1, serviceTwo.callCount);
	}
	@Test	
	public void shouldSeeAnnotationToBroadcastInvocationAndSucceedWhenErrors() throws Exception {
		
		clientService.broadcast("throwException");
		
		assertEquals(1, serviceOne.callCount);
		assertEquals(1, serviceTwo.callCount);
	}
	
	@Test
	public void shouldNotStallWhenEndpointWasDisabled() throws Exception {
		long start = DateTimeUtils.currentTimeMillis();
		
		clientService.broadCastAndDisable("throwException");
		assertEquals(0, serviceOne.callCount);
		assertEquals(1, serviceTwo.callCount);
		
		System.out.println("\n\n============================================================ stage 2 == \n\n");
		
		clientService.broadCastAndDisable("doStuff");
		
		assertEquals(1, serviceOne.callCount);
		assertEquals(2, serviceTwo.callCount);
		long end = DateTimeUtils.currentTimeMillis();
		
		assertTrue("Took too long > 2s", end - start < 2000);
		
	}
}
