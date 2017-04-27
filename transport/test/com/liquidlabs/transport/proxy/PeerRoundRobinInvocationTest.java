package com.liquidlabs.transport.proxy;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
public class PeerRoundRobinInvocationTest  {
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
		serviceOne = new PeerFancyDummyServiceImpl("AAA");
		remoteOneFactory.registerMethodReceiver("methodReceiver", serviceOne);
		remoteOneFactory.start();
		
		remoteTwoFactory = new ProxyFactoryImpl(transportFactory,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", 10002, "testServiceC"), executor, "");
		serviceTwo = new PeerFancyDummyServiceImpl("BBB");
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
	public void testShouldRoundRobinWithFactor() throws Exception {
		
		clientService.shouldRoundRobinWithFactor("one");
		clientService.shouldRoundRobinWithFactor("one");
		clientService.shouldRoundRobinWithFactor("two");
		clientService.shouldRoundRobinWithFactor("two");
		
		assertEquals("[one, one]", serviceOne.received.toString());
		assertEquals("[two, two]", serviceTwo.received.toString());
	}
	
	@Test
	public void testShouldRoundRobinToNewPeer() throws Exception {
		clientService.shouldRoundRobinWithFactor("one");
		clientService.shouldRoundRobinWithFactor("one");
		
		ProxyFactoryImpl remoteThreeFactory = new ProxyFactoryImpl(transportFactory,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", 10003, "testService"), executor, "");
		PeerFancyDummyServiceImpl serviceThree = new PeerFancyDummyServiceImpl("CCC");
		remoteThreeFactory.registerMethodReceiver("methodReceiver", serviceThree);
		remoteThreeFactory.start();


		client.refreshAddresses(remoteOneFactory.getAddress().toString() +","+remoteTwoFactory.getAddress().toString() +"," +remoteThreeFactory.getAddress().toString());
		

		clientService.shouldRoundRobinWithFactor("two");
		clientService.shouldRoundRobinWithFactor("two");
		clientService.shouldRoundRobinWithFactor("three");
		clientService.shouldRoundRobinWithFactor("three");
		
		assertEquals("[one, one]", serviceOne.received.toString());
		assertEquals("[two, two]", serviceTwo.received.toString());
		assertEquals("[three, three]", serviceThree.received.toString());
	}
	
	

}
