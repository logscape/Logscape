package com.liquidlabs.transport.proxy;

import java.net.URISyntaxException;
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
public class PeerRoundRobinRecoveryTest  {
	TransportFactory transportFactory = new TransportFactoryImpl(Executors.newFixedThreadPool(5), "test");
	ExecutorService executor = Executors.newFixedThreadPool(5);
	
	private ProxyFactoryImpl proxyFactoryA;
	
	
	private ProxyFactoryImpl remoteOneFactory;
	private PeerFancyDummyService clientService;
	private PeerFancyDummyServiceImpl serviceOne;
	
	public ProxyClient<?> client;
	private AddressUpdater addressUpdater;
	
	@Before
	public void setUp() throws Exception {
		transportFactory.start();
		
		proxyFactoryA = new ProxyFactoryImpl(transportFactory,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", 10000, "testServiceA"), executor, "");
		proxyFactoryA.start();
		
		Thread.sleep(100);
		
		startServiceOne();
		
		Thread.sleep(100);
		addressUpdater = new AddressUpdater(){
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
		clientService = proxyFactoryA.getRemoteService("methodReceiver", PeerFancyDummyService.class, new String[] {  }, addressUpdater);
	}
	private void startServiceOne() throws URISyntaxException {
		remoteOneFactory = new ProxyFactoryImpl(transportFactory,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", 10001, "testServiceB"), executor, "");
		serviceOne = new PeerFancyDummyServiceImpl("AAA");
		remoteOneFactory.registerMethodReceiver("methodReceiver", serviceOne);
		remoteOneFactory.start();
	}
	@After
	public void tearDown() throws Exception {
		transportFactory.stop();
		proxyFactoryA.stop();
		
		remoteOneFactory.stop();
		Thread.sleep(50);
	}
	
	@Test
	public void testShouldRoundRobinWithFactor() throws Exception {
		
		// fail
		try {
			System.out.println(">>>>>>>>>>>>> SEND-1");
			clientService.shouldRoundRobinWithFactor("throwException");
		} catch (Throwable t) {
			t.printStackTrace();
		}
		
		updateEndpoint();
		
		System.out.println(">>>>>>>>>>>>> SEND-2");
		clientService.shouldRoundRobinWithFactor("two");
		
		assertEquals("[two]", serviceOne.received.toString());
	}
	private void updateEndpoint() {
		// feed it the address
		System.out.println(">>>>>>>>>>>>> UPDATE-1");
		try {
			addressUpdater.updateEndpoint(remoteOneFactory.getAddress().toString(), "");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	

}
