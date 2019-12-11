package com.liquidlabs.vso.lookup;

import com.liquidlabs.common.concurrent.NamingThreadFactory;
import com.liquidlabs.common.jmx.JmxHtmlServerImpl;
import com.liquidlabs.transport.Config;
import com.liquidlabs.transport.TransportFactoryImpl;
import com.liquidlabs.transport.proxy.AddressUpdater;
import com.liquidlabs.transport.proxy.ProxyClient;
import com.liquidlabs.transport.proxy.ProxyFactoryImpl;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LookupClientTest extends LookupBaseFunctionalTest {
	
	private ProxyFactoryImpl proxyClientFactory;
	int callCount;
	private TransportFactoryImpl transportFactory;
	private String location = "myLocation";
	private boolean isStrictLocationMatch;
	private String agentType = "";

	protected void setUp() throws Exception {
		super.setUp();
		
		ExecutorService executor = Executors.newFixedThreadPool(5, new NamingThreadFactory("Test" + getName()));
		transportFactory = new TransportFactoryImpl(executor, "test");
		transportFactory.start();
		
		proxyClientFactory = new ProxyFactoryImpl(transportFactory, Config.TEST_PORT, executor, "lookupClient");
		proxyClientFactory.start();		
	}
	protected void tearDown() throws Exception {
		super.tearDown();
		transportFactory.stop();
		proxyClientFactory.stop();
		System.out.println("Done");
	}



	@Test
	public void testAddressRemovalWorks() throws Throwable {
		
		AddressUpdater addressListener = new AddressUpdater(){
			public String getId() {
				return AddressUpdater.class.getSimpleName();
			}
			
			public void updateEndpoint(String address, String replicationAddress) {
				System.err.println(getId() + " updateEndpoint called with:" + address);
				if (address.equals("location2")){
				}
			}
			public void removeEndPoint(String address, String replicationAddress) {
				System.err.println("Removed address:" + address);
				callCount++;
			}
			
			public void syncEndPoints(String[] addresses, String[] replicationLocations) {
			}
			public void setProxyClient(ProxyClient<?> client) {
				System.err.println("setProxyClient called");
			}

			public void setId(String clientId) {
				// TODO Auto-generated method stub
				
			}
		};
		
		LookupSpace lookupSpace = proxyClientFactory.getRemoteService(LookupSpaceImpl.NAME, LookupSpace.class, new String[] {  "stcp://localhost:11000/space/client" }, addressListener);

		System.out.println("----------- REGISTER SERVICE -----------");
		lookupSpace.registerService(new ServiceInfo("myServiceName", "stcp://local.host:11000/LookupSpace", JmxHtmlServerImpl.locateHttpUrL(), location, agentType), -1);
		System.out.println("----------- REGISTER LISTENER -----------");
		lookupSpace.registerUpdateListener(addressListener.getId() , addressListener, "myServiceName", "resourceId", "who", location, isStrictLocationMatch);
		pause();
		System.out.println("----------- REGISTER -----------");
		lookupSpace.registerService(new ServiceInfo("myServiceName", "stcp://local.host:12000/LookupSpace", JmxHtmlServerImpl.locateHttpUrL(), location, agentType), -1);
		pause();
		System.out.println("----------- UNREGISTER -----------");
		lookupSpace.unregisterService(new ServiceInfo("myServiceName", "stcp://local.host:12000/LookupSpace", JmxHtmlServerImpl.locateHttpUrL(), location, agentType));
		pause();
		
		assertEquals(1, callCount);
	}
	

    @Test
	public void testClientCanRegisterService() throws Exception {
		LookupSpace lookupSpace = LookupSpaceImpl.getRemoteService("stcp://localhost:11000/space/client", proxyClientFactory, true, "ctx");
		lookupSpace.registerService(new ServiceInfo("myServiceName", "location", JmxHtmlServerImpl.locateHttpUrL(), location, agentType), -1);
		pause();
		assertEquals(1, lookupSpaceA.getServiceAddresses("myServiceName", location, true).length);
	}

	
	

}

