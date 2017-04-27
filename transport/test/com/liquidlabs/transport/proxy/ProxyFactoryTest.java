package com.liquidlabs.transport.proxy;

import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.transport.Config;
import com.liquidlabs.transport.TransportFactoryImpl;
import junit.framework.TestCase;

import java.util.concurrent.Executors;

public class ProxyFactoryTest extends TestCase {
	java.util.concurrent.ExecutorService executor = Executors.newFixedThreadPool(5);
	
	static class MyAddressUpdater implements AddressUpdater {

		public String getId() {
			return "id";
		}

		public void removeEndPoint(String address, String replicationAddress) {
			// TODO Auto-generated method stub
			
		}

		public void setProxyClient(ProxyClient<?> client) {
			// TODO Auto-generated method stub
			
		}

		public void updateEndpoint(String address, String replicationAddress) {
			
		}

		public void syncEndPoints(String[] addresses, String[] replicationLocations) {
			// TODO Auto-generated method stub
			
		}

		public void setId(String clientId) {
			// TODO Auto-generated method stub
			
		}
		
	}
	
	public void testShouldGetProxyEvenIfServiceUnavailable() throws Exception {
		TransportFactoryImpl transportFactory = new TransportFactoryImpl(ExecutorService.newDynamicThreadPool("pftest","PROXY_FACTORY_TEST"), "test");
		transportFactory.start();
		ProxyFactoryImpl proxyFactory = new ProxyFactoryImpl(transportFactory, Config.TEST_PORT, executor, "testService");
		proxyFactory.start();
		
		NotifyInterface remoteService = proxyFactory.getRemoteService("myService", NotifyInterface.class, new String [] {"stcp://localhost:1200/LookupSpace"}, new MyAddressUpdater());
		assertNotNull(remoteService);
	}

}
