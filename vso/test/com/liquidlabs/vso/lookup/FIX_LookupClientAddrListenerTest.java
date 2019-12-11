package com.liquidlabs.vso.lookup;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.jmx.JmxHtmlServerImpl;
import com.liquidlabs.transport.Config;
import com.liquidlabs.transport.TransportFactoryImpl;
import com.liquidlabs.transport.proxy.AddressUpdater;
import com.liquidlabs.transport.proxy.ProxyClient;
import com.liquidlabs.transport.proxy.ProxyFactoryImpl;

public class FIX_LookupClientAddrListenerTest extends LookupBaseFunctionalTest {
	
	private List<ProxyFactoryImpl> pcfs = new ArrayList<ProxyFactoryImpl>();
	private List<TransportFactoryImpl> tfs = new ArrayList<TransportFactoryImpl>();
	int clientCount = 10;
	
	AtomicInteger callCount = new AtomicInteger();
	ExecutorService executor = Executors.newCachedThreadPool();
	private String zone = "myLocation";
	private boolean isStrictLocationMatch;

	protected void xxxsetUp() throws Exception {
		super.setUp();
		
		for (int i = 0; i < clientCount; i++) {
			TransportFactoryImpl tfi = new TransportFactoryImpl(executor, "test");
			tfi.start();
			tfs.add(tfi);
			ProxyFactoryImpl pcf = new ProxyFactoryImpl(tfi, Config.TEST_PORT + i, executor, "lookupClient"+i);
			pcf.start();
			pcfs.add(pcf);
		}
		
		
	}
	protected void xxxtearDown() throws Exception {
		for (int i = 0; i < clientCount; i++) {
			tfs.get(i).stop();
			pcfs.get(i).stop();
		}
		super.tearDown();
	}

    public void testNothing() throws Exception {

    }

    // DodgyTest
	public void xxxtestAddressListenerGetsAnUpdate() throws Throwable {
		
		for (int i = 0; i < clientCount; i++) {
			final int cc = i;
		
			AddressUpdater addressListener = new AddressUpdater(){
				public String getId() {
					return AddressUpdater.class.getSimpleName() + cc;
				}
				
				public void updateEndpoint(String address, String replicationAddress) {
					callCount.incrementAndGet();
				}
				public void removeEndPoint(String address, String replicationAddress) {
					System.err.println("Removed address:" + address);
					
				}
	
				public void setProxyClient(ProxyClient<?> client) {
				}
	
				public void syncEndPoints(String[] addresses, String[] replicationLocations) {
					callCount.incrementAndGet();
					System.out.println(new Date() + " " + this.hashCode() + ": Called:" + Arrays.toString(addresses));
					
				}
	
				public void setId(String clientId) {
				}
			};
			LookupSpace remoteLU = pcfs.get(i).getRemoteService(LookupSpaceImpl.NAME, LookupSpace.class, new String[] {  "tcp://localhost:11000/space/client" }, addressListener);
			remoteLU.registerUpdateListener(addressListener.getId() , addressListener, "myServiceName", "resourceId"+cc, "context", zone, isStrictLocationMatch);
		}
		
		
		super.lookupSpaceA.registerService(new ServiceInfo("myServiceName", "tcp://192.168.1.1:1234", JmxHtmlServerImpl.locateHttpUrL(), zone, ""), -1);
		int waitCount = 0;
		while (callCount.get() < clientCount && waitCount++ < 100) {
			Thread.sleep(100);
		}
		assertEquals("Call Count was:" + callCount.get() + " Should have been:" + clientCount, callCount.get(), clientCount);
	}
}

