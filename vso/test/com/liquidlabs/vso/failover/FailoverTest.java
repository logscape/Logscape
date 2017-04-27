package com.liquidlabs.vso.failover;

import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.jmx.JmxHtmlServerImpl;
import com.liquidlabs.transport.proxy.AddressUpdater;
import com.liquidlabs.transport.proxy.ProxyClient;
import com.liquidlabs.vso.FunctionalTestBase;
import com.liquidlabs.vso.agent.ResourceAgent;
import com.liquidlabs.vso.deployment.bundle.Bundle;
import com.liquidlabs.vso.deployment.bundle.Service;
import com.liquidlabs.vso.lookup.ServiceInfo;
import com.liquidlabs.vso.work.WorkAssignment;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("unused")
public class FailoverTest extends FunctionalTestBase {
	


	private static final String PUBLISHER_SCRIPT = 	"	import com.liquidlabs.vso.failover.*\n" +
													"	println \"********************* do stuff PUBLISHER ******************************** \"\n" +
		
																			"  processMaker.java  \"-cp:.\", \"com.liquidlabs.vso.failover.MyService\", \"${lookupSpaceAddress}\"";


    @Override
	protected void setUp() throws Exception {
		agentCount = 3;
		super.setUp();
		List<ResourceAgent> agents2 = this.agents;
		for (ResourceAgent resourceAgent : agents2) {
			resourceAgent.addDeployedBundle("myBundle-0.01", "now");
		}
	}
	
	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
	}
	
	int removeEndPointCalled = 0;
	int updateEndPointCount = 0;
	Set<String> addresses = new HashSet<String>();
	private String location = "DEFAULT";
	private boolean isStrictLocationMatch;

    class MyFoo {
        CountDownLatch latch = new CountDownLatch(0);
        void messageRecieved() {
            latch.countDown();
        }
    }


	@Test
	public void testLookupSpaceServiceRegistrationFailsOver() throws Throwable {
        final MyFoo foo = new MyFoo();
        foo.latch = new CountDownLatch(1);
		// Setup ----------------
		ServiceInfo publisherA = new ServiceInfo("PUBLISHER", "tcp://locationA", JmxHtmlServerImpl.locateHttpUrL(), location, "");
		ServiceInfo publisherB = new ServiceInfo("PUBLISHER", "tcp://locationB", JmxHtmlServerImpl.locateHttpUrL(), location, "");
		lookupSpace.registerService(publisherA, 999);
		
		
		AddressUpdater addressListener = new AddressUpdater() {

			ProxyClient<?> client;
			private String updatedEndPoint;
			

			public String getId() {
				return "AddressUpdater";
			}

			public void removeEndPoint(String address, String replicationAddress) {
				System.out.println("************* removeEndpoint called:" + address);
				foo.messageRecieved();
				addresses.remove(address);
			}

			public void setProxyClient(ProxyClient<?> client) {
				this.client = client;
			}

			public void updateEndpoint(String address, String replicationAddress) {
				System.out.println("************* newAddressAdded:" + address);
				this.updatedEndPoint = address;
				updateEndPointCount++;
				addresses.add(address);
			}

			public void syncEndPoints(String[] array, String[] replicationLocations) {
                addresses.clear();
                addresses.addAll(Arrays.asList(array));
                foo.messageRecieved();
			}

			public void setId(String clientId) {
				// TODO Auto-generated method stub
				
			}
			
		};
		
		String[] serviceAddresses = lookupSpace.getServiceAddresses("PUBLISHER", location, true);
		assertTrue("Failed to get any PUBLISHER Addresses", serviceAddresses.length > 0);
		addresses.add(serviceAddresses[0]);
		pause();
		
		lookupSpace.registerUpdateListener(addressListener.getId() , addressListener, "PUBLISHER", "myResourceId", "who", location, isStrictLocationMatch);
		
		// TEST ---------------
		
		// test - 2nd service added
		lookupSpace.registerService(publisherB, 999);

        foo.latch.await(3, TimeUnit.SECONDS);
		assertEquals("Addresses:" + addresses, 2, addresses.size());

        foo.latch = new CountDownLatch(1);
		// test - 1st service removed, failover to 2nd
		lookupSpace.unregisterService(publisherA);

        foo.latch.await(3, TimeUnit.SECONDS);
		assertTrue(1 == addresses.size());
		assertEquals(publisherB.getLocationURI(), addresses.toArray(new String[0])[0]);

        foo.latch = new CountDownLatch(1);
		// test - 1st service restarted, should have added 1st to the list
		lookupSpace.registerService(publisherA, 999);
		foo.latch.await(3, TimeUnit.SECONDS);

		assertTrue(2 == addresses.size());
		assertTrue(addresses.toString(), addresses.toString().contains("locationA"));
		assertTrue(addresses.toString(), addresses.toString().contains("locationB"));

		foo.latch = new CountDownLatch(1);
		lookupSpace.unregisterService(publisherB);
         foo.latch.await(3, TimeUnit.SECONDS);
		assertTrue(1 == addresses.size());
		assertTrue(addresses.toString(), addresses.toString().contains("locationA"));
//
	}

	
	/***********
	 * 
	 * NOTE: this test fails under Java 6 on OSX 
	 * @throws Throwable 
	 */
	public void xxxxxxxxxxxxxxxxxxxxtestShouldFailToNewPublisherAddressWhenServiceIsRemoved() throws Throwable {
		Bundle bundle = new Bundle("myBundle", "0.01");
		
		// we want 2 publisher to register as Services
		Service service1 = new Service(bundle.getId(), "PUBLISHER", PUBLISHER_SCRIPT, "2");
		service1.setFork(true);
		service1.setPauseSeconds(1);
		bundle.addService(service1);
		

        final CountDownLatch addLatch = new CountDownLatch(2);
        final CountDownLatch removeLatch = new CountDownLatch(1);
		AddressUpdater addressListener = new AddressUpdater() {

			ProxyClient<?> client;
			private String updatedEndPoint;

			public String getId() {
				return "AddressUpdater";
			}

			public void removeEndPoint(String address, String replicationAddress) {
				removeEndPointCalled++;
                removeLatch.countDown();
			}

			public void setProxyClient(ProxyClient<?> client) {
				this.client = client;
			}

			public void updateEndpoint(String address, String replicationAddress) {
                addLatch.countDown();
				this.updatedEndPoint = address;
			}

			public void syncEndPoints(String[] addresses, String[] replicationLocations) {
                for (String address : addresses) {
                    addLatch.countDown();
                }
			}

			public void setId(String clientId) {
				// TODO Auto-generated method stub
				
			}
		};
		lookupSpace.registerUpdateListener(addressListener.getId() , addressListener, "PUBLISHER", "myResourceId", "who", location, isStrictLocationMatch);
        
        System.out.println("********** INSTALL ******************************************* ");
        bundleHandler.install(bundle);
        System.out.println("********** DEPLOY ******************************************* ");
        bundleHandler.deploy(bundle.getId(), ".");

		pause();
		pause();
		pause();
		
		pause();
        assertTrue(addLatch.await(10, TimeUnit.SECONDS));
		String[] serviceAddresses = lookupSpace.getServiceAddresses("PUBLISHER", location, true);
		assertEquals("Should have 2 service Instances but GOT:" + serviceAddresses.length, 2, serviceAddresses.length);
		
		// now shutdown the first service instance
		List<WorkAssignment> workAssignmentsForQuery = workAllocator.getWorkAssignmentsForQuery("serviceName contains PUBLISHER");
		WorkAssignment firstPublisherWorkAssignment = workAssignmentsForQuery.get(0);

		pause();
		
		// stopping the workAssignment, will tell the ResourceAgent to stop doing the work
		System.out.println(" *************************** Stopping workAssignent ******************************* ");
		workAllocator.unassignWork("MyRequestId", firstPublisherWorkAssignment.getId());
		
		// workAllocator.unassignWork will call the ResourceAgent.stop(workAssignment)
		// MyService shutdownHook will unregister from the lookupSpace
		// On Win32 Process.destroy() does not call the shutdown hook
		// so this test will fail
		pause();
		
		// the test will fail because the service shutdown hook isnt called
        assertTrue(removeLatch.await(10, TimeUnit.SECONDS));
		String[] serviceAddresses2 = lookupSpace.getServiceAddresses("PUBLISHER", location, true);
		assertEquals("Should have 1 service Instances but GOT:" + serviceAddresses.length, 1, serviceAddresses2.length);
		
		assertEquals("Remove EndPoint not called to allow the client to update, callcount:" + removeEndPointCalled, 1, removeEndPointCalled);
	}

	private void pause() {
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}
}
