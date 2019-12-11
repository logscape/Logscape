package com.liquidlabs.vso.deployment.bundle;

import java.util.List;

import com.liquidlabs.vso.FunctionalTestBase;
import com.liquidlabs.vso.agent.ResourceAgent;
import com.liquidlabs.vso.deployment.bundle.Bundle.Status;

import static org.junit.Assert.*;

public class BundleHandlerTest extends FunctionalTestBase {
	static int callCount = 0;
	
	private BundleHandler bundleHandler;
	
	private static final String SCRIPT_1 = 	"	import com.liquidlabs.vso.deployment.bundle.BundleHandlerTest\n" +
											"	println \"do stuff1\"\n" +
											"	BundleHandlerTest.incrementCount()\n" +
											"	println \"call count is: ${BundleHandlerTest.callCount}\"";
	private static final String SCRIPT_2 = 	"	import com.liquidlabs.vso.deployment.bundle.BundleHandlerTest\n" +
											"	println \"do stuff2\"\n" +
											"	BundleHandlerTest.incrementCount()\n" +
											"	println \"call count is: ${BundleHandlerTest.callCount}\"";;
											
											
											
											
	
	// used by groovy scripts to prove execution
	public synchronized static void incrementCount(){
		callCount++;
		System.out.println("incrementing callcount, newValue:" + callCount);
	}
	
	protected void setUp() throws Exception {
		super.setUp();
		for (ResourceAgent resourceAgent : this.agents) {
			resourceAgent.addDeployedBundle("myBundle-0.01", "now");
		}
		callCount = 0;
		Thread.sleep(1000);
		this.bundleHandler = new BundleHandlerImpl(bundleSpaceProxy);
	}
	
	protected void tearDown() throws Exception {
		super.tearDown();
	}
	
	public void XtestShouldAllocAndStartNineServiceInstances() throws Exception {
		
		Bundle bundle = new Bundle("myBundle","0.01");
		bundle.addService(new Service(bundle.getId(), "myService_ONE", SCRIPT_1, "2"));
		bundle.addService(new Service(bundle.getId(), "myService_TWO", SCRIPT_2, "1"));
		bundle.addService(new Service(bundle.getId(), "myService_THREE", SCRIPT_1, "3"));
		bundle.addService(new Service(bundle.getId(), "myService_FOUR", SCRIPT_1, "1"));
		bundle.addService(new Service(bundle.getId(), "myService_FIVE", SCRIPT_2, "2"));
		
		bundleHandler.install(bundle);
		bundleHandler.deploy(bundle.getId(), ".");
		Bundle bundle2 = bundleSpace.getBundle(bundle.getId());
		
		int timeout = 0;
		while (callCount < 9 || timeout++ < 30) {
			Thread.sleep(1000);
		}
		
		assertEquals(Status.ACTIVE, bundle2.getStatus());
		assertEquals(9, callCount);
	}
	
	public void XtestShouldAllocAndStartTHREEServiceInstances() throws Exception {
		
		int serviceInstanceCount = 3;
		Bundle bundle = new Bundle("myBundle", "0.01");
		bundle.addService(new Service(bundle.getId(), "myService1", SCRIPT_1, Integer.toString(serviceInstanceCount)));
		
		bundleHandler.install(bundle);
		bundleHandler.deploy(bundle.getId(), ".");
		Thread.sleep(2000);
		Bundle bundle2 = bundleSpace.getBundle(bundle.getId());
		
		assertEquals(Status.ACTIVE, bundle2.getStatus());
		assertEquals(serviceInstanceCount, callCount);
	}

	public void testShouldAllocAndStartONEServiceInstance() throws Exception {
		
		System.out.println("\n\n ---------------------------- STARTING ------------------\n\n");
		Bundle bundle = new Bundle("myBundle", "0.01");
		bundle.addService(new Service(bundle.getId(), "myService1", SCRIPT_1, "1"));
		
		bundleHandler.install(bundle);
		bundleHandler.deploy(bundle.getId(), ".");
		Thread.sleep(2000);
		Bundle bundle2 = bundleSpace.getBundle(bundle.getId());
		
		System.out.println("\n\n ---------------------------- DONE ------------------\n\n");		
		
		assertEquals(Status.ACTIVE, bundle2.getStatus());
		assertEquals(1, callCount);
	}
	
	public void XtestShouldStoreRetrieveBundle() throws Exception {
		Bundle bundle = new Bundle("myBundle", "0.01");
		bundle.addService(new Service(bundle.getId(), "myService1", SCRIPT_1, "1"));
		bundle.addService(new Service(bundle.getId(), "myService2", SCRIPT_2, "2"));
		
		bundleHandler.install(bundle);
		Thread.sleep(100);
		
		Bundle bundle2 = bundleSpace.getBundle(bundle.getId());
		assertNotNull(bundle2);
	}
	public void testShouldStoreAndRetrieveBundleServices() throws Exception {
		Bundle bundle = new Bundle("myBundle", "0.01");
		bundle.addService(new Service(bundle.getId(), "myService1", SCRIPT_1, "1"));
		bundle.addService(new Service(bundle.getId(), "myService2", SCRIPT_2, "2"));
		
		bundleHandler.install(bundle);
		Thread.sleep(100);
		
		List<Service> bundleServices = bundleSpace.getBundleServices(bundle.getId());
		assertNotNull(bundleServices);
		assertEquals(2, bundleServices.size());
	}
	
	public void testShouldRemoveBundle() throws Exception {
		Bundle bundle = new Bundle("myBundle", "0.01");
		bundle.addService(new Service(bundle.getId(), "myService1", SCRIPT_1, "1"));
		bundle.addService(new Service(bundle.getId(), "myService2", SCRIPT_2, "2"));
		
		bundleHandler.install(bundle);
		bundleHandler.deploy(bundle.getId(), ".");
		Thread.sleep(2000);
		bundleHandler.undeployBundle(bundle.getId());
		Thread.sleep(500);
		Bundle bundle2 = bundleSpace.getBundle(bundle.getId());
		assertNull("Bundle was NOT null", bundle2);
		try {
			bundleSpace.getBundleService("myBundle:myService1");
			fail("Should have thrown exception as bundle has been removed");
		}catch(RuntimeException re) {}
	}
}
