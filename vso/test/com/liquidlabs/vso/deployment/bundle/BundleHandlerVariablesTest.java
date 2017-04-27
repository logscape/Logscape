package com.liquidlabs.vso.deployment.bundle;

import com.liquidlabs.vso.FunctionalTestBase;
import com.liquidlabs.vso.agent.ResourceAgent;
import com.liquidlabs.vso.deployment.bundle.Bundle;
import com.liquidlabs.vso.deployment.bundle.Service;
import com.liquidlabs.vso.deployment.bundle.Bundle.Status;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class BundleHandlerVariablesTest extends FunctionalTestBase {
	static int callCount = 0;
	static CountDownLatch callCountLatch = new CountDownLatch(3);
	static CountDownLatch ormLatch = new CountDownLatch(2);


	private static final String SCRIPT_1 = 	"	import com.liquidlabs.vso.deployment.bundle.BundleHandlerVariablesTest\n" +
											"	println \"do stuff1\"\n" +
											"	BundleHandlerVariablesTest.incrementCount(\"stuff\")\n" +
											"	println \"call count is: ${BundleHandlerVariablesTest.callCount}\"";
	
	private static final String SCRIPT_2 = 	"	import com.liquidlabs.vso.deployment.bundle.BundleHandlerVariablesTest\n" +
											"	println \"do stuff2 - orm value is:${ORM}\"\n" +
											"	BundleHandlerVariablesTest.incrementCount(\"${ORM}\")\n" +
											"	println \"call count is: ${BundleHandlerVariablesTest.callCount}\"";;
	
	public static int ORMCount = 0;
	// used by groovy scripts to prove execution
	public synchronized static void incrementCount(String ormValue){
		callCount++;
        callCountLatch.countDown();
		System.out.println("incrementing callcount, newValue:" + callCount + " @ " + System.currentTimeMillis());
		System.out.println("ORMValue is:" + ormValue);
		if (!ormValue.equals("stuff")){
			ORMCount++;
            ormLatch.countDown();
		}
	}
	
	protected void setUp() throws Exception {
		super.setUp();
		for (ResourceAgent resourceAgent : this.agents) {
			resourceAgent.addDeployedBundle("myBundle-0.01", "now");
		}
		Thread.sleep(1000);
	}
	
	protected void tearDown() throws Exception {
		super.tearDown();
	}


	public void testShouldPassVariablesThroughForRegisteredServiceNames() throws Exception {
		Bundle bundle = new Bundle("myBundle", "0.01");
		Service service1 = new Service(bundle.getId(), "ORM", SCRIPT_1, "1");
		service1.setPauseSeconds(1);
		bundle.addService(service1);
		
		Service service2 = new Service(bundle.getId(), "myService2", SCRIPT_2, "2");
		bundle.addService(service2);
		
		bundleHandler.install(bundle);
		
		bundleHandler.deploy(bundle.getId(), ".");

        assertTrue("ORMLatch Timed Out", ormLatch.await(20, TimeUnit.SECONDS));
        assertTrue(callCountLatch.await(5, TimeUnit.SECONDS));
		Bundle bundle2 = bundleSpace.getBundle(bundle.getId());
		assertEquals(Status.ACTIVE, bundle2.getStatus());
	}
}
