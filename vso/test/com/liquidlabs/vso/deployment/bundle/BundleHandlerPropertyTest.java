package com.liquidlabs.vso.deployment.bundle;

import com.liquidlabs.vso.FunctionalTestBase;
import com.liquidlabs.vso.agent.ResourceAgent;
import com.liquidlabs.vso.deployment.bundle.Bundle.Status;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class BundleHandlerPropertyTest extends FunctionalTestBase {
	static int callCount = 0;
	
	private static final String SCRIPT_1 = 	"	import com.liquidlabs.vso.deployment.bundle.BundleHandlerPropertyTest\n" +
											"	println \"do stuff1\"\n" +
											"	BundleHandlerPropertyTest.incrementCount(\"${myProperty}\")\n" +
											"	println \"call count is: ${BundleHandlerPropertyTest.callCount}\"";
	
	private static final String SCRIPT_2 = 	"	import com.liquidlabs.vso.deployment.bundle.BundleHandlerPropertyTest\n" +
											"	println \"do stuff2 - orm value is:${myProperty}\"\n" +
											"	BundleHandlerPropertyTest.incrementCount(\"${myProperty}\")\n" +
											"	println \"call count is: ${BundleHandlerPropertyTest.callCount}\"";
	
	private static final String SCRIPT_3 = 	"	import com.liquidlabs.vso.deployment.bundle.BundleHandlerPropertyTest\n" +
											"	BundleHandlerPropertyTest.incrementCount(\"${myProperty1}\")\n" +
											"	BundleHandlerPropertyTest.incrementCount(\"${myProperty2}\")\n" +
											"	println \"call count is: ${BundleHandlerPropertyTest.callCount}\"";


    public static CountDownLatch latch = new CountDownLatch(0);

	public static int PropertyCount = 0;
	// used by groovy scripts to prove execution
	public synchronized static void incrementCount(String propertyValue){
		callCount++;
		System.out.println("incrementing callcount, newValue:" + callCount);
		System.out.println("PropertyValue is:" + propertyValue);
		if (propertyValue.equals("Value2")){
			PropertyCount++;
		}
        latch.countDown();
	}
	
	protected void setUp() throws Exception {
		super.setUp();
		for (ResourceAgent resourceAgent : this.agents) {
			resourceAgent.addDeployedBundle("myBundle-0.01", "now");
		}
	}
	
	protected void tearDown() throws Exception {
		super.tearDown();
		PropertyCount = 0;
		callCount = 0;
	}
	public void testShouldPassPropertyFromServiceDescriptorOntoAgentAndGroovy() throws Exception {
		latch = new CountDownLatch(3);
        Bundle bundle = new Bundle("myBundle", "0.01");
		Service service1 = new Service(bundle.getId(), "ORM", SCRIPT_1, "1");
		service1.setProperty("myProperty=Value1");
		bundle.addService(service1);
		
		Service service2 = new Service(bundle.getId(), "myService2", SCRIPT_2, "2");
		service2.setProperty("myProperty=Value2");
		bundle.addService(service2);
		
		bundleHandler.install(bundle);
		bundleHandler.deploy(bundle.getId(), ".");
		Thread.sleep(1000);
		Bundle bundle2 = bundleSpace.getBundle(bundle.getId());
		
		assertEquals(Status.ACTIVE, bundle2.getStatus());
		assertTrue(latch.await(10, TimeUnit.SECONDS));
		assertEquals("Should have seen 2 script invocations with ORM being passed in", 2, PropertyCount);
	}

	public void testShouldPassMultipleCustomPropertyFromServiceDescriptorOntoAgentAndGroovyScript() throws Exception {
		latch = new CountDownLatch(2);
        Bundle bundle = new Bundle("myBundle", "0.01");
		Service service1 = new Service(bundle.getId(), "ORM", SCRIPT_3, "1");
		service1.setProperty("myProperty1=Value2,myProperty2=Value2");
		bundle.addService(service1);
		
		bundleHandler.install(bundle);
		bundleHandler.deploy(bundle.getId(), ".");
		Thread.sleep(1000);

        assertTrue(latch.await(10, TimeUnit.SECONDS));
		assertEquals(2, callCount);
		assertEquals("Should have seen 2 script invocations with PropertyValue", 2, PropertyCount);
	}
}
