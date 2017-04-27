package com.liquidlabs.vso.deployment.bundle;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;

import com.liquidlabs.vso.FunctionalTestBase;
import com.liquidlabs.vso.agent.ResourceAgent;
import com.liquidlabs.vso.agent.ResourceAgentImpl;

public class BundleBackgroundServicesTest extends FunctionalTestBase {
	static AtomicInteger callCount = new AtomicInteger(0);
	static AtomicInteger fooCount = new AtomicInteger(0);

	private static final String SCRIPT_1 = "	import com.liquidlabs.vso.deployment.bundle.BundleBackgroundServicesTest\n"
			+ "	println \">>>>>>>>>>>>>>>> ${resourceId}\"\n"
			+ "	BundleBackgroundServicesTest.incrementCount(\"stuff\")\n"
			+ "	println \"<<<<<<<<<<<<<< call count is: ${BundleBackgroundServicesTest.callCount}\"";
	
	private static final String SCRIPT_FOO = "	import com.liquidlabs.vso.deployment.bundle.BundleBackgroundServicesTest\n"
		+ "	println \">>>>>>>>>>>>>>>>FOOOOOOOOO ${resourceId}\"\n"
		+ "	BundleBackgroundServicesTest.incrementFooCount(\"fooo\")\n"
		+ "	println \"<<<<<<<<<<<<<<FOOOOOOOOO call count is: ${BundleBackgroundServicesTest.fooCount}\"";

	public synchronized static void incrementCount(String ormValue) {
		callCount.incrementAndGet();
		System.out.println("=====================* incrementing callcount, newValue:" + callCount);
	}
	public synchronized static void incrementFooCount(String ormValue) {
		fooCount.incrementAndGet();
		System.out.println("=====================* incrementing callcount, newValue:" + fooCount);
	}

	private String releaseDate = "now";

	@Before
	public void setUp() throws Exception {
		super.setUp();
		callCount.set(0);
		fooCount.set(0);
		for (ResourceAgent resourceAgent : this.agents) {
			resourceAgent.addDeployedBundle("myBundle-0.01", releaseDate);
		}
	}

	@After
	protected void tearDown() throws Exception {
		super.tearDown();
	}

	public void testShouldAssignBackgroundServicesToNewResource() throws Exception {
		Bundle bundle = new Bundle("myBundle", "0.01");
		Service service1 = new Service(bundle.getId(), "ORM", SCRIPT_1, "1");

		service1.setBackground(true);
		service1.setInstanceCount("-1");
		bundle.addService(service1);

		bundleHandler.install(bundle);
		bundleHandler.deploy(bundle.getId(), ".");
		
		// wait for the first 10 agents to be deployed (CallCount == 10) - fail if it doesnt happen
		assertWaitOnCallCount(8, 10);
		
		// Start a new Agent and check that 
		ResourceAgentImpl createResourceAgent = createResourceAgent();
		createResourceAgent.addDeployedBundle("myBundle-0.01", releaseDate);

		// CallCount should now be 11
		assertWaitOnCallCount(8, 11);
	}

	private void assertWaitOnCallCount(int waitSeconds, int expectedCallCount) {
		try {
			int waited = 0;
			while (callCount.get() < expectedCallCount && waited++ < waitSeconds) {
					Thread.sleep(1000);
			}
			assertEquals(expectedCallCount, callCount.get());
		
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
//	public void testShouldWaitForDependentServicesToStart() throws Exception {
//		for (ResourceAgent agent : agents) {
//			agent.addDeployedBundle("otherBundle-0.01", releaseDate);
//			agent.addDeployedBundle("myBundle-0.01", releaseDate);
//		}
//		
//		Bundle otherBundle = new Bundle("otherBundle", "0.01");
//		Service log = new Service(otherBundle.getId(), "LOG", SCRIPT_1, "-1");
//		log.setBackground(true);
//		otherBundle.addService(log);
//		
//		bundleHandler.install(otherBundle);
//		bundleHandler.deploy(otherBundle.getId(), ".");
//		
//		int firstCount = 5;
//		
//		final Bundle bundle = new Bundle("myBundle", "0.01");
//		Service serviceONE = new Service(bundle.getId(), "FIRST", SCRIPT_1, "1");
//		serviceONE.setBackground(true);
//		serviceONE.setInstanceCount(Integer.toString(firstCount));
//		bundle.addService(serviceONE);
//
//		// start SECOND ON resources that are not FIRST - i.e. FIRST AND SECOND are exclusive - but SECOND waits for FIRST
//		Service serviceTWO = new Service(bundle.getId(), "SECOND", SCRIPT_FOO, "1");
//		serviceTWO.setBackground(true);
//		serviceTWO.setInstanceCount("-1");
//		serviceTWO.setResourceSelection("workId notContains myBundle-0.01:FIRST");
//		serviceTWO.addDependency(serviceONE.getId());
//		bundle.addService(serviceTWO);
//
//		new Thread(new Runnable() {
//			public void run() {
//				try {
//					bundleHandler.install(bundle);
//					bundleHandler.deploy(bundle.getId(), ".");
//				} catch (Exception e) {
//					e.printStackTrace();
//					fail(e.toString());
//				}
//			}}).start();
//
//		//createSomeAgents();
//
//		
//		Thread.sleep(20000);
//		List<ResourceProfile> orms = resourceSpace.findResourceProfilesBy("workId contains FIRST");
//		
//		assertEquals(firstCount, orms.size());
//		int numFoos = agents.size() - firstCount;
//		List<ResourceProfile> foos = resourceSpace.findResourceProfilesBy("workId contains SECOND");
//		if (foos.size() != numFoos) {
//			for (ResourceProfile resourceProfile : foos) {
//				System.out.println(String.format("================== Resource[%s] workIds[%s]", resourceProfile.getResourceId(), resourceProfile.getWorkIds()));
//			}
//		}
//		assertEquals(numFoos, foos.size());
//		for (ResourceAgent agent : agents) {
//			ResourceProfile profile = resourceSpace.getResourceDetails(agent.getId() + "-0");
//			String workIds = profile.getWorkIds();
//			boolean result1 = workIds.contains(serviceONE.getId())	|| workIds.contains(serviceTWO.getId());
//			assertTrue("should contain at least one workId - " + workIds, result1);
//			boolean result2 = workIds.contains(serviceONE.getId()) && workIds.contains(serviceTWO.getId());
//			assertFalse("should only contain one workId - " + workIds, result2);
//		}
//	}
//
//	private void createSomeAgents() throws UnknownHostException,
//			URISyntaxException {
//		for(int i = 0; i < 10; i++) {
//			ResourceAgentImpl createResourceAgent = createResourceAgent();
//			createResourceAgent.addDeployedBundle("otherBundle-0.01", releaseDate);
//			createResourceAgent.addDeployedBundle("myBundle-0.01", releaseDate);
//			try {
//				Thread.sleep(100);
//			} catch (InterruptedException e) {
//			}
//		}
//	}

}
