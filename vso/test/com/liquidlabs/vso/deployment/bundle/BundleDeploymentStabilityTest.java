package com.liquidlabs.vso.deployment.bundle;

import com.liquidlabs.vso.FunctionalTestBase;
import com.liquidlabs.vso.agent.ResourceAgent;
import com.liquidlabs.vso.agent.ResourceAgentImpl;

import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Done to test deployent reliability/stability under different scenarios
 * See ticket:77
 *
 */
public class BundleDeploymentStabilityTest extends FunctionalTestBase {
	
	static ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<String, Integer>();
	
	private static final String SCRIPT_1 = 	"	import com.liquidlabs.vso.deployment.bundle.BundleDeploymentStabilityTest\n" +
											"	println \"do stuff PUBLISHER\"\n" +
											"	BundleDeploymentStabilityTest.incrementCount(\"one\")\n";
	
	private static final String SCRIPT_2 = 	"	import com.liquidlabs.vso.deployment.bundle.BundleDeploymentStabilityTest\n" +
											"	println \"do stuff REPLICATOR\"\n" +
											"	BundleDeploymentStabilityTest.incrementCount(\"two\")\n";

	public static Integer callCount = 0;
	private int workerCount = 10;
	
	// used by groovy scripts to prove execution
	public static void incrementCount(String param){
		System.out.println("*** received:" + param);
		if (!map.contains(param)){
			map.put(param, 1);
			System.out.println("***" + param + "-" + 1);
		} else {
			int count = map.get(param) +  1;
			map.put(param, count);
		}
		
		synchronized (callCount){
			callCount++;
			System.out.println(">>>>>***" + param + " callCount:" + callCount);
		}
		
	}

	protected void setupAgents() throws UnknownHostException, URISyntaxException{
		for (int i = 0; i < workerCount; i++){
			ResourceAgentImpl resourceAgentImpl = createResourceAgent(false);
			agents.add(resourceAgentImpl);
		}		
	}
	
	protected void setUp() throws Exception {
		super.setUp();
		for (ResourceAgent resourceAgent : this.agents) {
			resourceAgent.addDeployedBundle("myBundle-0.01", "now");
			
		}
	}
	
	protected void tearDown() throws Exception {
		super.tearDown();
	}
	
	public void testShouldAssignOneSystemServiceAnd19BackgroundServicesToResources() throws Exception {
		Bundle bundle = new Bundle("myBundle", "0.01");
		
		// we want 1 publisher and its a System service
		Service service1 = new Service(bundle.getId(), "PUBLISHER", SCRIPT_1, "1");
		service1.setProperty("ServiceType=System");
		service1.setInstanceCount("1");
		service1.setPauseSeconds(0);
		
		// everything else gets a replicator, but it cannot run where a System service is running
		Service service2 = new Service(bundle.getId(), "REPLICATOR", SCRIPT_2, "1");
		service2.setBackground(true);
		service2.setInstanceCount("-1");
		service2.setResourceSelection("customProperties notContains \"ServiceType=System\"");
		
		bundle.addService(service2);
		bundle.addService(service1);
		
		Thread thread = new Thread(){
			public void run() {
				//startup the agents after a short pause
				try {
					Thread.sleep(1500);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				for (ResourceAgent agent : agents){
					agent.start();
				}
			}
		};
		thread.start();
		
		
		System.out.println("********** INSTALL ******************************************* ");
		bundleHandler.install(bundle);
		System.out.println("********** DEPLOY ******************************************* ");
		bundleHandler.deploy(bundle.getId(), ".");
		int wait = 0;
		System.err.println("1 Waiting.... callcount is:" + callCount);
		
		while (callCount <= workerCount && wait++ < 60) {
			System.err.println("Waiting.... callcount is:" + callCount + " workerCount is:" + workerCount);
			Thread.sleep(1000); 
		}
		thread.join();
		
		assertEquals(workerCount+1, callCount.intValue());
	}
}

