package com.liquidlabs.vso.container;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.liquidlabs.transport.proxy.ProxyFactoryImpl;
import com.liquidlabs.vso.FunctionalTestBase;
import com.liquidlabs.vso.agent.ResourceAgent;
import com.liquidlabs.vso.container.sla.GroovySLA;
import com.liquidlabs.vso.container.sla.Rule;
import com.liquidlabs.vso.container.sla.SLAValidator;
import com.liquidlabs.vso.container.sla.TimePeriod;
import com.liquidlabs.vso.deployment.bundle.Bundle;
import com.liquidlabs.vso.deployment.bundle.Service;
import com.liquidlabs.vso.work.WorkAssignment;

public class SLAContainerTBOTest extends FunctionalTestBase {
	
	private SLAContainer slaContainer;
	private DumbConsumer consumer;

	String serviceScript = 
		" println ''\n" +
		" println 'SERVICE IS RUNNING GGGGGGGGGGGGGGGG'\n"+
		" println ''\n";
		
	String addScript = 
		"import com.liquidlabs.vso.container.Add\n" +
		"return new Add('cpuCount > 0', 1)";
	
	String removeScript = 
		"import com.liquidlabs.vso.container.Remove\n" +
		"return new Remove('mflops > 0', 1);\n";
	private GroovySLA sla;
    private CountDownLatch addLatch;
    private CountDownLatch releaseLatch;

    @Override
	protected void setUp() throws Exception {
		agentCount = 3;
		super.setUp();
	
		System.out.println("================= SETUP:" + getName() + "==============================================");
	}

	private String getName() {
		return getClass().getSimpleName();
	}

	@Override
	protected void tearDown() throws Exception {
		System.out.println("==============================================" + getName() + " ========================== TEARDOWN");
		super.tearDown();
		System.out.println("==============================================" + getName() + " ========================== DONE");
	}
	
	public void testCreatesGoodLogMsg() throws Exception {
		setupSLAContainer(addScript);
		Logger logger = Logger.getLogger("stuff");
		SLAContainer.logStats(slaContainer, logger);
		
	}
	
	public void testSLAContainerAddRuleFiresOnlyOnce() throws Exception {
	
		setupSLAContainer(addScript);
		
		pause();
		slaContainer.run();
		slaContainer.run();
		slaContainer.run();
		slaContainer.run();

		assertThat(addLatch.await(10, TimeUnit.SECONDS), is(true));
		assertEquals(1, slaContainer.allocatedResources.size());
	}

    
	public void testSLAContainerRemoveRuleFiresOnlyOnce() throws Exception {
		
		setupSLAContainer(removeScript);
		
		int satisfiedCount = resourceSpace.requestResources("TEST_REQUESTXXXXXXXXXXX", 1, 9, "",  "doStuff", -1, slaContainer.getOwnerId(), "");
		pause();
		assertEquals(1, satisfiedCount);
		assertEquals(1, slaContainer.allocatedResources.size());
		
		
		pause();
		slaContainer.run();
		slaContainer.run();
		slaContainer.run();
		slaContainer.run();

		assertThat(releaseLatch.await(10, TimeUnit.SECONDS), is(true));
	}

	private void setupSLAContainer(String slaScript) throws Exception {
		
		Bundle bundle = getABundle(slaScript);
	
		bundleHandler.install(bundle);
		bundleHandler.deploy(bundle.getId(), ".");
		WorkAssignment workAssignment = bundleHandler.getWorkAssignmentForService("myBundle-0.01:serviceToRun");
		
		pause();
		
		ProxyFactoryImpl proxyFactory = proxyFactories.get(0);
		slaContainer = new SLAContainer("resourceId", "myBundle-0.01:serviceToRun", workAssignment, "consumerName", consumer, sla, "sla.xml", resourceSpace, workAllocator, monitorSpace, proxyFactory.getAddress(), ".", "myBundle", null, proxyFactory, agents.get(0), new SLAValidator());
		proxyFactory.registerMethodReceiver("slaContainer", slaContainer);
		resourceSpace.registerAllocListener(slaContainer, slaContainer.getOwnerId(), slaContainer.getOwnerId());
		
		pause();
		
	}
	private Bundle getABundle(String slaScript) {
		
		sla = new GroovySLA();
		sla.setConsumerClass(DumbConsumer.class.getName());
		Rule addRule = new Rule(slaScript, 3, 10);
		
		TimePeriod period = new TimePeriod(addRule, "00:00", "23:59");
		period.setOneOff(true);
		sla.addTimePeriod(period);
        addLatch = new CountDownLatch(1);
        releaseLatch = new CountDownLatch(1);
        consumer = new DumbConsumer(addLatch, releaseLatch);
		
		for (ResourceAgent resourceAgent : this.agents) {
			resourceAgent.addDeployedBundle("myBundle-0.01", "now");
		}
		
		pause();
		Bundle bundle = new Bundle("myBundle", "0.01");
		bundle.addService(new Service(bundle.getId(), "serviceToRun", serviceScript, "1"));
		return bundle;
	}

	private void pause() {
		try {
			Thread.sleep(1 * 1000);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
}
