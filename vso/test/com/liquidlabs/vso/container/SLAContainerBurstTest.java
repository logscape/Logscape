package com.liquidlabs.vso.container;

import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Set;

import com.liquidlabs.transport.proxy.ProxyFactoryImpl;
import com.liquidlabs.vso.FunctionalTestBase;
import com.liquidlabs.vso.agent.ResourceAgent;
import com.liquidlabs.vso.container.SLA1.Action;
import com.liquidlabs.vso.container.sla.GroovySLA;
import com.liquidlabs.vso.container.sla.Rule;
import com.liquidlabs.vso.container.sla.SLAValidator;
import com.liquidlabs.vso.container.sla.TimePeriod;
import com.liquidlabs.vso.deployment.bundle.Bundle;
import com.liquidlabs.vso.deployment.bundle.Service;
import com.liquidlabs.vso.work.WorkAssignment;

public class SLAContainerBurstTest extends FunctionalTestBase {
	
	private static final String SERVICE_NAME = "serviceToRun";
	private SLAContainer slaContainer;
	Action testAction;
	private DumbConsumer consumer;
	
	private int agentCount = 21;
	// SETUP
	String script = "" + 
					"import com.liquidlabs.vso.container.Add\n" +
					"import com.liquidlabs.vso.container.Remove\n" +
					"  \treturn new Add(\"cpuCount > 0\", 20)";
	private Bundle bundle;
	private WorkAssignment workAssignment;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		
		System.out.println(getName() + "==============================================");
		for (ResourceAgent agent : agents) {
				agent.addDeployedBundle("myBundle-0.01", "releaseDate");
		}
		
		bundle = new Bundle("myBundle", "0.01");
		bundle.addService(new Service(bundle.getId(), SERVICE_NAME, script, "1"));
		bundleHandler.install(bundle);
		bundleHandler.deploy(bundle.getId(), ".");
		workAssignment = bundleHandler.getWorkAssignmentForService(bundle.getId() + ":" + SERVICE_NAME);
		pause();
	}
	
	protected void setupAgents() throws UnknownHostException, URISyntaxException{
		for (int i = 0; i < agentCount; i++){
			createResourceAgent(false);
		}		
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
	}
	public void testBIGRampUpUsingSLA() throws Exception {
	
		
		GroovySLA sla = new GroovySLA();
		sla.setConsumerClass(DumbConsumer.class.getName());
		
		sla.addTimePeriod(new TimePeriod(new Rule(script, 3, 10), "00:00", "23:59"));
		consumer = new DumbConsumer();
		
		if (proxyFactories.size() ==  0) return;
		
		ProxyFactoryImpl proxyFactory = proxyFactories.get(0);
		slaContainer = new SLAContainer(WorkAssignment.getId("resourceId", bundle.getId(), SERVICE_NAME), bundle.getId() + ":serviceToRun", workAssignment, "consumerName", consumer, sla, "sla.xml", resourceSpace, workAllocator, monitorSpace, proxyFactory.getAddress(), ".", bundle.getId(), null, proxyFactory, agents.get(0), new SLAValidator());
		proxyFactory.registerMethodReceiver("slaContainer", slaContainer);
		resourceSpace.registerAllocListener(slaContainer, slaContainer.getOwnerId(), slaContainer.getOwnerId());
		
		
		consumer.setQueueLength(10.0);
		Thread.sleep(1000);
		pause();
		slaContainer.run();
		
		Thread.sleep(10000);

		assertEquals(20, slaContainer.getResourceCount());
	}

	
	
	public class TesterSLA extends SLA1 {
		public Action getAction(Set<String> allocatedResources) {
			return testAction;
		}
	}


	private void pause() throws InterruptedException {
		Thread.sleep(2 * 1000);
	}
}
