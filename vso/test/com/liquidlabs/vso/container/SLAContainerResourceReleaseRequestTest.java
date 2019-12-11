package com.liquidlabs.vso.container;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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
import com.liquidlabs.vso.resource.AllocListener;
import com.liquidlabs.vso.work.WorkAssignment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SLAContainerResourceReleaseRequestTest extends FunctionalTestBase {
	
	private static final String MY_BUNDLE_0_01 = "myBundle-0.01";
	private static final String SERVICE_NAME = "serviceToRun";
	private SLAContainer slaContainer;
	Action testAction;
	private DumbConsumer consumer;
	private String scriptOne = "print 'script ===================== '";
	private Bundle bundle;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		System.out.println(getName() + "==============================================");
		for (ResourceAgent agent : agents) {
			agent.addDeployedBundle(MY_BUNDLE_0_01, "r");
		}
		bundle = new Bundle("myBundle", "0.01");
		bundle.addService(new Service(bundle.getId(), SERVICE_NAME, scriptOne, "0"));
		
		bundleHandler.install(bundle);
		bundleHandler.deploy(bundle.getId(), ".");
		
		System.out.println(getName() + "=======SETUP DONE=================================\n\n");
	}

	private String getName() {
		return getClass().getSimpleName();
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
	}
	public void testShouldReleaseLowestPriorityAllocation() throws Exception {
		scriptOne = "import com.liquidlabs.vso.container.Add\n" +
		"    return new Add(\"cpuCount > 0\", 1)";
		GroovySLA sla = new GroovySLA();
		sla.setConsumerClass(DumbConsumer.class.getName());
		
		// need to test priority is accounted for in RELEASE negotiation
		ArrayList<Rule> rules = new ArrayList<Rule>();
		for (int i = 0; i < super.agents.size(); i++){
			int priority = super.agents.size() - i;
			rules.add(new Rule(scriptOne, i, priority));
		}
		
		TimePeriod timePeriod = new TimePeriod(new Rule("",0,0), "00:00", "23:59");
		timePeriod.setRules(rules);
		sla.addTimePeriod(timePeriod);
		
		consumer = new DumbConsumer();
		
		ProxyFactoryImpl proxyFactory = proxyFactories.get(0);
		
		String slaResourceId = "slaContainerResourceId";
		WorkAssignment workAssignment = new WorkAssignment(slaResourceId, slaResourceId, 0, bundle.getId(), "slaContainer", "", 100);
		slaContainer = new SLAContainer(workAssignment.getId(), bundle.getId() + ":serviceToRun", workAssignment, "consumerName", consumer, sla, "sla.xml", resourceSpace, workAllocator, monitorSpace, proxyFactory.getAddress(), ".", bundle.getId(), null, proxyFactory, agents.get(0), new SLAValidator());
		workAllocator.getWorkAllocatorSpace().store(workAssignment, -1);
		
		proxyFactory.registerMethodReceiver("slaContainer", slaContainer);
		resourceSpace.registerAllocListener(slaContainer, slaContainer.getOwnerId(), slaContainer.getOwnerId());
		
		consumer.setQueueLength(2.0);
		
		for (int count = 0; count < super.agents.size(); count++){
			System.out.println(super.agents.size() + " =================================== RUN ======================= " + count + " size:" + consumer.myResources.size() + " " + consumer.myResources);
			slaContainer.run();
			shortPause();		
		}
		
		Thread.sleep(2000);
		System.out.println(super.agents.size() + " DONE =================================== RUN ======================= DONE size:" + consumer.myResources.size() + " " + consumer.myResources);
		
		// whatever was added last to DumbConsumer has the lowest priority (based upon the Rules (max, priority))
		String lastResourceId = consumer.myResources.get(consumer.myResources.size()-1);
		
		
		MyResourceAllocListener allocListener = new MyResourceAllocListener();
		resourceSpace.registerAllocListener(allocListener, consumer.name(), "consumerB");
		
		Thread.sleep(500);
		// last item should be added with priority 1 - since add grabbed all agents and each rule requested in descending order
		// of priority down to 1
		int lowPriority = 2;
		
		System.out.println("\n\n\n=====<<<<<<<<<<<<<<<<<<<<<<<<<<<<< REQUEST ==========");
		int requestResources = resourceSpace.requestResources("**************ConsumerB-release", 1, lowPriority, "", bundle.getId() + ":serviceToRun", -1, "consumerB", "");
		
		slaContainer.run();

		Thread.sleep(2000);
		
		System.out.println("\n\n\n=====<<<<<<<<<<<<<<<<<<<<<<<<<<<<< DONE REQUEST ==========");
		assertEquals("Resources:" + allocListener.resourceIds + " Should have been given an item", 1, requestResources);
		assertTrue("Should have items in the allocListener, but was empty", allocListener.resourceIds.size() > 0 );
		assertEquals("Should have released Item with LowerPriority!:" + lastResourceId, lastResourceId, allocListener.resourceIds.get(0));
		
		
	}

	private void shortPause() throws InterruptedException {
		Thread.sleep(200);
	}
	
	public class TesterSLA extends SLA1 {
		public Action getAction(Set<String> allocatedResources) {
			return testAction;
		}
	}


	public static class MyResourceAllocListener implements AllocListener {
		public List<String> resourceIds = new ArrayList<String>();
		public void add(String requestId, List<String> resourceIds, String owner, int priority) {
			for (String resourceId : resourceIds) {
				System.out.println("================>>  ADDING ResourceId:" + resourceId + " Got:" + resourceId);
				this.resourceIds.add(resourceId);
			}
		}
		public List<String> release(String requestId, List<String> resourceIds, int releaseCount) {
			ArrayList<String> result = new ArrayList<String>();
			for (String string : result) {
				if (result.size() < releaseCount) result.add(string);
			}
			System.out.println("=================<< RELEASE ResourceId:" + result);
			return result;
		}
		public void take(String requestId, String owner, List<String> resourceIds) {
			for (String resourceId : resourceIds) {
				System.out.println("=================<< REMOVING ResourceId:" + resourceId);
				Iterator<String> iterator = resourceIds.iterator();
				while (iterator.hasNext()){
					if (iterator.next().equals(requestId)) {
						iterator.remove();
					}
				}
			}
		}
		public void pending(String requestId, List<String> resourceIds, String owner, int priority) {
		}
		public void satisfied(String requestId, String owner, List<String> resourceIds) {
		}
		public String getId() {
			return "testId";
		}
	}
}
