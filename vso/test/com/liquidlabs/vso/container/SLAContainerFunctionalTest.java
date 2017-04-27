package com.liquidlabs.vso.container;

import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.transport.proxy.ProxyFactoryImpl;
import com.liquidlabs.vso.FunctionalTestBase;
import com.liquidlabs.vso.agent.ResourceAgent;
import com.liquidlabs.vso.agent.ResourceAgentImpl;
import com.liquidlabs.vso.container.sla.GroovySLA;
import com.liquidlabs.vso.container.sla.Rule;
import com.liquidlabs.vso.container.sla.SLAValidator;
import com.liquidlabs.vso.container.sla.TimePeriod;
import com.liquidlabs.vso.deployment.bundle.Bundle;
import com.liquidlabs.vso.deployment.bundle.Service;
import com.liquidlabs.vso.work.WorkAssignment;

import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class SLAContainerFunctionalTest extends FunctionalTestBase {

    private SLAContainer slaContainer;
    private String ownerId;
    private DumbConsumer consumer;

    String script = "import com.liquidlabs.vso.container.Add\n" +
            "import com.liquidlabs.vso.container.Remove\n" +
            "System.out.println(\"QueueLength \" + queueLength);\n" +
            "if (queueLength < 10.0) " +
            "\treturn new Remove(\"mflops > 0\", 1);\n" +
            "else\n" +
            "\treturn new Add(\"cpuCount > 0\", 1)";
    private CountDownLatch adds;
    private CountDownLatch removes;

    protected void setUp() throws Exception {
        System.out.println(new Date() + " SETUP AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
        super.setUp();
        System.out.println(new Date() + " SETUP VBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB");

        GroovySLA sla = new GroovySLA();
        sla.setConsumerClass(DumbConsumer.class.getName());
        Rule rule = new Rule(script, 3, 10);
        sla.addTimePeriod(new TimePeriod(rule, "00:00", "23:59"));
        adds = new CountDownLatch(1);
        removes = new CountDownLatch(1);
        consumer = new DumbConsumer(adds, removes);

        for (ResourceAgent resourceAgent : this.agents) {
            resourceAgent.addDeployedBundle("myBundle-0.01", "releaseDate");
        }
        System.out.println(new Date() + " SETUP CCCCCCCCCCCCCCCCCCCCCCCCCCCC");

        Bundle bundle = new Bundle("myBundle", "0.01");
        bundle.addService(new Service(bundle.getId(), "serviceToRun", script, "1"));
        bundleHandler.install(bundle);
        bundleHandler.deploy(bundle.getId(), ".");
        WorkAssignment workAssignment = bundleHandler.getWorkAssignmentForService("myBundle-0.01:serviceToRun");


        ProxyFactoryImpl proxyFactory = proxyFactories.get(0);
        slaContainer = new SLAContainer("resourceId", "myBundle-0.01:serviceToRun", workAssignment, "consumerName", consumer, sla, "sla.xml", resourceSpace, workAllocator, monitorSpace, proxyFactory.getAddress(), ".", "myBundle", null, proxyFactory, agents.get(0), new SLAValidator());
        proxyFactory.registerMethodReceiver("slaContainer", slaContainer);

        resourceSpace.registerAllocListener(slaContainer, slaContainer.getOwnerId(), slaContainer.getOwnerId());
        ownerId = slaContainer.getOwnerId();

        pause();
        System.out.println(new Date() + " SETUP DDDDDDDDDDDDDDDDDDDDDDDDDD ");
        System.out.println(new Date() + "================= SETUP:" + getName() + "==============================================");
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void XXXXX_DISABLED_FOR_TEAMCITY_testAResourceSpaceCanFORCEFreeResourceAndSLAContainerIsAware() throws Exception {
	    ResourceAgentImpl agent = (ResourceAgentImpl) agents.get(1);
	
	    pause();
	
	    // SLA should request resources when SLAContainer executes
	    String resourceId = agent.getResourceProfile().getResourceId();
	    resourceSpace.assignResources("req1", Arrays.asList(resourceId), ownerId, 1, "someWorkId", -1);
	    String[] resourceIds = resourceSpace.getResourceIdsForAssigned(ownerId);
	
	    assertEquals(1, resourceIds.length);
	
	    assertTrue(adds.await(10, TimeUnit.SECONDS));
	    assertEquals(1, slaContainer.getResourceCount());
	
	    // free the resource to make the SLAContainer release it
	    resourceSpace.forceFreeResourceAllocation("", "requestId", resourceIds[0]);
	
	    assertTrue(removes.await(10, TimeUnit.SECONDS));
	    assertEquals(0, slaContainer.allocatedResources.size());
	
	
	}

	public void testSLAContainerRunnableRequestsResources() throws Exception {

        // make SLA Container Grow
        consumer.setQueueLength(111.0);

        slaContainer.run();
        
        pause();

        // SlaContainerRunnable to pass evaluation onto ResourceContainer
        assertTrue(adds.await(10, TimeUnit.SECONDS));
        assertEquals("Expected to be given 1 allocd resource",1, slaContainer.allocatedResources.size());


        // Test 2 - should shrink
        consumer.setQueueLength(5.0);

        slaContainer.run();
        pause();
        assertTrue(removes.await(10, TimeUnit.SECONDS));
        assertEquals(0, slaContainer.allocatedResources.size());
    }


    public void XXXXX_DISABLED_FOR_TEAMCITY_testASingleResourcesGetAssignedtoSLAContainer() throws Exception {

        ResourceAgentImpl agent = (ResourceAgentImpl) agents.get(1);

        pause();

        // SLAContainer should pick up resources when assigned
        String resourceId = agent.getResourceProfile().getResourceId();
        resourceSpace.assignResources("req1", Arrays.asList(resourceId), ownerId, 1, "someWorkId", -1);
        String[] resourceIds = resourceSpace.getResourceIdsForAssigned(ownerId);

        assertEquals(1, resourceIds.length);

        assertTrue(adds.await(10, TimeUnit.SECONDS));
        assertEquals(1, slaContainer.allocatedResources.size());
        //		String resourceDetails = slaContainer.getResourceDetails(resources[0]);
    }


    private void pause() throws InterruptedException {
        Thread.sleep(500);
    }

}
