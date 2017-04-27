package com.liquidlabs.vso.container;

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

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class SLAContainerRampUpDownSimulatorTest extends FunctionalTestBase {

    private SLAContainer slaContainer;
    Action testAction;
    private DumbConsumer consumer;
    private Service service;
    private WorkAssignment workAssignment;
    private GroovySLA sla;

    // SETUP
    String scriptOne = "" +
            "import com.liquidlabs.vso.container.Add\n" +
            "import com.liquidlabs.vso.container.Remove\n" +
            "" +
            "  System.out.println(\"***** QueueLength \" + queueLength);\n" +
            "  if (queueLength == 0.0) " +
            "    \treturn new Remove(\"mflops > 0\", 1);\n" +
            "  else if (queueLength > 0) \n" +
            "  \treturn new Add(\"cpuCount > 0\", 1)";
    // SETUP
    String scriptMulti = "import com.liquidlabs.vso.container.Add\n" +
            "import com.liquidlabs.vso.container.Remove\n" +
            "System.out.println(\"***** QueueLength \" + queueLength);\n" +
            "if (queueLength < 10.0) " +
            "\treturn new Remove(\"mflops > 0\", 2);\n" +
            "else\n" +
            "\treturn new Add(\"cpuCount > 0\", 2)";

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        System.out.println(getName() + "==============================================");
        for (ResourceAgent agent : agents) {
            agent.addDeployedBundle("bundleName", "r");
        }
        pause();


        sla = new GroovySLA();
        sla.setConsumerClass(DumbConsumer.class.getName());

        String script = getName().contains("MultipleResourcesSLA") ? scriptMulti : scriptOne;

        TimePeriod period = new TimePeriod(new Rule(script, 10, 10), "00:00", "23:59");
        period.setOneOff(false);
        sla.addTimePeriod(period);
        consumer = new DumbConsumer();
        Bundle bundle = new Bundle("bundleName", "0.01");
        service = new Service(bundle.getId(), "serviceToRun", "println 'hello'", "0");
        bundle.addService(service);
        bundleSpace.registerBundle(bundle);
        bundleSpace.registerBundleService(service);
        workAssignment = bundleHandler.getWorkAssignmentForService(service.getId());
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testRampUpAndRampdownUsingSLA() throws Exception {


        if (proxyFactories.size() == 0) return;

        ProxyFactoryImpl proxyFactory = proxyFactories.get(0);
        slaContainer = new SLAContainer("resourceId", service.getId(), workAssignment, "consumerName", consumer, sla, "sla.xml", resourceSpace, workAllocator, monitorSpace, proxyFactory.getAddress(), ".", "bundleName", null, proxyFactory, agents.get(0), new SLAValidator());
        proxyFactory.registerMethodReceiver("slaContainer", slaContainer);
        resourceSpace.registerAllocListener(slaContainer, slaContainer.getOwnerId(), slaContainer.getOwnerId());


        consumer.setQueueLength(10.0);
        pause();
        slaContainer.run();
        pause();
        slaContainer.run();
        pause();
        slaContainer.run();

        Thread.sleep(2000);

        assertEquals(3, slaContainer.getResourceCount());

        consumer.setQueueLength(0.0);
        slaContainer.run();
        shortPause();
        slaContainer.run();
        shortPause();
        slaContainer.run();
        shortPause();
        slaContainer.run();
        shortPause();

        consumer.setQueueLength(0.0);
        slaContainer.run();
        shortPause();

        slaContainer.run();
        shortPause();

        slaContainer.run();
        shortPause();

        assertEquals(0, slaContainer.getResourceCount());

    }

    public void testRampUpAndRampdownUsingMultipleResourcesSLA() throws Exception {

        CountDownLatch adds = new CountDownLatch(6);
        CountDownLatch removes = new CountDownLatch(6);
        ProxyFactoryImpl proxyFactory = proxyFactories.get(0);
        DumbConsumer consumer = new DumbConsumer(adds, removes);
        slaContainer = new SLAContainer("resourceId", "serviceToRun", workAssignment, "consumerName", consumer, sla, "sla.xml", resourceSpace, workAllocator, monitorSpace, proxyFactory.getAddress(), ".", "bundleName", null, proxyFactory, agents.get(0), new SLAValidator());
        proxyFactory.registerMethodReceiver("slaContainer", slaContainer);
        resourceSpace.registerAllocListener(slaContainer, slaContainer.getOwnerId(), slaContainer.getOwnerId());
        consumer.setQueueLength(10.0);
        pause();
        slaContainer.run();
        shortPause();
        slaContainer.run();
        shortPause();
        slaContainer.run();


        assertTrue(adds.await(10, TimeUnit.SECONDS));

        consumer.setQueueLength(2.0);
        slaContainer.run();
        shortPause();
        slaContainer.run();
        shortPause();
        slaContainer.run();
        shortPause();
        slaContainer.run();
        shortPause();

        assertTrue(removes.await(10, TimeUnit.SECONDS));

    }


    private void shortPause() throws InterruptedException {
        Thread.sleep(200);
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
