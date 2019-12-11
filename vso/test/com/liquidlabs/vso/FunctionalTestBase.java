package com.liquidlabs.vso;

import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.liquidlabs.common.TestModeSetter;
import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.transport.Config;
import com.liquidlabs.transport.TransportFactoryImpl;
import com.liquidlabs.transport.TransportProperties;
import com.liquidlabs.transport.proxy.ProxyFactoryImpl;
import com.liquidlabs.vso.agent.ResourceAgent;
import com.liquidlabs.vso.agent.ResourceAgentImpl;
import com.liquidlabs.vso.deployment.BundleDeploymentService;
import com.liquidlabs.vso.deployment.bundle.BundleHandler;
import com.liquidlabs.vso.deployment.bundle.BundleHandlerImpl;
import com.liquidlabs.vso.deployment.bundle.BundleSpace;
import com.liquidlabs.vso.deployment.bundle.BundleSpaceImpl;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.LookupSpaceImpl;
import com.liquidlabs.vso.monitor.MonitorSpace;
import com.liquidlabs.vso.monitor.MonitorSpaceImpl;
import com.liquidlabs.vso.resource.ResourceSpace;
import com.liquidlabs.vso.resource.ResourceSpaceImpl;
import com.liquidlabs.vso.work.WorkAllocator;
import com.liquidlabs.vso.work.WorkAllocatorImpl;

public class FunctionalTestBase  {
	static int callCount = 0;
	
	protected BundleHandler bundleHandler;
	protected BundleSpace bundleSpace;
	protected LookupSpace lookupSpace;
	protected ResourceSpace resourceSpace;
	protected WorkAllocatorImpl workAllocator;
	protected MonitorSpace monitorSpace;
	
	protected List<ResourceAgent> agents =  new ArrayList<ResourceAgent>();
	protected List<ProxyFactoryImpl> proxyFactories = new ArrayList<ProxyFactoryImpl>();

	private ResourceSpace resourceSpaceProxy;

	private WorkAllocator workAllocatorProxy;

	private LookupSpace lookupSpaceProxy;

	protected BundleSpace bundleSpaceProxy;

	private TransportFactoryImpl transportFactory;

	private java.util.concurrent.ExecutorService executor;

	private BundleDeploymentService bundleDeploymentService;
	static int count = 0;

	protected void setUp() throws Exception {
		TestModeSetter.setTestMode();
		
		VSOProperties.setBasePort(10000 + count++);
		TransportProperties.setInvocationTimeoutSecs(5);
		TransportProperties.setMCastEnabled(Boolean.FALSE);
		
		System.out.println(new Date().toString() + " ================================== Setup =================================");
		

		
		executor = ExecutorService.newDynamicThreadPool("worker", ResourceSpace.NAME);
		transportFactory = new TransportFactoryImpl(executor, "test");
		transportFactory.start();


		ProxyFactoryImpl proxyFactory = new ProxyFactoryImpl(transportFactory, Config.TEST_PORT -1, executor, "testBaseProxyFactory");
		proxyFactory.start();
		proxyFactories.add(proxyFactory);
		
		
		lookupSpace = new LookupSpaceImpl(VSOProperties.getBasePort(), VSOProperties.getReplicationPort());
		lookupSpace.start();
		lookupSpaceProxy = proxyFactory.getRemoteService(LookupSpace.NAME, LookupSpace.class, new String[] {  lookupSpace.getEndPoint().toString() });
		

		resourceSpace = ResourceSpaceImpl.boot( lookupSpace.getEndPoint().toString());
		resourceSpaceProxy = ResourceSpaceImpl.getRemoteService("TestSetup", lookupSpaceProxy, proxyFactory);

		
		workAllocator = WorkAllocatorImpl.boot(lookupSpace.getEndPoint().toString());
		workAllocatorProxy = WorkAllocatorImpl.getRemoteService("TestSetup", lookupSpace, proxyFactory);

		bundleSpace = BundleSpaceImpl.boot(lookupSpace.getEndPoint().toString(), null);
		bundleSpaceProxy = BundleSpaceImpl.getRemoteService("TestSetup", lookupSpace, proxyFactory);
		
		bundleDeploymentService = new BundleDeploymentService();

		monitorSpace = MonitorSpaceImpl.boot(lookupSpace.getEndPoint().toString());
		monitorSpace.start();
		setupAgents();
		
		bundleHandler = new BundleHandlerImpl(bundleSpaceProxy);
		Thread.sleep(3000);
		System.out.println(new Date().toString() + " ================================== Running =================================\n\n");
	}
	
	protected int agentCount = 10;
	protected void setupAgents() throws UnknownHostException, URISyntaxException{
		System.out.println(new Date().toString() + " ================================== Starting agents\n\n");
		for (int i = 0; i < agentCount; i++){
			createResourceAgent(true);
		}		
	}

	protected ResourceAgentImpl createResourceAgent() throws UnknownHostException, URISyntaxException {
		return createResourceAgent(true);
	}
	
	protected ResourceAgentImpl createResourceAgent(boolean startEmUp) throws UnknownHostException, URISyntaxException {
		TransportProperties.setProxySchedulerPoolSize(2);
		
		ProxyFactoryImpl proxyFactory = new ProxyFactoryImpl(transportFactory, Config.TEST_PORT, executor, "testBase2");
		proxyFactory.start();
		
		this.proxyFactories.add(proxyFactory);
		
		// Use a ProxyHandle to ensure all functionality works across the wire
		ResourceAgentImpl agent = new ResourceAgentImpl(workAllocatorProxy, resourceSpaceProxy, bundleDeploymentService, lookupSpace, proxyFactory, lookupSpace.getEndPoint().toString(), "http://jmxStuff", 1);
		proxyFactory.registerMethodReceiver(ResourceAgent.NAME, agent);
		agent.start();
		agents.add(agent);
		return agent;
	}
	
	protected void tearDown() throws Exception {
		System.out.println(new Date().toString() +" ================================== TEARDOWN =================================:count:" + agents.size());

		
	
		System.out.println(new Date().toString() +" ================================== STOPPING AGENTS xxxxxxxxxxxx =================================:count:" + agents.size());
		for (ResourceAgent agent : agents) {
			System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx Stopping:" + agent.getId());
			agent.stop();
		}
        agents.clear();
        
		System.out.println(new Date().toString() + " ================== STOPPING SERVICES=============================");
		
		
		
		System.out.println(new Date().toString() +"  -------- 1");
		monitorSpace.stop();
		
		System.out.println(new Date().toString() +"  -------- 2");
		bundleSpace.stop();
		bundleSpaceProxy = null;
		
		System.out.println(new Date().toString() +"  -------- 3");
		workAllocator.stop();
		workAllocatorProxy = null;
		
		System.out.println(new Date().toString() +"  -------- 4");
		resourceSpace.stop();
		resourceSpaceProxy = null;
		System.out.println(new Date().toString() +"  -------- 5");
		lookupSpace.stop();
		lookupSpaceProxy = null;
		
		bundleDeploymentService = null;
		
		for (ProxyFactoryImpl proxyFactory : this.proxyFactories) {
			proxyFactory.stop();
		}
		transportFactory.stop();
		System.out.println(new Date().toString() + " ================================== TEADOWN - DONE =================================");
		
		monitorSpace = null;
		bundleSpace = null;
		lookupSpace = null;
		workAllocator = null;
		resourceSpace = null;
		transportFactory = null;
		bundleHandler = null;
		bundleDeploymentService = null;
		LookupSpaceImpl.sharedMapperFactory.stop();
		LookupSpaceImpl.sharedMapperFactory = null;
		
		executor.shutdownNow();
		 
		Thread.sleep(1000);
		
	}

}
