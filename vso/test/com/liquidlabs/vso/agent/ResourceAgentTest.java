package com.liquidlabs.vso.agent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.liquidlabs.vso.deployment.ScriptForker;
import org.jmock.Mockery;
import org.junit.Before;
import org.junit.Test;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.LifeCycle.State;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.agent.process.ProcessHandler;
import com.liquidlabs.vso.deployment.DeploymentService;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.resource.ResourceSpace;
import com.liquidlabs.vso.work.WorkAllocator;
import com.liquidlabs.vso.work.WorkAssignment;

public class ResourceAgentTest  {
	
	Mockery context = new Mockery();
	
	private ResourceAgentImpl resourceAgent;
	private WorkAllocator workAllocator;
	private ResourceSpace resourceSpace;
	private LookupSpace lookupSpace;
	private ProxyFactory proxyFactory;
	private DeploymentService deploymentService;
	
	ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);
    private ScriptForker scriptForker;
    private ProcessHandler processHandler;


    @Before
	public void setUp() throws Exception {
    	System.setProperty("test.mode","true");
	
		workAllocator = mock(WorkAllocator.class);
		resourceSpace = mock(ResourceSpace.class);
		lookupSpace = mock(LookupSpace.class);
		proxyFactory = mock(ProxyFactory.class);
		deploymentService = mock(DeploymentService.class);
        scriptForker = mock(ScriptForker.class);
        processHandler = mock(ProcessHandler.class);

        when(proxyFactory.getAddress()).thenReturn(new URI("tcp://stuff:8000"));
        when(proxyFactory.getScheduler()).thenReturn(scheduler);
        when(proxyFactory.getExecutor()).thenReturn(scheduler);
        when(resourceSpace.getSystemResourceId()).thenReturn(100);

		resourceAgent = new ResourceAgentImpl(workAllocator, resourceSpace, deploymentService, lookupSpace, proxyFactory, "tcp://localhost:11000", "http://stuff", 2);
		resourceAgent.setScriptForker(scriptForker);
        resourceAgent.setProcessHandler(processHandler);

		System.setProperty("test.mode", "true");
		
		resourceAgent.start();
		
	}


    @Test
    public void testShouldGetCorrectFilesForDelete() throws Exception {
        boolean deleteMe = resourceAgent.acceptForDelete(new File("/opt/logscape/work/CRAP_SERVER_/yay.log"));
        assertFalse(deleteMe);
        deleteMe = resourceAgent.acceptForDelete(new File("/opt/logscape/work/CRAP/yay.log"));
        assertTrue(deleteMe);


    }

	
	@Test
	public void testShouldGetGoodReleaseDate() throws Exception {
		String result = resourceAgent.removeReleaseDateTags(" <buildId>28-Oct-10 17:53:29</buildId>");
		assertEquals("28-Oct-10 17:53:29", result);
		
	}
	

	@Test
	public void testShouldMarkForegroundExitingAsError() throws Exception {

		List<ResourceProfile> profiles = resourceAgent.getProfiles();
		for (ResourceProfile resourceProfile : profiles) {
			resourceProfile.setHostName("myHost");
			resourceProfile.setPort(8000);
		}

        when(processHandler.manage(any(WorkAssignment.class), any(Process.class))).thenThrow(new RuntimeException());
		resourceAgent.start(createExitingWorkAssignment(profiles.get(0), 1, false));
		

//		assertTrue("WorkIds should NOT be empty, was [" + profiles.get(0).getWorkIds() + "]", profiles.get(0).getWorkId().contains("myBundle"));
//        verify(workAllocator).update(anyString(), contains("RUNNING"));
        verify(workAllocator).update(anyString(), contains("replaceWith ERROR AND errorMsg replaceWith 'process"));
	}
    
	@Test
	public void testShouldMarkBackgroundExitingAsError() throws Exception {

		List<ResourceProfile> profiles = resourceAgent.getProfiles();
		for (ResourceProfile resourceProfile : profiles) {
			resourceProfile.setHostName("myHost");
			resourceProfile.setPort(8000);
		}
		
		when(processHandler.isRunning(0)).thenReturn(true);
		resourceAgent.start(createExitingWorkAssignment(profiles.get(0), 1, true));
		

		assertTrue("WorkIds should NOT be empty, was [" + profiles.get(0).getWorkIds() + "]", profiles.get(0).getWorkId().contains("myBundle"));
	}

	@Test
    public void testShouldFailingToStartProcessAsError() throws Exception {

		List<ResourceProfile> profiles = resourceAgent.getProfiles();
		for (ResourceProfile resourceProfile : profiles) {
			resourceProfile.setHostName("myHost");
			resourceProfile.setPort(8000);
		}

        when(processHandler.manage(any(WorkAssignment.class), any(Process.class))).thenThrow(new RuntimeException());
		resourceAgent.start(createBadWorkAssignment(profiles.get(0)));

		assertTrue("WorkIds should be empty, was [" + profiles.get(0).getWorkIds() + "]", profiles.get(0).getWorkId().isEmpty());
        verify(workAllocator).update(anyString(), contains("ERROR"));

	}

	@Test
	public void testShouldStopTheCorrectServiceInstance() throws Exception {
		
		List<ResourceProfile> profiles = resourceAgent.getProfiles();
		for (ResourceProfile resourceProfile : profiles) {
			resourceProfile.setHostName("myHost");
			resourceProfile.setPort(8000);
		}
		WorkAssignment ws1 = createWorkAssignment(profiles.get(0), false);
		when(processHandler.isRunning(0)).thenReturn(true);
		
		resourceAgent.start(ws1);
		resourceAgent.start(createWorkAssignment(profiles.get(1), false));
		resourceAgent.stop(ws1);
		assertEquals("WorkIds should be empty, but wasnt - was [" + profiles.get(0).getWorkIds() + "]", "", profiles.get(0).getWorkId());
		assertEquals("myhost-8000-1:myBundle-1.0:Service", profiles.get(1).getWorkId());
	}	
	
	@Test
	public void testShouldFindDeployedBundlesDir() throws Exception {
		resourceAgent.scanForDeployments("test-data/deployed-bundles");
		ResourceProfile resourceProfile = resourceAgent.getResourceProfile();
		String deployedBundles = resourceProfile.getDeployedBundles();
		assertTrue("1 DeployedBundles was:" + deployedBundles, deployedBundles.contains("someAppA-1.0"));
		assertTrue("2 DeployedBundles was:" + deployedBundles, deployedBundles.contains("someAppB-1.0"));
		assertTrue("3 DeployedBundles was:" + deployedBundles, deployedBundles.contains("someAppB-DEV-1.0"));
		
	}
	@Test
	public void testShouldUndeployBundleName() throws Exception {
		
		resourceAgent.scanForDeployments("test-data/deployed-bundles");
		
		resourceAgent.removeBundle("someAppA-1.0");
		
		ResourceProfile resourceProfile = resourceAgent.getResourceProfile();
		String deployedBundles = resourceProfile.getDeployedBundles();
		System.err.println(deployedBundles);
		assertFalse("Should have removed someAppA:" + deployedBundles, deployedBundles.contains("someAppA-1.0"));
		assertTrue("DeployedBundles was:" + deployedBundles, deployedBundles.contains("someAppB-1.0"));
	}
	@Test
	public void testShouldScanOnStartup() throws Exception {
		VSOProperties.setSystemBundleDir("build");
		VSOProperties.setDeployedBundleDir("test-data/deployed-bundles");
		resourceAgent.scanForDeployments();
		ResourceProfile resourceProfile = resourceAgent.getResourceProfile();
		String deployedBundles = resourceProfile.getDeployedBundles();
		assertTrue("1 Deployed bundles was:" + deployedBundles, deployedBundles.contains("someAppA-1.0"));
		assertTrue("2 Deployed bundles was:" + deployedBundles, deployedBundles.contains("someAppB-1.0"));
	}
	
	@Test
	public void testShouldHaveMoreThanOneProfile() throws Exception {
//		resourceAgent = new ResourceAgentImpl((WorkAllocator)workAllocator.proxy(), (ResourceSpace)resourceSpace.proxy(), (LookupSpace)lookupSpace.proxy(), (ProxyFactory) proxyFactory.proxy(), "tcp://localhost:11000", "http://stuff", 2);
		List<ResourceProfile> profiles = resourceAgent.getProfiles();
		assertEquals(2, profiles.size());
		assertEquals(0, profiles.get(0).getId());
		assertEquals(1,profiles.get(1).getId());
	}
	
	@Test
	public void testShouldRegisterMultipleResources() throws Exception {
        when(resourceSpace.registerResource(any(ResourceProfile.class), anyInt())).thenReturn("lease1").thenReturn("lease2");
		resourceAgent.start();
        verify(resourceSpace, times(2)).registerResource(any(ResourceProfile.class), anyInt());
	}
	
	@Test
	public void testShouldStartWorkWithCorrectProfile() throws Exception {
		
		List<ResourceProfile> profiles = resourceAgent.getProfiles();
		for (ResourceProfile resourceProfile : profiles) {
			resourceProfile.setHostName("myHost");
			resourceProfile.setPort(8000);
		}
		when(processHandler.isRunning(0)).thenReturn(true);
		resourceAgent.start(createWorkAssignment(profiles.get(0), false));
		resourceAgent.start(createWorkAssignment(profiles.get(1), false));
		assertEquals("myhost-8000-0:myBundle-1.0:Service", profiles.get(0).getWorkId());
		assertEquals("myhost-8000-1:myBundle-1.0:Service", profiles.get(1).getWorkId());
	}
	
	

	@Test
	public void testShouldBounceForegroundServiceThatExited() throws Exception {
		List<ResourceProfile> profiles = resourceAgent.getProfiles();
		for (ResourceProfile resourceProfile : profiles) {
			resourceProfile.setHostName("myHost");
			resourceProfile.setPort(8000);
		}
		when(processHandler.isRunning(0)).thenReturn(true);
		WorkAssignment ws1 = createExitingWorkAssignment(profiles.get(0), 1, false);
		resourceAgent.start(ws1);
		assertEquals("myhost-8000-0:myBundle-1.0:Service", profiles.get(0).getWorkId());
		resourceAgent.stop(ws1);
		assertEquals("", profiles.get(0).getWorkId());
		ws1.setStatus(State.ASSIGNED);
		resourceAgent.start(ws1);
		assertEquals("myhost-8000-0:myBundle-1.0:Service", profiles.get(0).getWorkId());
	}
	@Test
	public void testShouldBounceBackgroundServiceThatExited() throws Exception {
		List<ResourceProfile> profiles = resourceAgent.getProfiles();
		for (ResourceProfile resourceProfile : profiles) {
			resourceProfile.setHostName("myHost");
			resourceProfile.setPort(8000);
		}
		when(processHandler.isRunning(0)).thenReturn(true);
		WorkAssignment ws1 = createExitingWorkAssignment(profiles.get(0), 1, true);
		resourceAgent.start(ws1);
		assertEquals("myhost-8000-0:myBundle-1.0:Service", profiles.get(0).getWorkId());
		resourceAgent.stop(ws1);
		assertEquals("", profiles.get(0).getWorkId());
		ws1.setStatus(State.ASSIGNED);
		resourceAgent.start(ws1);
		assertEquals("myhost-8000-0:myBundle-1.0:Service", profiles.get(0).getWorkId());
	}
	
	@Test
	public void testShouldBounceBackgroundServiceInstance() throws Exception {

		List<ResourceProfile> profiles = resourceAgent.getProfiles();
		for (ResourceProfile resourceProfile : profiles) {
			resourceProfile.setHostName("myHost");
			resourceProfile.setPort(8000);
		}
		when(processHandler.isRunning(0)).thenReturn(true);
		WorkAssignment ws1 = createWorkAssignment(profiles.get(0), true);
		resourceAgent.start(ws1);
		assertEquals("myhost-8000-0:myBundle-1.0:Service", profiles.get(0).getWorkId());
		resourceAgent.stop(ws1);
		ws1.setStatus(State.ASSIGNED);
		assertEquals("", profiles.get(0).getWorkId());
		resourceAgent.start(ws1);
		assertEquals("myhost-8000-0:myBundle-1.0:Service", profiles.get(0).getWorkId());
		resourceAgent.stop(ws1);
		assertEquals("", profiles.get(1).getWorkId());
	}
	
	@Test
	public void testShouldReallyStopTheCorrectServiceInstance() throws Exception {
		List<ResourceProfile> profiles = resourceAgent.getProfiles();
		for (ResourceProfile resourceProfile : profiles) {
			resourceProfile.setHostName("myHost");
			resourceProfile.setPort(8000);
		}
        when(processHandler.isRunning(0)).thenReturn(true);
		WorkAssignment ws1 = createWorkAssignment(profiles.get(0), true);
		resourceAgent.start(ws1);
		WorkAssignment ws2 = createWorkAssignment(profiles.get(1), true);
		resourceAgent.start(ws2);
		resourceAgent.stop(ws2);
		assertEquals("myhost-8000-0:myBundle-1.0:Service", profiles.get(0).getWorkId());
		assertEquals("", profiles.get(1).getWorkId());
	}

	@Test
	public void testShouldDeployBundleToAllResourceProfiles() throws Exception {
		List<ResourceProfile> profiles = resourceAgent.getProfiles();
		for (ResourceProfile resourceProfile : profiles) {
			resourceProfile.setHostName("myHost");
			resourceProfile.setPort(8000);
		}
		resourceAgent.addDeployedBundle("aBundle", "now");
		for (ResourceProfile profile : profiles) {
			assertTrue(profile.getDeployedBundles().contains("aBundle"));
		}
	}

	private WorkAssignment createWorkAssignment(ResourceProfile resourceProfile, boolean background) {
		WorkAssignment workAssignment = new WorkAssignment(resourceProfile.agentId, resourceProfile.resourceId, resourceProfile.getId(),"myBundle-1.0", "Service", "processMaker.fork 'sleep', '120'", 9);
		workAssignment.setStatus(LifeCycle.State.ASSIGNED);
		workAssignment.setFork(true);
		workAssignment.setBackground(background);
		return workAssignment;
	}
	private WorkAssignment createBadWorkAssignment(ResourceProfile resourceProfile) {
		WorkAssignment workAssignment = new WorkAssignment(resourceProfile.agentId, resourceProfile.resourceId, resourceProfile.getId(),"myBundle-1.0", "Service", "processMaker.fork 'XXXX', '2'", 9);
		workAssignment.setStatus(LifeCycle.State.ASSIGNED);
		workAssignment.setFork(true);
		return workAssignment;
	}
	private WorkAssignment createExitingWorkAssignment(ResourceProfile resourceProfile, int sleep, boolean background) {
		WorkAssignment workAssignment = new WorkAssignment(resourceProfile.agentId, resourceProfile.resourceId, resourceProfile.getId(),"myBundle-1.0", "Service", String.format("processMaker.fork 'sleep', '%d'", sleep), 9);
		workAssignment.setStatus(LifeCycle.State.ASSIGNED);
		workAssignment.setFork(true);
		workAssignment.setBackground(background);
		return workAssignment;
	}

}
