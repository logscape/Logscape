package com.liquidlabs.vso.resource;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.jmock.Mock;
import org.jmock.MockObjectTestCase;

import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.agent.ResourceProfile;
import com.liquidlabs.vso.lookup.LookupSpace;

public class OldResourceSpaceTest extends MockObjectTestCase {
	private Mock lookupSpace;
	private ResourceSpaceImpl resourceSpace;
	private String location;
	private ScheduledExecutorService scheduler;

	protected void setUp() throws Exception {
		lookupSpace = mock(LookupSpace.class);
		lookupSpace.stubs();
		lookupSpace.stubs().method("unregisterService").will(returnValue(true));
		SpaceServiceImpl resService = new SpaceServiceImpl((LookupSpace) lookupSpace.proxy(),new ORMapperFactory(), ResourceSpace.NAME + "_RES", Executors.newScheduledThreadPool(2), false, false, false);
		SpaceServiceImpl allocService = new SpaceServiceImpl((LookupSpace) lookupSpace.proxy(),new ORMapperFactory(), ResourceSpace.NAME + "_ALLOC", Executors.newScheduledThreadPool(2), false, false, false);
		SpaceServiceImpl allResourcesEver = new SpaceServiceImpl((LookupSpace) lookupSpace.proxy(),new ORMapperFactory(), ResourceSpace.NAME + "_ALL", Executors.newScheduledThreadPool(2), false, false, true);

		resourceSpace = new ResourceSpaceImpl(resService, allocService, allResourcesEver);
		resourceSpace.start();
		
		// create some resources
		for (int i = 0; i < 100; i++) {
			ResourceProfile resourceProfile = createResource(i);
			
			resourceSpace.registerResource(resourceProfile, -1);
		}

		System.out.println("===================SETUP======== " + getName() + " =======================================");
	}
	protected void tearDown() throws Exception {
		System.out.println("===================TEARDOWN======== " + getName() + " =======================================");
		resourceSpace.stop();
	}
	
	public void testShouldExpireLeases() throws Exception {
		MyRegListener myRegListener = new MyRegListener();
		resourceSpace.registerResourceRegisterListener(myRegListener, "TEST-LISTENER", null, -1);
		ResourceProfile resource = createResource(110);
		resourceSpace.registerResource(resource, 2);
		Thread.sleep(100);
		resourceSpace.registerResource(resource, 10);
		Thread.sleep(11000);
		ResourceProfile profile = resourceSpace.getResourceDetails("Resource=110");
		assertNull("the lease should have expired", profile);
		assertNotNull("should have called unregister", myRegListener.unregistered);
	}
	
	

	private ResourceProfile createResource(int id) {
		ResourceProfile resourceProfile = new ResourceProfile();
		resourceProfile.setResourceId("Resource:" + id);
		resourceProfile.setDeployedBundles("replicator-1.0");
		resourceProfile.setDeployedBundles("vso-ui-1.0");
		resourceProfile.setDeployedBundles("flow-ex2-1.0");
		if (id < 95) {
			resourceProfile.setOwnership("DEDICATED");
		}
		else {
			resourceProfile.setOwnership("DESKTOP");
		}
		return resourceProfile;
	}
	
	public void testShouldExpand2Groups() throws Exception {
		String expandedGroup = resourceSpace.expandResourceGroups("group('Dedicated') OR group('Harvested') AND deployedBundles contains \"matrix-1.0\"");
		System.err.println("grp:" + expandedGroup);
	}
	public void testShouldExpandResourceGroupIntoSelectionString() throws Exception {
		resourceSpace.registerResourceGroup(new ResourceGroup("myGroup","mflops > 0 AND resourceId containsAny 'one-bit,two'","does stuff", "now"));
		String expandedGroup = resourceSpace.expandResourceGroups("group('myGroup') AND mflop > 0");
		System.err.println(expandedGroup);
		assertTrue(expandedGroup.contains("containsAny 'one-bit,two'"));
	}
	
	public void testShouldFindResourceWithMFLopsANDOwnership() throws Exception {
		List<String> resourceIds = resourceSpace.findResourceIdsBy("mflops > 0 AND ownership equals DEDICATED AND deployedBundles contains \"flow-ex2-1.0\"");
		assertEquals("Should have got 95 resources!", 95, resourceIds.size());
	}
	public void testShouldFindResourceWithDeployedBundle() throws Exception {
		int requestResources = resourceSpace.requestResources("requestId", 1, 1, "mflops > 0 AND deployedBundles contains \"flow-ex2-1.0\" ", "workId", 1000, "requestOwnerId", "");
		assertEquals(1, requestResources);
	}
	
	
	
	static class MyRegListener implements ResourceRegisterListener {
		public String unregistered;
		
		public void register(String resourceId, ResourceProfile resourceProfile) {
			
		}

		public void unregister(String resourceId,
				ResourceProfile resourceProfile) {
			unregistered = resourceId;
		}

		public String getId() {
			return "RRListener";
		}
		
	}
	
	public void testFindByMFLops() throws Exception {
		System.out.println("Purged: " + resourceSpace.purge());
		createProfiles(80, 4);
		List<String> findResourceIdsBy = resourceSpace.findResourceIdsBy("mflops > 0 AND ownership equals DEDICATED");
		assertEquals(4, findResourceIdsBy.size());
	}
	public void testFindByCustomProperties() throws Exception {
		ResourceProfile profile = new ResourceProfile("url", 0, scheduler);
		profile.setHostName("localhost");
		profile.setPort(90);
		profile.setCustomProperty("grid", "http://jars.db.com:80");
		profile.setCustomProperty("config", "active");
		profile.setCustomProperty("nonmanaged", "true");
		String registerResource = resourceSpace.registerResource(profile, 1000);
		
		assertNotNull(registerResource);
		List<String> ids = resourceSpace.findResourceIdsBy("customProperties contains nonmanaged=true AND customProperties notContains ServiceType=System");
		assertEquals(1,ids.size());
	}
	
	private void createProfiles(int port, int count) throws Exception {
		for(int i = 0; i<count;i++) {
			ResourceProfile profile = new ResourceProfile("url", i, scheduler);
			profile.setHostName("localhost");
			profile.setPort(port);
			profile.setDeployedBundles("myBundle");
			String registerResource = resourceSpace.registerResource(profile, 100000);
			assertNotNull("Got null for lease", registerResource);
		}
	}
	
	
}
