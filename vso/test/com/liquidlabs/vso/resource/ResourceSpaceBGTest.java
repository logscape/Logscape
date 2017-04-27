package com.liquidlabs.vso.resource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.jmock.Mock;
import org.jmock.MockObjectTestCase;

import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.agent.ResourceProfile;
import com.liquidlabs.vso.lookup.LookupSpace;

public class ResourceSpaceBGTest extends MockObjectTestCase {
	
	
	private Mock lookupSpace;
	private ResourceSpaceImpl resourceSpace;
	private String location;

	protected void setUp() throws Exception {
		lookupSpace = mock(LookupSpace.class);
		lookupSpace.stubs();
		lookupSpace.stubs().method("unregisterService").will(returnValue(true));
		SpaceServiceImpl resService = new SpaceServiceImpl((LookupSpace) lookupSpace.proxy(),new ORMapperFactory(), ResourceSpace.NAME + "_RES", Executors.newScheduledThreadPool(2), false, false, false);
		SpaceServiceImpl allocService = new SpaceServiceImpl((LookupSpace) lookupSpace.proxy(),new ORMapperFactory(), ResourceSpace.NAME + "_ALLOC", Executors.newScheduledThreadPool(2), false, false, false);
		SpaceServiceImpl allResourcesEver = new SpaceServiceImpl((LookupSpace) lookupSpace.proxy(),new ORMapperFactory(), ResourceSpace.NAME + "_ALL", Executors.newScheduledThreadPool(2), false, false, true);

		resourceSpace = new ResourceSpaceImpl(resService, allocService, allResourcesEver);
		resourceSpace.start();
		
		System.out.println("=========================== " + getName() + " =======================================");
	}
	protected void tearDown() throws Exception {
		Thread.sleep(100);
		resourceSpace.stop();
		Thread.sleep(100);
	}
	

	public void testBGOwnerGetsARotatesTheList() throws Exception {
		int resources = 10;
		createProfiles(1000, resources);
		
		AllocListener owner = new MyAllocOwner();
		resourceSpace.registerAllocListener(owner, "me", "me");
		
		Thread.sleep(1000);
		
		for (int i = 0; i < resources+15; i++) {
			if (i == 9) { 
				System.err.println("wait");
			}
			int requestBGResources = resourceSpace.requestBGResources("1", 1, 1, "mflops > 0", "do stuff", 100, "me", "doStuff");
			assertEquals(1, requestBGResources);
			
		}
		assertEquals(resources+15, given.size());
	}
	
	public void testBGOwnerGetsAdifferentResourceEachTime() throws Exception {
		int resources = 10;
		createProfiles(1000, resources);
		
		AllocListener owner = new MyAllocOwner();
		resourceSpace.registerAllocListener(owner, "me", "me");
		
		Thread.sleep(1000);
		
		for (int i = 0; i < resources; i++) {
			int requestBGResources = resourceSpace.requestBGResources("1", 1, 1, "mflops > 0", "do stuff", 100, "me", "doStuff");
			assertEquals(1, requestBGResources);
			
		}
		assertEquals(resources, given.size());
	}

	
	public void testBGOwnerGetsAResource() throws Exception {
		createProfiles(1000, 10);
		
		AllocListener owner = new MyAllocOwner();
		resourceSpace.registerAllocListener(owner, "me", "me");
		
		Thread.sleep(1000);
		int requestBGResources = resourceSpace.requestBGResources("1", 1, 1, "mflops > 0", "do stuff", 100, "me", "doStuff");
		assertEquals(requestBGResources, given.size());
		assertEquals(1, given.size());
	}
	
	
	
	List<String> given = new ArrayList<String>();
	private ScheduledExecutorService scheduler;
	
	class MyAllocOwner implements AllocListener {

		public void add(String requestId, List<String> resourceIds, String owner, int priority) {
			given.addAll(resourceIds);
		}

		public void pending(String requestId, List<String> resourceIds, String owner, int priority) {
		}

		public List<String> release(String requestId, List<String> resourceIds, int releaseCount) {
			return null;
		}

		public void satisfied(String requestId, String owner, List<String> resourceIds) {
		}

		public void take(String requestId, String owner, List<String> resourceIds) {
		}
		public String getId() {
			return "id";
		}
	}
	
	private void createProfiles(int port, int count) throws Exception {
		for(int i = 0; i<count;i++) {
			ResourceProfile profile = new ResourceProfile("url", i, scheduler);
			profile.setHostName("localhost");
			profile.setPort(port);
			profile.setDeployedBundles("myBundle");
			resourceSpace.registerResource(profile, 180);
		}
	}	
}
