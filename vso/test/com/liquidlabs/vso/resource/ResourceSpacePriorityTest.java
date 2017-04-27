package com.liquidlabs.vso.resource;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;

import org.jmock.Mock;
import org.jmock.MockObjectTestCase;

import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.agent.ResourceProfile;
import com.liquidlabs.vso.lookup.LookupSpace;

public class ResourceSpacePriorityTest extends MockObjectTestCase {
	
	
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
		Thread.sleep(200);
		resourceSpace.stop();
	}
	
	public void testShouldGetLostResources() throws Exception {

		ResourceProfile rp0 = new ResourceProfile();
		rp0.setDeployedBundles("boot-1.0,dashboard-1.0,replicator-1.0,vs-admin-1.0,vs-log-1.0,vs-util-1.0,");
		rp0.setResourceId("rp0");
		rp0.setMflops(300);
		resourceSpace.registerResource(rp0, -1);
		List<String> resourceIds = Arrays.asList(rp0.getResourceId());
		
		System.out.println("-------------------------------------------------");
		resourceSpace.assignResources("TEST_HI_P",resourceIds, "BOOTER", 1000, "booting stuff", -1);
		System.out.println("-------------------------------------------------");
		
		MyAllocListener allocListener = new MyAllocListener();
		resourceSpace.registerAllocListener(allocListener, "TestCase", "TestOwner");
		
		resourceSpace.requestResources("TEST_REQUEST", 1, 100, "", "test Stuff", 1, "TestCase","Testing stuff");
		
		Thread.sleep(2000);
		
		assertEquals(0, allocListener.ids.size());
	}
	

	public class MyAllocListener implements AllocListener {
		Set<String> ids = new HashSet<String>();

		public void add(String requestId, List<String> resourceIds, String owner, int priority) {
			ids.addAll(resourceIds);
			System.out.println("ADDING:::::::::::::::: " + resourceIds);
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
}
