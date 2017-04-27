package com.liquidlabs.vso.resource;

import static org.junit.Assert.*;

import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.transport.serialization.ObjectTranslator;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.agent.ResourceProfile;
import com.liquidlabs.vso.lookup.LookupSpace;
import org.jmock.Mock;
import org.jmock.MockObjectTestCase;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ResourceSpaceTest extends MockObjectTestCase {
	
	
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
		
		System.out.println("=========================== " + getName() + " =======================================");
	}
	protected void tearDown() throws Exception {
		Thread.sleep(200);
		resourceSpace.stop();
	}
	@Test
	public void testShouldExpandListOfItemsToResourceIds() throws Exception {
		saveResourceProfile("rrp1");
		saveResourceProfile("rrp2");
		saveResourceProfile("rrp3");
		saveResourceProfile("rrp4");
		
		ResourceGroup group = new ResourceGroup();
		group.setName("grp1");
		group.setResourceSelection("resourceId contains rp4");
		resourceSpace.registerResourceGroup(group);
		
		// substring version
		Set<String> list1 = resourceSpace.expandIntoResourceIds(new HashSet<String>(Arrays.asList("rp")));
		assertEquals(4, list1.size());
		
		// wildcard version
		Set<String> list2 = resourceSpace.expandIntoResourceIds(new HashSet<String>(Arrays.asList("*rp*")));
		assertEquals(4, list2.size());
		
		// resourceGroup version
		Set<String> list3 = resourceSpace.expandIntoResourceIds(new HashSet<String>(Arrays.asList("group:grp1")));
		assertEquals(1, list3.size());

	}
	
	public void testShould() throws Exception {
		saveResourceProfile("rrp1");
		saveResourceProfile("rrp2");
		saveResourceProfile("rrp3");
		saveResourceProfile("rrp4");
		
		Set<String> list = resourceSpace.expandGroupIntoHostnames("grp1");
		assertEquals(0, list.size());
		
	}
	private void saveResourceProfile(String string) throws Exception {
		ResourceProfile rp1 = new ResourceProfile();
		rp1.setResourceId(string);
		resourceSpace.registerResource(rp1, -1);
		
	}
	public void testShouldFindResourcesWithContainsAny() throws Exception {
		String[] types = new String[] { "Management", "Native", "Agent", "Failover","Tailer", "Server"};
		int id = 0;
		for (String type : types) {
			ResourceProfile rp0 = new ResourceProfile();
			rp0.setDeployedBundles("boot-1.0,dashboard-1.0,replicator-1.0,vs-admin-1.0,vs-log-1.0,vs-util-1.0,");
			rp0.setResourceId("rp" + id++);
			rp0.setType(type);
			resourceSpace.registerResource(rp0, -1);
		}
		String query = "type containsAny 'Management,Native,Agent,Failover,Tailer,Server'";
		List<String> foundIds = resourceSpace.findResourceIdsBy(query);
		assertEquals("FoundIds:" + foundIds, types.length, foundIds.size());
		List<ResourceProfile> ids = resourceSpace.findResourceProfilesBy("type containsAny 'Management,Native,Agent,Failover,Tailer,Server'");
		assertEquals("FoundIds:" + foundIds, types.length, ids.size());
		
		ObjectTranslator ot = new ObjectTranslator();
		for (ResourceProfile resourceProfile : ids) {
			assertTrue("should have matched:" + resourceProfile.type, ot.isMatch(query, resourceProfile));
		}
		
	}
	
	public void testShouldGetLostResources() throws Exception {

		ResourceProfile rp0 = new ResourceProfile();
		rp0.setDeployedBundles("boot-1.0,dashboard-1.0,replicator-1.0,vs-admin-1.0,vs-log-1.0,vs-util-1.0,");
		rp0.setResourceId("rp0");
		rp0.setMflops(300);
		resourceSpace.registerResource(rp0, -1);
		resourceSpace.unregisterResource("", rp0.getResourceId());
		
		Collection<String> lostResources = resourceSpace.getLostResources();
		
		assertEquals(1, lostResources.size());
	}
	
	public void testShouldReturnCorrectUsingANDResources() throws Exception {
		ResourceProfile rp0 = new ResourceProfile();
		rp0.setDeployedBundles("boot-1.0,dashboard-1.0,replicator-1.0,vs-admin-1.0,vs-log-1.0,vs-util-1.0,");
		rp0.setResourceId("rp0");
		rp0.setMflops(300);
		resourceSpace.registerResource(rp0, -1);
		
		ResourceProfile rp1 = new ResourceProfile();
		rp1.setDeployedBundles("boot-1.0,dashboard-1.0,replicator-1.0,vs-admin-1.0,vs-log-1.0,vs-util-1.0,coherence-1.0,ice-1.0");
		rp1.setResourceId("rp1");
		rp1.setMflops(300);
		resourceSpace.registerResource(rp1, -1);
		
		ResourceProfile rp2 = new ResourceProfile();
		rp2.setDeployedBundles("boot-1.0,dashboard-1.0,replicator-1.0,vs-admin-1.0,vs-log-1.0,vs-util-1.0");
		rp2.setResourceId("rp2");
		rp2.setMflops(300);
		resourceSpace.registerResource(rp2, -1);

		String resourceTemplate = "(group('Dedicated') OR mflops > 0)"; 
		String template = resourceTemplate + " AND deployedBundles contains '" + "ice-1.0" + "' AND customProperties notContains ServiceType=System";
		List<String> resourceIds = resourceSpace.findResourceIdsBy(template);
		assertEquals(1, resourceIds.size());
		assertEquals("rp1", resourceIds.get(0));
	}
	
	public void testAllocOwnerIsCalledBack() throws Exception {
		Mock allocOwner = mock(AllocListener.class);
		allocOwner.expects(once()).method("add").withAnyArguments();
		resourceSpace.registerAllocListener((AllocListener) allocOwner.proxy(), "listener", "owner");
		resourceSpace.assignResources("requestId", Arrays.asList("one"), "owner", 100, "doStuff", -1);
		// need to wait for event to fire
		Thread.sleep(200);
		allocOwner.verify();
	}
	
	public void testShouldFindResourceProfiles() throws Exception {
		createProfiles(80, 50);
		List<String> findResourceIdsBy = resourceSpace.findResourceIdsBy("mflops > 0 AND ownership equals DEDICATED");
		assertEquals(50, findResourceIdsBy.size());
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

    public void testShouldAssignAvailableResources() throws Exception {
		Set<String> pending = new HashSet<String>();
		
		int allocCount = 50;
        CountDownLatch countDownLatch = new CountDownLatch(allocCount);
        resourceSpace.registerAllocListener(new MyAddAllocListener(countDownLatch), "listener", "owner");

		createProfiles(80, allocCount);
		List<String> resourceIds = resourceSpace.findResourceIdsBy("mflops > 0 AND ownership equals DEDICATED");
		int result = resourceSpace.assignAvailableResources("requestId", resourceIds, pending, 10, allocCount, "owner", "doStuff", -1);
		
		assertEquals(allocCount, result);
		countDownLatch.await(20, TimeUnit.SECONDS);
        List<Allocation> allocs = resourceSpace.getAllocsFor("owner");
		
		assertEquals(allocCount, allocs.size());
		assertEquals("owner", allocs.get(0).owner);
		assertEquals(10, allocs.get(0).priority);
		assertEquals("doStuff", allocs.get(0).workIdIntent);
	}
	
	public void testShouldAssignOneAvailableResources() throws Exception {
		Set<String> pending = new HashSet<String>();
		
		int allocCount = 50;
		Mock allocOwner = mock(AllocListener.class);
		allocOwner.expects(once()).method("add").withAnyArguments();
		resourceSpace.registerAllocListener((AllocListener) allocOwner.proxy(), "listener", "owner");
		
		createProfiles(80, allocCount);
		List<String> resourceIds = resourceSpace.findResourceIdsBy("mflops > 0 AND ownership equals DEDICATED");
		int result = resourceSpace.assignAvailableResources("requestId", resourceIds, pending, 10, 1, "owner", "doStuff", -1);
		
		Thread.sleep(1000);
		assertEquals(1, result);
		List<Allocation> allocs = resourceSpace.getAllocsFor("owner");
		
		assertEquals(1, allocs.size());
		assertEquals("owner", allocs.get(0).owner);
		assertEquals(10, allocs.get(0).priority);
		assertEquals("doStuff", allocs.get(0).workIdIntent);
	}
    
	public void testShouldAssignNotAssignTheSameOneRepeatedly() throws Exception {
		Set<String> pending = new HashSet<String>();
		
		int allocCount = 50;
        CountDownLatch countDownLatch = new CountDownLatch(3);
        MyAddAllocListener listenerAdd = new MyAddAllocListener(countDownLatch);
		resourceSpace.registerAllocListener(listenerAdd, "listenerAdd", "owner");
		
		createProfiles(80, allocCount);
		List<String> resourceIds = resourceSpace.findResourceIdsBy("mflops > 0 AND ownership equals DEDICATED");
		resourceSpace.assignAvailableResources("requestId", resourceIds, pending, 10, 1, "owner", "doStuff", -1);
		resourceSpace.assignAvailableResources("requestId", resourceIds, pending, 10, 1, "owner", "doStuff", -1);
		
		assertEquals(1, resourceSpace.assignAvailableResources("requestId", resourceIds, pending, 10, 1, "owner", "doStuff", -1));
		List<Allocation> allocs = resourceSpace.getAllocsFor("owner");
		
		assertEquals(3, allocs.size());
		assertEquals("owner", allocs.get(0).owner);
		assertTrue(countDownLatch.await(20, TimeUnit.SECONDS));
	}
}
