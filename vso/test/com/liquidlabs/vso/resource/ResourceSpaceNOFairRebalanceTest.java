package com.liquidlabs.vso.resource;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import com.liquidlabs.common.Pair;
import org.jmock.Mock;
import org.jmock.MockObjectTestCase;

import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.agent.ResourceProfile;
import com.liquidlabs.vso.lookup.LookupSpace;

public class ResourceSpaceNOFairRebalanceTest extends MockObjectTestCase {
	
	
	private Mock lookupSpace;
	private ResourceSpaceImpl resourceSpace;
	Set<String> pending = new HashSet<String>();
	private String location;
	private ScheduledExecutorService scheduler;

	protected void setUp() throws Exception {
		lookupSpace = mock(LookupSpace.class);
		lookupSpace.stubs();
		lookupSpace.stubs().method("unregisterService").will(returnValue(true));
		SpaceServiceImpl resService = new SpaceServiceImpl((LookupSpace) lookupSpace.proxy(),new ORMapperFactory(), ResourceSpace.NAME + "_RES", Executors.newScheduledThreadPool(2), false, false, false);
		SpaceServiceImpl allocService = new SpaceServiceImpl((LookupSpace) lookupSpace.proxy(),new ORMapperFactory(), ResourceSpace.NAME + "_ALLOC", Executors.newScheduledThreadPool(2), false, false, false);
		SpaceServiceImpl allResourcesEver = new SpaceServiceImpl((LookupSpace) lookupSpace.proxy(),new ORMapperFactory(), ResourceSpace.NAME + "_ALL", Executors.newScheduledThreadPool(2), false, false, true);

		VSOProperties.setResourceFairShareOff(true);
		resourceSpace = new ResourceSpaceImpl(resService, allocService, allResourcesEver);
		resourceSpace.start();
		
		System.out.println("=========================== " + getName() + " =======================================");
	}
	protected void tearDown() throws Exception {
		resourceSpace.stop();
	}
	
	
	public void testShouldRebalanceTwoPeers() throws Exception {
		
		// SETUP
		int allocCount = 100;
		int countNeeded = 70;
		
		ReleasingAllocOwner allocOwnerONE = new ReleasingAllocOwner(resourceSpace, "ownerONE");
		resourceSpace.registerAllocListener(allocOwnerONE, "listener1", "ownerONE");
		
		ReleasingAllocOwner allocOwnerTWO = new ReleasingAllocOwner(resourceSpace, "ownerTWO");
		resourceSpace.registerAllocListener(allocOwnerTWO, "listener2", "ownerTWO");
		
		
		CountingAllocOwner allocOwnerTHREE = new CountingAllocOwner();
		resourceSpace.registerAllocListener(allocOwnerTHREE, "listener3", "ownerTHREE");
		
		createProfiles(80, allocCount);
		
		List<String> resourceIds = resourceSpace.findResourceIdsBy("mflops > 0 AND ownership equals DEDICATED");
		
		// Give All Resources to this owner
		resourceSpace.assignAvailableResources("requestId", resourceIds, pending, 100, 50, "ownerONE", "doStuff", -1);
		resourceSpace.assignAvailableResources("requestId", resourceIds, pending, 100, 50, "ownerTWO", "doStuff", -1);
		
		// TEST
		int allocd = resourceSpace.assignEqualPriorityResources("requestId", resourceIds, pending, 100, countNeeded, "ownerTHREE", "doStuff2", -1);
		Thread.sleep(5000);
		assertEquals(countNeeded, allocd);
		System.err.println("======================= count:" + allocOwnerTHREE.count.intValue());
		assertEquals(countNeeded, allocOwnerTHREE.count.intValue());
		
		// VERIFY
		List<Allocation> allocs = resourceSpace.getAllocsFor("ownerTHREE");
		
		// pooo
		if (allocs.size() > allocCount) {
			for (Allocation allocation : allocs) {
				System.err.println("Alloc:" + allocation.id);
			}
		}
		assertEquals("Did not have " + countNeeded + " count", countNeeded, allocs.size());
		for (Allocation allocation : allocs) {
			assertTrue("Allocation has wrong type", allocation.type.equals(AllocType.ALLOCATED));
		}
	}
//	public void testShouldRebalanceSinglePeerWithPreExistingAllocWhenItHasLoads() throws Exception {
//		
//		// SETUP
//		int allocCount = 250;
//		int countNeeded = 50;
//		
//		ReleasingAllocOwner allocOwnerONE = new ReleasingAllocOwner(resourceSpace, "owner");
//		resourceSpace.registerAllocListener(allocOwnerONE, "listener1", "owner");
//		
//		
//		CountingAllocOwner allocOwnerTWO = new CountingAllocOwner();
//		resourceSpace.registerAllocListener(allocOwnerTWO, "listener2", "ownerTWO");
//		
//		createProfiles(80, allocCount);
//		
//		List<String> resourceIds = resourceSpace.findResourceIdsBy("mflops > 0 AND ownership equals DEDICATED");
//		
//		// TEST 
//		
//		// Give 50 to One
//		resourceSpace.assignAvailableResources("requestId", resourceIds, pending, 100, 50, "owner", "doStuff", -1);
//		// Setup prexisting alloc to  200
//		resourceSpace.assignAvailableResources("requestId", resourceIds, pending, 100, 200, "ownerTWO", "doStuff", -1);
//		
//		// TEST
//		// Take the 50 from One
//		int allocd = resourceSpace.assignEqualPriorityResources("requestId", resourceIds, pending, 100, countNeeded, "ownerTWO", "doStuff2", -1);
//		Thread.sleep(5000);
//		assertEquals(countNeeded, allocd);
//		System.err.println("======================= count:" + allocOwnerTWO.count.intValue());
//		assertEquals(250, allocOwnerTWO.count.intValue());
//		
//		// VERIFY
//		List<Allocation> allocs = resourceSpace.getAllocsFor("ownerTWO");
//		
//		// pooo
//		if (allocs.size() > allocCount) {
//			for (Allocation allocation : allocs) {
//				System.err.println("Alloc:" + allocation.id);
//			}
//		}
//	}
	public void testShouldRebalanceSinglePeerWithPreExistingAlloc() throws Exception {
		
		// SETUP
		int allocCount = 37;
		int countNeeded = 5;
		
		ReleasingAllocOwner allocOwnerONE = new ReleasingAllocOwner(resourceSpace, "owner");
		resourceSpace.registerAllocListener(allocOwnerONE, "listener1", "owner");
		
		
		CountingAllocOwner allocOwnerTWO = new CountingAllocOwner();
		resourceSpace.registerAllocListener(allocOwnerTWO, "listener2", "ownerTWO");
		
		createProfiles(80, allocCount);
		
		List<String> resourceIds = resourceSpace.findResourceIdsBy("mflops > 0 AND ownership equals DEDICATED");
		
		// TEST 
		
		// Give 26 to One
		resourceSpace.assignAvailableResources("requestId", resourceIds, pending, 100, 26, "owner", "doStuff", -1);
		// Setup prexisting alloc to  11
		resourceSpace.assignAvailableResources("requestId", resourceIds, pending, 100, 11, "ownerTWO", "doStuff", -1);
		
		// TEST
		int allocd = resourceSpace.assignEqualPriorityResources("requestId", resourceIds, pending, 100, countNeeded, "ownerTWO", "doStuff2", -1);
		Thread.sleep(5000);
		assertEquals(countNeeded, allocd);
		System.err.println("======================= count:" + allocOwnerTWO.count.intValue());
		assertEquals(countNeeded + 11, allocOwnerTWO.count.intValue());
		
		// VERIFY
		List<Allocation> allocs = resourceSpace.getAllocsFor("ownerTWO");
		
		// pooo
		if (allocs.size() > allocCount) {
			for (Allocation allocation : allocs) {
				System.err.println("Alloc:" + allocation.id);
			}
		}
	}
	
	public void testShouldRebalanceSinglePeer() throws Exception {
		
		// SETUP
		int allocCount = 36;
		int countNeeded = 36;
		
		ReleasingAllocOwner allocOwnerONE = new ReleasingAllocOwner(resourceSpace, "owner");
		resourceSpace.registerAllocListener(allocOwnerONE, "listener1", "owner");
		
		
		CountingAllocOwner allocOwnerTWO = new CountingAllocOwner();
		resourceSpace.registerAllocListener(allocOwnerTWO, "listener2", "ownerTWO");
		
		createProfiles(80, allocCount);
		
		List<String> resourceIds = resourceSpace.findResourceIdsBy("mflops > 0 AND ownership equals DEDICATED");
		
		// Give All Resources to this owner
		resourceSpace.assignAvailableResources("requestId", resourceIds, pending, 100, allocCount, "owner", "doStuff", -1);
		
		// TEST
		int allocd = resourceSpace.assignEqualPriorityResources("requestId", resourceIds, pending, 100, countNeeded, "ownerTWO", "doStuff2", -1);
		Thread.sleep(5000);
		assertEquals(countNeeded, allocd);
		System.err.println("======================= count:" + allocOwnerTWO.count.intValue());
		assertEquals(countNeeded, allocOwnerTWO.count.intValue());
		
		// VERIFY
		List<Allocation> allocs = resourceSpace.getAllocsFor("ownerTWO");
		
		// pooo
		if (allocs.size() > allocCount) {
			for (Allocation allocation : allocs) {
				System.err.println("Alloc:" + allocation.id);
			}
		}
		assertEquals("Did not have " + countNeeded + " count", countNeeded, allocs.size());
		for (Allocation allocation : allocs) {
			assertTrue("Allocation has wrong type", allocation.type.equals(AllocType.ALLOCATED));
		}
	}
	
	
	
	public static class CountingAllocOwner implements AllocListener {
		public AtomicInteger count = new AtomicInteger(0);
		public HashSet<String> resourceIds = new HashSet<String>();

		public void add(String requestId, List<String> resourceIds, String owner, int priority) {
			count.incrementAndGet();
//			System.err.println(count + " got:" + resourceIds);
			this.resourceIds.addAll(resourceIds);
		}

		public List<String> release(String requestId, List<String> resourceIds, int releaseCount) {
			return resourceIds.subList(0, releaseCount);
		}

		public void take(String requestId, String owner, List<String> resourceIds) {
		}

		public void pending(String requestId, List<String> asList, String owner, int priority) {
		}

		public void satisfied(String requestId, String owner, List<String> asList) {
		}

		public String getId() {
			return "id";
		}
	}
	public static class ReleasingAllocOwner implements AllocListener {
		final ResourceSpace rs;
		ArrayBlockingQueue<String> releaseQueue = new ArrayBlockingQueue<String>(10000);
		private Thread releaseThread;
		private final String id;
		public long sleepThreadhold = 50;

		public ReleasingAllocOwner(final ResourceSpace rs, final String id) {
			this.rs = rs;
			this.id = id;
			releaseThread = new Thread(){
				public void run() {
					try {
						// do a crappy stall so the test will work - where it has a chance to 
						// setup pending allocations before we release them.
						Thread.sleep(1000);
						while (true) {
							String take = releaseQueue.take();
							Thread.sleep(sleepThreadhold);
//							System.out.println(id +" >>>>>>>Releasing:" + take);
							rs.releasedResources(Arrays.asList(take));
						}
					} catch (InterruptedException e) {
					}
				}
			};
			releaseThread.setDaemon(true);
			releaseThread.start();
		}

		public void add(String requestId, List<String> resourceIds, String owner, int priority) {
		}
		public void pending(String requestId, List<String> asList, String owner, int priority) {
		}
		public void satisfied(String requestId, String owner, List<String> asList) {
		}

		// Must return list before releasing anything
		public List<String> release(String requestId, List<String> resourceIds, int releaseCount) {
			ArrayList<String> result = new ArrayList<String>();
			int done = 0;
			for (String string : resourceIds) {
				if (done++ < releaseCount) {
//					System.out.println(id + " - Going to release:" + string);
					releaseQueue.add(string);
					result.add(string);
				}
			}
			return result;
		}

		public void take(String requestId, String owner, List<String> resourceIds) {
		}

		public String getId() {
			return id;
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
