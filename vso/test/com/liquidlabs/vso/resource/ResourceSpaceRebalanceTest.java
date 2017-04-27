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
import com.liquidlabs.vso.agent.ResourceProfile;
import com.liquidlabs.vso.lookup.LookupSpace;

public class ResourceSpaceRebalanceTest extends MockObjectTestCase {
	
	
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

		resourceSpace = new ResourceSpaceImpl(resService, allocService, allResourcesEver);
		resourceSpace.start();

		System.out.println("=========================== " + getName() + " =======================================");
	}
	protected void tearDown() throws Exception {
		resourceSpace.stop();
	}
	
	
	public void testShouldRebalanceTWOSlotsEvenly() throws Exception {
		
		// SETUP
		int allocCount = 2;
		
		ReleasingAllocOwner allocOwnerONE = new ReleasingAllocOwner(resourceSpace, "ownerONE");
		resourceSpace.registerAllocListener(allocOwnerONE, "listener1", "ownerONE");
		
		
		ReleasingAllocOwner allocOwnerTWO = new ReleasingAllocOwner(resourceSpace, "ownerTWO");
		resourceSpace.registerAllocListener(allocOwnerTWO, "listener2", "ownerTWO");
		
		createProfiles(80, allocCount);
		
		String template = "mflops > 0 AND ownership equals DEDICATED";
		List<String> resourceIds = resourceSpace.findResourceIdsBy(template);
		resourceSpace.assignAvailableResources("requestId", resourceIds, pending, 100, 1, "ownerONE", "doStuff", -1);
		resourceSpace.assignAvailableResources("requestId", resourceIds, pending, 100, 1, "ownerTWO", "doStuff", -1);
		
		resourceSpace.requestResources("TEST", 5, 100, template, "WORK?", 1, "ownerTWO", "ownerTWO");
		
		// TEST
		Thread.sleep(1000);
		assertEquals(0, allocOwnerONE.release);
		assertEquals(0, allocOwnerONE.release);
		
		// VERIFY
		assertEquals(1, resourceSpace.getAllocsFor("ownerONE").size());
		assertEquals(1, resourceSpace.getAllocsFor("ownerTWO").size());
		
	}
	public void testShouldRebalanceLotsWithMultiplePeers() throws Exception {
		
		// SETUP
		int peerCount = 10;
		int allocCount = peerCount * 20;
		int countNeeded = allocCount/peerCount;
		
		createProfiles(80, allocCount);
		
		List<ReleasingAllocOwner> peers = new ArrayList<ReleasingAllocOwner>();
		List<String> resourceIds = resourceSpace.findResourceIdsBy("mflops > 0 AND ownership equals DEDICATED");
		System.out.println("Total ResourceIds:" + resourceIds.size());
		assertEquals("Didnt get expected ResourceCount", allocCount, resourceIds.size());
		
		for (int i = 0; i < peerCount; i++) {
			ReleasingAllocOwner allocOwnerONE = new ReleasingAllocOwner(resourceSpace, "owner" + i);
			resourceSpace.registerAllocListener(allocOwnerONE, "listener"+i, "owner"+i);
			peers.add(allocOwnerONE);
			
			int assigned = resourceSpace.assignAvailableResources("requestId", resourceIds, pending, 100, countNeeded, "owner"+i, "doStuff", -1);
			System.out.println("  ------------- Assigned:" + assigned + "  ==> " + "owner:" + i + " requested:" + countNeeded);
		}
		
		CountingAllocOwner allocOwnerTWO = new CountingAllocOwner();
		resourceSpace.registerAllocListener(allocOwnerTWO, "listenerNEW", "ownerNEW");
		
		// TEST
		int allocd = resourceSpace.assignEqualPriorityResources("requestId", resourceIds, pending, 100, countNeeded, "ownerNEW", "doStuff2", -1);
		
		int correctValue = 18; // (200/11 == 18 or 19 - want to reclaim least amount to prevent repeated shuffling)
		
		assertEquals("Peers:" + peerCount + " allocs:" + allocCount + " countPerPeer:" + countNeeded, correctValue, allocd);
		Thread.sleep(5000);
		System.err.println(allocd + " ======================= count:" + allocOwnerTWO.count.intValue());
		assertEquals(correctValue, allocOwnerTWO.count.intValue());
		
		// VERIFY
		List<Allocation> allocs = resourceSpace.getAllocsFor("ownerNEW");
		
		// pooo
		if (allocs.size() > allocCount) {
			for (Allocation allocation : allocs) {
				System.err.println("Alloc:" + allocation.id);
			}
		}
		assertEquals("Did not have " + correctValue + " count", correctValue, allocs.size());
		for (Allocation allocation : allocs) {
			assertTrue("Allocation has wrong type", allocation.type.equals(AllocType.ALLOCATED));
		}
	}
    //DodgyTest?
	public void xxxtestShouldNotGrabAllocWhenThereIsPending() throws Exception {
		
		ReleasingAllocOwner allocOwnerONE = new ReleasingAllocOwner(resourceSpace, "ownerONE");
		// 1) Make RELEASE PAUSE
		allocOwnerONE.sleepThreadhold = 100;
		
		resourceSpace.registerAllocListener(allocOwnerONE, "listener1", "ownerONE");
		CountingAllocOwner allocOwnerTWO = new CountingAllocOwner();
		resourceSpace.registerAllocListener(allocOwnerTWO, "listener2", "ownerTWO");
		createProfiles(80, 4);
		List<String> resourceIds = resourceSpace.findResourceIdsBy("mflops > 0");
		
		resourceSpace.assignAvailableResources("requestId", resourceIds, pending, 100, 4, "ownerONE", "doStuff", -1);
		
		// should get first item
		resourceSpace.assignEqualPriorityResources("R1", resourceIds, pending, 100, 1, "ownerTWO", "doStuff", -1);
		
		// 2) Skip though another Request BEFORE it is RELEASED - so we hit a PENDING ALLOCATION for the previous request 
		Thread.sleep(20);
		// should get different second item
		resourceSpace.assignEqualPriorityResources("R1", resourceIds, pending, 100, 1, "ownerTWO", "doStuff", -1);
		Thread.sleep(100);
		
		// 3. wait for the allocs to be fullfilled
		Thread.sleep(2000);
		// verify we have 2 different resources
		assertEquals("ResourcesWas:" + allocOwnerTWO.resourceIds, 2, allocOwnerTWO.resourceIds.size());
	}
	
	public void testShouldGetWhateverTheCountValueIsAndNotMore() throws Exception {
		// SETUP
		int allocCount = 100;
		int countNeeded = 50;
		
		ReleasingAllocOwner allocOwnerONE = new ReleasingAllocOwner(resourceSpace, "owner");
		resourceSpace.registerAllocListener(allocOwnerONE, "listener1", "owner");
		
		
		CountingAllocOwner allocOwnerTWO = new CountingAllocOwner();
		resourceSpace.registerAllocListener(allocOwnerTWO, "listener2", "ownerTWO");
		
		createProfiles(80, allocCount);
		
		List<String> resourceIds = resourceSpace.findResourceIdsBy("mflops > 0 AND ownership equals DEDICATED");
		resourceSpace.assignAvailableResources("requestId", resourceIds, pending, 100, 55, "owner", "doStuff", -1);
		resourceSpace.assignAvailableResources("requestId", resourceIds, pending, 100, 35, "ownerTWO", "doStuff", -1);

		// TEST - we only want 1
		int allocd = resourceSpace.assignEqualPriorityResources("requestId", resourceIds, pending, 100, 1, "ownerTWO", "doStuff2", -1);
		Thread.sleep(5000);
		assertEquals(1, allocd);
	}
	
	public void testShouldRebalanceSinglePeer() throws Exception {
		
		// SETUP
		int allocCount = 100;
		int countNeeded = 50;
		
		ReleasingAllocOwner allocOwnerONE = new ReleasingAllocOwner(resourceSpace, "owner");
		resourceSpace.registerAllocListener(allocOwnerONE, "listener1", "owner");
		
		
		CountingAllocOwner allocOwnerTWO = new CountingAllocOwner();
		resourceSpace.registerAllocListener(allocOwnerTWO, "listener2", "ownerTWO");
		
		createProfiles(80, allocCount);
		
		List<String> resourceIds = resourceSpace.findResourceIdsBy("mflops > 0 AND ownership equals DEDICATED");
		resourceSpace.assignAvailableResources("requestId", resourceIds, pending, 100, allocCount, "owner", "doStuff", -1);
		
		// TEST
		int allocd = resourceSpace.assignEqualPriorityResources("requestId", resourceIds, pending, 100, countNeeded, "ownerTWO", "doStuff2", -1);
		Thread.sleep(10000);
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
	
	public void testShouldRebalanceSinglePeerAndNoWobble() throws Exception {
		
		// SETUP
		int allocCount = 25;
		int countNeeded = 20;
		Set<String> pending = new HashSet<String>();
		
		ReleasingAllocOwner allocOwnerONE = new ReleasingAllocOwner(resourceSpace, "owner");
		resourceSpace.registerAllocListener(allocOwnerONE, "listener1", "owner");
		
		
		CountingAllocOwner allocOwnerTWO = new CountingAllocOwner();
		resourceSpace.registerAllocListener(allocOwnerTWO, "listener2", "ownerTWO");
		
		createProfiles(80, allocCount);
		
		List<String> resourceIds = resourceSpace.findResourceIdsBy("mflops > 0 AND ownership equals DEDICATED");
		
		// give "owner" all resources
		System.out.println("================ 1");
		int allocd0 = resourceSpace.assignAvailableResources("requestId", resourceIds, pending, 100, allocCount, "owner", "doStuff", -1);
		Thread.sleep(1000);
		assertEquals(allocCount, allocd0);
		
		// TEST - 1
		System.out.println("================ 2");
		int allocd1 = resourceSpace.assignEqualPriorityResources("requestId", resourceIds, pending, 100, countNeeded, "ownerTWO", "doStuff2", -1);
		Thread.sleep(1000);
		assertEquals(12, allocd1);
		
		// Allocs should now be "0"
		System.out.println("================ 3");
		int allocd2 = resourceSpace.assignEqualPriorityResources("requestId", resourceIds, pending, 100, countNeeded, "ownerTWO", "doStuff2", -1);
		assertEquals("Should have remained at 0 - but didnt", 0, allocd2);
		
		// Allocs should remain at "0"
		System.out.println("================ 4");
		int allocd3 = resourceSpace.assignEqualPriorityResources("requestId", resourceIds, pending, 100, countNeeded, "ownerTWO", "doStuff2", -1);
		assertEquals("Should have remained at 0 - but didnt", 0, allocd3);
		
	}
	
	public void testShouldRebalanceMultiplePeers() throws Exception {
		
		// SETUP
		int allocCount = 90;
		int peerCount = 2;
		int countNeeded = allocCount/(peerCount+1);
		
		createProfiles(80, allocCount);
		
		List<ReleasingAllocOwner> peers = new ArrayList<ReleasingAllocOwner>();
		List<String> resourceIds = resourceSpace.findResourceIdsBy("mflops > 0 AND ownership equals DEDICATED");
		
		for (int i = 0; i < peerCount; i++) {
			ReleasingAllocOwner allocOwnerONE = new ReleasingAllocOwner(resourceSpace, "owner" + i);
			resourceSpace.registerAllocListener(allocOwnerONE, "listener"+i, "owner"+i);
			peers.add(allocOwnerONE);
			
			resourceSpace.assignAvailableResources("requestId", resourceIds, pending, 100, allocCount/peerCount, "owner"+i, "doStuff", -1);
		}
		
		CountingAllocOwner allocOwnerTWO = new CountingAllocOwner();
		resourceSpace.registerAllocListener(allocOwnerTWO, "listenerNEW", "ownerNEW");
		
		// TEST
		int remaining = resourceSpace.assignEqualPriorityResources("requestId", resourceIds, pending, 100, countNeeded, "ownerNEW", "doStuff2", -1);
		
		Thread.sleep(5000);
	//	assertEquals(0, remaining);
		System.err.println("======================= count:" + allocOwnerTWO.count.intValue());
		assertEquals(countNeeded, allocOwnerTWO.count.intValue());
		
		// VERIFY
		List<Allocation> allocs = resourceSpace.getAllocsFor("ownerNEW");
		
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
		public volatile int take = 0;
		public HashSet<String> resourceIds = new HashSet<String>();

		public void add(String requestId, List<String> resourceIds, String owner, int priority) {
			count.incrementAndGet();
			System.err.println(count + " got:" + resourceIds);
			this.resourceIds.addAll(resourceIds);
		}

		public List<String> release(String requestId, List<String> resourceIds, int releaseCount) {
			return resourceIds.subList(0, releaseCount);
		}

		public void take(String requestId, String owner, List<String> resourceIds) {
			take++;
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
		volatile int take = 0;
		volatile int release = 0;
		

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
							System.out.println(id +" >>>>>>>Releasing:" + take);
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

		// Must return list before releasing anything
		public List<String> release(String requestId, List<String> resourceIds, int releaseCount) {
			release += releaseCount;
			ArrayList<String> result = new ArrayList<String>();
			int done = 0;
			for (String string : resourceIds) {
				if (done++ < releaseCount) {
					System.out.println(id + " - Going to release:" + string);
					releaseQueue.add(string);
					result.add(string);
				}
			}
			return result;
		}

		public void take(String requestId, String owner, List<String> resourceIds) {
			take++;
		}

		public void pending(String requestId, List<String> resourceIds, String owner, int priority) {
		}

		public void satisfied(String requestId, String owner, List<String> resourceIds) {
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
