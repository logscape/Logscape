package com.liquidlabs.vso.resource;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.jmock.Mock;
import org.jmock.MockObjectTestCase;

import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.agent.ResourceProfile;
import com.liquidlabs.vso.lookup.LookupSpace;

public class ResourceSpaceReclaimTest extends MockObjectTestCase {
	
	
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
		resourceSpace.stop();
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
	
	public void testShouldAssignLowerPriorityResources() throws Exception {
		
		Set<String> pending = new HashSet<String>();
		// SETUP
		int allocCount = 100;
		ReleasingAllocOwner allocOwnerONE = new ReleasingAllocOwner(resourceSpace);
		resourceSpace.registerAllocListener(allocOwnerONE, "listener1", "owner");
		
		CountingAllocOwner allocOwnerTWO = new CountingAllocOwner();
		resourceSpace.registerAllocListener(allocOwnerTWO, "listener2", "ownerTWO");
		
		createProfiles(80, allocCount);
		List<String> resourceIds = resourceSpace.findResourceIdsBy("mflops > 0 AND ownership equals DEDICATED");
		resourceSpace.assignAvailableResources("requestId", resourceIds, pending, 10, allocCount, "owner", "doStuff", -1);
		
		// TEST
		int allocd = resourceSpace.assignLowerPriorityResources("requestId", resourceIds, pending, 100, allocCount, "ownerTWO", "doStuff2", -1);
		Thread.sleep(5000);
		assertEquals(100, allocd);
		System.err.println("======================= count:" + allocOwnerTWO.count.intValue());
		assertEquals(allocCount, allocOwnerTWO.count.intValue());
		
		// VERIFY
		List<Allocation> allocs = resourceSpace.getAllocsFor("ownerTWO");
		
		// pooo
		if (allocs.size() > allocCount) {
			for (Allocation allocation : allocs) {
				System.err.println("Alloc:" + allocation.id);
			}
		}
		assertEquals("Did not have " + allocCount + " count", allocCount, allocs.size());
		for (Allocation allocation : allocs) {
			assertTrue("Allocation has wrong type", allocation.type.equals(AllocType.ALLOCATED));
		}
	}
	
	public static class CountingAllocOwner implements AllocListener {
		public AtomicInteger count = new AtomicInteger(0);

		public void add(String requestId, List<String> resourceIds, String owner, int priority) {
			count.incrementAndGet();
			System.err.println("got:" + resourceIds);
		}

		public List<String> release(String requestId, List<String> resourceIds, int releaseCount) {
			return null;
		}

		public void take(String requestId, String owner, List<String> resourceIds) {
		}

		public void pending(String requestId, List<String> resourceIds, String owner, int priority) {
		}

		public void satisfied(String requestId, String owner, List<String> resourceIds) {
		}

		public String getId() {
			return "od1";
		}
		
	}
	public static class ReleasingAllocOwner implements AllocListener {
		final ResourceSpace rs;
		ArrayBlockingQueue<String> releaseQueue = new ArrayBlockingQueue<String>(10000);
		private Thread releaseThread;

		public ReleasingAllocOwner(final ResourceSpace rs) {
			this.rs = rs;
			releaseThread = new Thread(){
				public void run() {
					try {
						// do a crappy stall so the test will work - where it has a chance to 
						// setup pending allocations before we release them.
						Thread.sleep(1000);
						while (true) {
							String take = releaseQueue.take();
							System.out.println(">>>>>>>Releasing:" + take);
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
			ArrayList<String> result = new ArrayList<String>();
			int done = 0;
			for (String string : resourceIds) {
				if (done++ < releaseCount) {
					System.out.println("Going to release:" + string);
					releaseQueue.add(string);
					result.add(string);
				}
			}
			return result;
		}

		public void take(String requestId, String owner, List<String> resourceIds) {
		}

		public void pending(String requestId, List<String> resourceIds, String owner, int priority) {
		}

		public void satisfied(String requestId, String owner, List<String> resourceIds) {
		}

		public String getId() {
			return "releaser1";
		}
		
	}

}
