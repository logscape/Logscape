package com.liquidlabs.vso.deployment.bundle;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.junit.Assert;
import org.junit.Test;

import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.vso.SpaceService;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.agent.ResourceProfile;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.ServiceInfo;
import com.liquidlabs.vso.work.WorkAllocator;
import com.liquidlabs.vso.work.WorkAssignment;

public class BackgroundServiceAllocatorTest {
	
	
	class MyServiceFinder implements ServiceFinder {

		public boolean isServiceRunning(String serviceId) {
			return true;
		}

		public boolean isWaitingForDependencies(String serviceName, String bundleName) {
			return false;
		}

	}

	private LookupSpace lookupSpace;
	private ORMapperFactory ormFactory;

    @Test
    public void shouldDoSquat() {
        //
    }
//	TODO @Test DodgyTest?
	public void testShouldOnlyAllocateWorkToAResourceOnce() throws Exception {
		Mockery context = new Mockery();
		final WorkAllocator workAllocator = context.mock(WorkAllocator.class);
		lookupSpace = context.mock(LookupSpace.class);
		ormFactory = new ORMapperFactory();
		
		context.checking(new Expectations() {
			{
				atLeast(1).of(lookupSpace).registerService(with(any(ServiceInfo.class)), with(any(long.class))); will(returnValue("lease"));
				atLeast(1).of(lookupSpace).unregisterService(with(any(ServiceInfo.class))); will(returnValue(true));
				// want 10 allocation requests to be made
				exactly(10).of(workAllocator).assignWork(with(any(String.class)), with(any(String.class)),  with(any(WorkAssignment.class)));
			}
		});
		
		SpaceServiceImpl workToAlloc = new SpaceServiceImpl(lookupSpace, ormFactory, BundleSpace.NAME+"_PEND", ormFactory.getScheduler(), false, false, true);
		workToAlloc.start(this, "myBundle-1.0");
		
		
		WorkAssignment workAssignment = new WorkAssignment();
		workAssignment.setAllocationsOutstanding(10);
		workAssignment.setBackground(true);
		workAssignment.setResourceSelection("workId notContains BOO AND workId notContains FOO");
		workAssignment.setBundleId("myBundle-0.01");
		workAssignment.setServiceName("FOO");
		workAssignment.setResourceId("resource");
		workToAlloc.store(workAssignment, -1);
		
		BackgroundServiceAllocator allocator = new BackgroundServiceAllocator((SpaceService) workToAlloc, new HashMap<String, String>(), workAllocator, null, new MyServiceFinder(), Executors.newScheduledThreadPool(5));
		
		// create 10 different resource profiles
		List<ResourceProfile> profiles = createResourceProfiles(10);
		
		System.out.println("Registering -------------------------------");
		// register them so they get assigned to
		for(int i = 0; i<9; i++) {
			ResourceProfile resourceProfile = profiles.get(i);
			allocator.register(resourceProfile.getResourceId(), profiles.get(i));
			resourceProfile.addWorkAssignmentId(workAssignment.getId());
			Thread.sleep(10);
			
		}
		System.out.println("Reregistering -------------------------------");
		// register them again to check that the 
		for(int i = 0; i<9; i++) {
			allocator.register(profiles.get(i).getResourceId(), profiles.get(i));
		}
		WorkAssignment stateWork = workToAlloc.findById(WorkAssignment.class, workAssignment.getId());
		Assert.assertEquals(1, stateWork.getAllocationsOutstanding());
		allocator.register(profiles.get(9).getResourceId(), profiles.get(9));
		WorkAssignment stateWork2 = workToAlloc.findById(WorkAssignment.class, workAssignment.getId());
		Assert.assertEquals(0, stateWork2.getAllocationsOutstanding());
	}
	
	private List<ResourceProfile> createResourceProfiles(int count) {
		List<ResourceProfile> list = new ArrayList<ResourceProfile>(count);
		for (int i = 0; i<count; i++) {
			ResourceProfile resourceProfile = new ResourceProfile();
			resourceProfile.setDeployedBundles("myBundle-0.01");
			resourceProfile.setResourceId(String.valueOf(i));
			list.add(resourceProfile);
		}
		return list;
	}
 
}
