package com.liquidlabs.vso.deployment.bundle;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.junit.Before;
import org.junit.Test;

import com.liquidlabs.common.net.URI;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.agent.ResourceProfile;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.ServiceInfo;
import com.liquidlabs.vso.work.WorkAllocator;
import com.liquidlabs.vso.work.WorkAssignment;
import com.liquidlabs.vso.work.WorkListener;

public class BGServiceAllocTest {
	
	Mockery context = new Mockery();
	private LookupSpace lookupSpace;
	private ORMapperFactory ormFactory;
	@Before
	public void before() throws Exception {
		lookupSpace = context.mock(LookupSpace.class);
		ormFactory = new ORMapperFactory();
		context.checking(new Expectations(){{
				atLeast(1).of(lookupSpace).registerService(with(any(ServiceInfo.class)), with(any(long.class))); will(returnValue("lease"));
				atLeast(1).of(lookupSpace).unregisterService(with(any(ServiceInfo.class))); will(returnValue(true));
				atLeast(1).of(lookupSpace).renewLease(with(any(String.class)), with(any(int.class)));
			}}
		);

        
        
	}

    @Test
    public void should() {
        
    }

    //	@Test DodgyTest? Not sure why she no work
	public void shouldReAllocBouncedAgent() throws Exception {
		
		
		SpaceServiceImpl workToAlloc = new SpaceServiceImpl(lookupSpace, ormFactory, BundleSpace.NAME+"_PEND", ormFactory.getScheduler(), false, false, true);
		workToAlloc.start(this, "boot-1.0");

        final CountDownLatch countDownLatch = new CountDownLatch(2);
        TestWA workAllocator = new TestWA(countDownLatch);
		
		BackgroundServiceAllocator allocator = new BackgroundServiceAllocator(workToAlloc, new HashMap<String,String>(), workAllocator, new URI("stcp://localhost"), new MyServiceFinder(), Executors.newScheduledThreadPool(5));
		WorkAssignment work = new WorkAssignment("agent","",0,"bundle-1.0", "serviceName", "script", 10);
		work.setBackground(true);
		work.setAllocationsOutstanding(1);
		work.setResourceId("agent");
		workToAlloc.store(work, -1);
		ResourceProfile profile = new ResourceProfile();
		profile.setResourceId("resourceId");
		profile.setDeployedBundles("bundle-1.0");
		
		allocator.register(profile.getResourceId(), profile);
		
		// TEST - does the alloc again
		allocator.register(profile.getResourceId(), profile);


		work.setResourceId(profile.getResourceId());
		profile.addWorkAssignmentId(work.getId());
		
		allocator.register(profile.getResourceId(), profile);

        countDownLatch.await(10, TimeUnit.SECONDS);
        assertTrue(workAllocator.stuff.contains(profile.getResourceId()));
        assertThat(workAllocator.stuff.size(), is(2));

	}
	
	private static class MyServiceFinder implements ServiceFinder {

		public boolean isServiceRunning(String serviceId) {
			return false;
		}

		public boolean isWaitingForDependencies(String serviceName,String bundleName) {
			return false;
		}
	}
	private static class TestWA implements WorkAllocator {
        private final CountDownLatch countDownLatch;
		List<String> stuff = new ArrayList();
        public TestWA(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }

        public void assignWork(String requestId, String resourceId,WorkAssignment workInfo) {
			stuff.add(resourceId);
			countDownLatch.countDown();
		}
	
		@Override
		public void assignWork(String requestId, String workingDirectory,List<String> resourceIds, int priority,WorkAssignment workAssignmentForService, State pending) {
        }
	
		@Override
		public String bounce(String serviceId) {
			return null;
		}
	
		@Override
		public URI getEndPoint() {
			return null;
		}
	
		@Override
		public List<WorkAssignment> getWorkAssignmentsForBundle(String bundleId) {
			return null;
		}
	
		@Override
		public List<WorkAssignment> getWorkAssignmentsForQuery(String query) {
			return null;
		}
	
		@Override
		public int getWorkIdCountForQuery(String query) {
			return 0;
		}
	
		@Override
		public List<Integer> getWorkIdCountForQuery(String... workIdQueries) {
			return null;
		}
	
		@Override
		public String[] getWorkIdsAssignedToResource(String id) {
			return null;
		}
	
		@Override
		public String[] getWorkIdsForQuery(String query) {
			return null;
		}
	
		@Override
		public void registerWorkListener(WorkListener workListener,String resourceId, String listenerId, boolean replayMissedEvents) throws Exception {
		}
	
		@Override
		public boolean renewLease(String workListenerLease, int expires)
				throws Exception {
			// TODO Auto-generated method stub
			return false;
		}
	
		@Override
		public boolean renewWorkLeases(String ownerId, int timeSeconds) {
			return false;
		}
	
		@Override
		public void saveWorkAssignment(WorkAssignment workInfo, String resourceId) {
		}
	
		@Override
		public void suspendWork(String requestId, String workAssignmentId) {
		}
	
		@Override
		public void unassignWork(String requestId, String workAssignmentId) {
		}
	
		@Override
		public int unassignWorkForQuery(String requestId, String query) {
			return 0;
		}
	
		@Override
		public int unassignWorkFromBundle(String bundleId) {
			return 0;
		}
	
		@Override
		public void unassignWorkFromResource(String requestId, String resourceName,
				String fullBundleName, String serviceName) {
		}
	
		@Override
		public void unregisterWorkListener(String listenerId) {
		}
	
		@Override
		public void update(String id, String updateStatement) throws Exception {
		}
	
		@Override
		public void start() {
		}
	
		@Override
		public void stop() {
		}

		@Override
		public void removeWorkAssignmentsForResourceId(String agentId,
				long beforeTime) {
			// TODO Auto-generated method stub
			
		}
	}

}
