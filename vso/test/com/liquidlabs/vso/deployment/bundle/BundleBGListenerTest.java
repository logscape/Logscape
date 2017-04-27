package com.liquidlabs.vso.deployment.bundle;

import com.liquidlabs.common.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.liquidlabs.common.ThreadUtil;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.agent.ResourceProfile;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.ServiceInfo;
import com.liquidlabs.vso.resource.ResourceSpace;
import com.liquidlabs.vso.work.WorkAllocator;
import com.liquidlabs.vso.work.WorkAssignment;

public class BundleBGListenerTest {
	

	private WorkAllocator workAllocator;
	private BackgroundServiceAllocator bundleBGListener;
	Mockery context = new Mockery();
	private LookupSpace lookupSpace;
	private ORMapperFactory ormFactory;
	private SpaceServiceImpl workToAllocate;

	@Before
	public void setUp() throws Exception {
		workAllocator = context.mock(WorkAllocator.class);
		lookupSpace = context.mock(LookupSpace.class);
		ormFactory = new ORMapperFactory();
		context.checking(new Expectations(){{
				atLeast(1).of(lookupSpace).registerService(with(any(ServiceInfo.class)), with(any(long.class))); will(returnValue("lease"));
				atLeast(1).of(lookupSpace).unregisterService(with(any(ServiceInfo.class))); will(returnValue(true));
			}}
		);
		
		workToAllocate = new SpaceServiceImpl(lookupSpace, ormFactory, BundleSpace.NAME+"_PEND", ormFactory.getScheduler(), false, false, true);
		workToAllocate.start(this, "boot-1.0");

		
	
		
		
		Map<String, String> variables = new HashMap<String, String>();
		bundleBGListener = new BackgroundServiceAllocator(workToAllocate , variables, workAllocator, new URI("tcp://localhost"), new ServiceFinder() {

			public boolean isServiceRunning(String serviceId) {
				return true;
			}
			public boolean isWaitingForDependencies(String serviceName,
					String bundleName) {
				return false;
			}}, Executors.newScheduledThreadPool(5));
		
	}
	
	@Test
	public void testBackgroundWorkIsNOTAppliedToResourceWithoutBundle() throws Exception {
		// expect nothing
		
		// setup
		WorkAssignment workAssignment = new WorkAssignment("none", "none-0", 0, "NOBundle", "doSomeWork", "script", 10);
		workAssignment.setBackground(true);
		
//		context.checking(new Expectations() {
//			{
//				atLeast(1).of(workAllocator).assignWork(with(any(String.class)), with(any(String.class)), with(any(WorkAssignment.class)));
//			}
//		});
		
		// test
		workToAllocate.store(workAssignment, -1);
		bundleBGListener.register("resource1", new ResourceProfile());
		
		// workAllocator has 0 expects
		
	}
	
	@Test
	public void testBackgroundWorkISAppliedToResourceBundle() throws Exception {
		System.out.println("=====================" +  ThreadUtil.getMethodName());
		
		// setup
		WorkAssignment workAssignment = new WorkAssignment("none", "none-0", 0,"myBundle", "doSomeWork", "script", 10);
		workAssignment.setBackground(true);
		workAssignment.setAllocationsOutstanding(-1);
		workToAllocate.store(workAssignment, -1);
		ResourceProfile resourceProfile = new ResourceProfile();
		resourceProfile.setDeployedBundles("myBundle-0.01");
		
		// expects
		context.checking(new Expectations() {
			{
				one(workAllocator).assignWork(with(any(String.class)), with(any(String.class)), with(any(WorkAssignment.class)));
			}
		});
		
		// test
		bundleBGListener.register("resource1", resourceProfile);
		
	}
	
//	@Test DodgyTest? BackgroundAlloc stuff needs a rethink on testing
	public void testMultipleRegOfSameResourceIdDontMeanMultipleAllocations() throws Exception {
		System.out.println("======================== Running:" + ThreadUtil.getMethodName());
		
		int concurrency = 10;
		final WorkAssignment workAssignment = new WorkAssignment("none", "none-0", 0,"myBundle", "doSomeWork", "script", 10);
		workAssignment.setBackground(true);
		workAssignment.setAllocationsOutstanding(concurrency);
		workToAllocate.store(workAssignment, -1);
		
		final ResourceProfile resourceProfile = new ResourceProfile();
		resourceProfile.setDeployedBundles("myBundle-0.01");
		
		// expects
		context.checking(new Expectations() {
			{
				exactly(10).of(workAllocator).assignWork(with(any(String.class)), with(any(String.class)), with(any(WorkAssignment.class)));
			}
		});
		
		Assert.assertEquals("Only 1 item should have been assigned", concurrency, workToAllocate.findById(WorkAssignment.class, workAssignment.getId()).getAllocationsOutstanding());
		
		ExecutorService executor = Executors.newFixedThreadPool(concurrency);

		for (int i = 0; i < concurrency; i++) {
			executor.execute(new Runnable(){
				public void run() {
					try {
						bundleBGListener.register(resourceProfile.getResourceId(), resourceProfile);
						WorkAssignment copy = workAssignment.copy();
						// NOTE: this test is going to fail unless the Profile comes back with a started workId
						copy.setResourceId(resourceProfile.getResourceId());
						resourceProfile.addWorkAssignmentId(copy.getId());
					} catch (Throwable t) {
						t.printStackTrace();
					}
				}
			});
		}
		Thread.sleep(500);
		Assert.assertEquals("Only 1 item should have been assigned, outstanding should be:" + (concurrency - 1), concurrency -1, workToAllocate.findById(WorkAssignment.class, workAssignment.getId()).getAllocationsOutstanding());
		
	}

}
