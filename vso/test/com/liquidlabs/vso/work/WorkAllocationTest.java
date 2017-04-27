package com.liquidlabs.vso.work;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.vso.FunctionalTestBase;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class WorkAllocationTest extends FunctionalTestBase {
	
	private static final String SCRIPT_1 = 
	"	import com.liquidlabs.vso.work.WorkAllocationTest\n" +
	"	println \"do stuff1\"\n" +
	"	WorkAllocationTest.incrementCount()\n" +
	"	println \"call count is: ${WorkAllocationTest.callCount}\"";
	
	private static int callCount;
    private static CountDownLatch latch;

	protected void setUp() throws Exception {
		agentCount = 2;
		super.setUp();
		callCount = 0;
        latch = new CountDownLatch(1);
	}
	protected void tearDown() throws Exception {
		super.tearDown();
	}
	public static void incrementCount(){
		callCount++;
        latch.countDown();
	}

	public void testShouldPurgeAssignmentsBeforeTime() throws Exception {
		String resourceId = agents.get(0).getId();
		workAllocator.assignWork("requestId", resourceId + "-0", new WorkAssignment(resourceId, resourceId + "-0",0, "bundle", "service", SCRIPT_1, 10));
		
		workAllocator.removeWorkAssignmentsForResourceId(resourceId, System.currentTimeMillis()+1);
		assertEquals(0, workAllocator.getWorkAssignmentsForQuery("").size());
	}
	public void testShouldUpdateWork() throws Exception {
		String resourceId = agents.get(0).getId();
		workAllocator.assignWork("requestId", resourceId + "-0", new WorkAssignment(resourceId, resourceId + "-0",0, "bundle", "service", SCRIPT_1, 10));
		
		List<WorkAssignment> work = workAllocator.getWorkAssignmentsForQuery("");
		assertEquals(1, work.size());
		
		workAllocator.update(work.get(0).getId(), "status replaceWith " + LifeCycle.State.RUNNING);
		
		List<WorkAssignment> work2 = workAllocator.getWorkAssignmentsForQuery("");
		assertEquals(1, work2.size());
		
		assertEquals("Status Should have been running but was:" + work2.get(0).getStatus(), LifeCycle.State.RUNNING, work2.get(0).getStatus());
		
	}
	
	public void testShouldDoWork() throws Exception {
		String resourceId = agents.get(0).getId();
		workAllocator.assignWork("requestId", resourceId + "-0", new WorkAssignment(resourceId, resourceId + "-0",0, "bundle", "service", SCRIPT_1, 10));
		assertThat(latch.await(2, TimeUnit.SECONDS), is(true));
		assertEquals("should have done 1 item of work BUT callCount was:" + callCount + " Agent:" + resourceId + " not Assigned", 1, callCount);
	}
	
	public void testShouldUnAssignWork() throws Exception {
		String resourceId = agents.get(0).getId();
		System.out.println("========================= Assign work ============" + resourceId);
		WorkAssignment workInfo = new WorkAssignment(resourceId, resourceId + "-0",0, "bundle", "service", SCRIPT_1, 10);
		workAllocator.assignWork("requestId", resourceId + "-0", workInfo);

        assertThat(latch.await(2, TimeUnit.SECONDS), is(true));
		System.out.println("========================= Unregister listener ============");
		workAllocator.unregisterWorkListener(resourceId);
		agents.get(0).stop(workInfo);
		
		System.out.println("========================= Register listener ============");
		workAllocator.registerWorkListener(agents.get(0), resourceId, resourceId, false);
        latch = new CountDownLatch(1);
        workAllocator.assignWork("requestId", resourceId + "-0", new WorkAssignment(resourceId, resourceId + "-0",0, "bundle", "serviceTWO", SCRIPT_1, 10));

        assertThat(latch.await(2, TimeUnit.SECONDS), is(true));

		assertEquals("should have done 2 item of work BUT callCount was:" + callCount + " Agent:" + resourceId + " not Assigned", 2, callCount);
	}
	
	public void testShouldUpdateWorkStatus() throws Exception {
		String resourceId = "res1";
		WorkAssignment work = new WorkAssignment(resourceId, resourceId + "-0",0, "bundle", "service", SCRIPT_1, 10);
		workAllocator.assignWork("requestId", resourceId + "-0", work);
		workAllocator.update(work.getId(), "status replaceWith RUNNING");
		List<WorkAssignment> works = workAllocator.getWorkAssignmentsForQuery("id equals " + work.getId());
		System.err.println(works);
		assertEquals(1, works.size());
		assertEquals(LifeCycle.State.RUNNING, works.get(0).getStatus());
		
		// dont want any exceptions
		
	}
}
