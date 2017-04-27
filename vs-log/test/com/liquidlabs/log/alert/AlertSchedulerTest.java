package com.liquidlabs.log.alert;

import com.liquidlabs.admin.AdminSpace;
import com.liquidlabs.log.space.AggSpace;
import com.liquidlabs.log.space.LogSpace;
import com.liquidlabs.vso.SpaceService;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.ServiceInfo;
import com.liquidlabs.vso.resource.ResourceSpace;
import org.jmock.Mock;
import org.jmock.MockObjectTestCase;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AlertSchedulerTest extends MockObjectTestCase {
	
	private Mock logSpace;
	private Mock aggSpace;
	private Mock logDataSpaceService;
	private Mock lookupSpace;
	private ServiceInfo serviceInfo;
	private Mock adminSpace;
	private AlertScheduler scheduler;
    private Mock resourceSpace;

    public void setUp() {
		VSOProperties.setResourceType("Management");
		
		logSpace = mock(LogSpace.class);
		logSpace.stubs();
		logSpace.expects(once()).method("getScheduleNames").with(ANYTHING).will(returnValue(new ArrayList()));
		aggSpace = mock(AggSpace.class);
		adminSpace = mock(AdminSpace.class);
		logDataSpaceService = mock(SpaceService.class);
		lookupSpace = mock(LookupSpace.class);
        resourceSpace = mock(ResourceSpace.class);
		serviceInfo = new ServiceInfo();
		
		scheduler = new AlertScheduler((LogSpace)logSpace.proxy(), (AggSpace)aggSpace.proxy(), (AdminSpace)adminSpace.proxy(), (SpaceService) logDataSpaceService.proxy(), (LookupSpace) lookupSpace.proxy(),  serviceInfo, (ResourceSpace) resourceSpace.proxy());
		
		scheduler.start();
	}

    public void testShouldGetWebSocketPort() throws Exception {
        Schedule testSchedule = new Schedule("task1", "* * * * *","","","","","","","","","","","",true, false,"","","",true,"11000","");
        assertEquals("11000",testSchedule.getWebSocketPort());

        assertEquals("11000",new Schedule("task1", "* * * * *","","","","","","","","","","","",true, false,"","","",true,"11000","someOtherCrap").getWebSocketPort());

        assertEquals("",new Schedule("task1", "* * * * *","","","","","","","","","","","",true, false,"","","",true,"","").getWebSocketPort());


    }
	
	public void testShouldScheduleAndRunTask() throws Exception {
		CountDownLatch latch = new CountDownLatch(1);
		TestSchedule testSchedule = new TestSchedule("task1", "* * * * *", latch);
		scheduler.scheduleATask(testSchedule);
		
		boolean finished = latch.await(70, TimeUnit.SECONDS);
		assertTrue(finished);
		
	}
	public static class TestSchedule extends Schedule {
		public boolean fired = false;
		private final CountDownLatch latch;
		public TestSchedule(String name, String cron, CountDownLatch latch) {
			super.name = name;
			super.cron = cron;
			this.latch = latch;
		}
		public void run(LogSpace logSpace, AggSpace aggSpace,
				SpaceService spaceService, AdminSpace adminSpace,
				ScheduledExecutorService scheduler) {
			System.out.println("Running:" + this.name);
			fired = true;
			latch.countDown();
		}
	}

}
