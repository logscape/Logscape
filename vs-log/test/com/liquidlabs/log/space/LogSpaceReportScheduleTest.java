package com.liquidlabs.log.space;

import com.liquidlabs.log.alert.Schedule;
import com.liquidlabs.log.alert.report.HostLogSummary;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.space.VSpaceProperties;
import com.liquidlabs.space.lease.Lease;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.resource.ResourceSpace;
import org.jmock.Mock;
import org.jmock.MockObjectTestCase;
import org.junit.Test;

import java.io.File;
import java.util.List;


public class LogSpaceReportScheduleTest extends MockObjectTestCase{
	
	private Mock lookupSpace;
	private LogSpaceImpl logSpace;
	private ORMapperFactory factory;
	private ResourceSpace resourceSpace;
	
	@Override
	protected void setUp() throws Exception {
        new File(VSpaceProperties.baseSpaceDir(), "touch.file").delete();
		VSOProperties.setResourceType("Management");
		System.setProperty(Lease.PROPERTY, "1");
		lookupSpace = mock(LookupSpace.class);
		lookupSpace.stubs();
		lookupSpace.stubs().method("registerService").withAnyArguments().will(returnValue("xxxx"));
		lookupSpace.stubs().method("unregisterService").withAnyArguments().will(returnValue(true));
		factory = new ORMapperFactory();

		SpaceServiceImpl spaceServiceImpl = new SpaceServiceImpl((LookupSpace)lookupSpace.proxy(),factory, LogSpace.NAME, factory.getProxyFactory().getScheduler(), false, false, true);
		logSpace = new LogSpaceImpl(spaceServiceImpl, spaceServiceImpl,null, null, null, null, null, resourceSpace, (LookupSpace) lookupSpace.proxy());
		logSpace.start();
	}
	
	@Override
	protected void tearDown() throws Exception {
        new File(VSpaceProperties.baseSpaceDir(), "touch.file").delete();
		logSpace.stop();
		factory.stop();
	}
	
	@Test
	public void testShouldGetDurationCorrectly() throws Exception {
		Schedule thirtyMins = new Schedule("admin", "30m Exceptions", "Exception Report", "", "", "work/temp/{date}/30minExReport-{time}.text", "", "", "1", "", "", "", "", false, false, "*/30 * * * *", "all", "", true,"", "");
		// normally it will == 30mins - (when executed on schedule)
		assertTrue(thirtyMins.getTTL() == 30);
	}
	
	public void testShouldOnlySeeMyReport() throws Exception {
		
		HostLogSummary hostLogSummary = new HostLogSummary();
		hostLogSummary.filename = "/Users/neil/workspace/master/buildT/logscape/work/stuff/things/logspace.log";
		System.out.println(hostLogSummary.getXMLRow());
		List<Schedule> schedules = logSpace.getSchedules("");
		for (Schedule schedule : schedules) {
			logSpace.deleteSchedule(schedule.name);
		}
		
			
		logSpace.saveSchedule(new Schedule("admin", "aSchedule", "reportName", "","script", "copyAction", "logAction", "lastRun","1","", "", "", "",false, false, "10 10 * * *", "other", "",true,"",""));
		logSpace.saveSchedule(new Schedule("admin22", "aSchedule22", "reportName22", "","script", "copyAction", "logAction", "lastRun","1","", "", "", "",false, false, "10 10 * * *", "other", "",true,"",""));
		logSpace.saveSchedule(new Schedule("MY_USER", "mySchedule", "reportName", "","script", "copyAction", "logAction", "lastRun","1","", "", "", "",false, false, "10 10 * * *", "userDept", "",true,"",""));
		logSpace.saveSchedule(new Schedule("MY_USER_XX", "mySchedule444", "reportName444", "","script", "copyAction", "logAction", "lastRun","1","", "", "", "",false, false, "10 10 * * *", "other", "",true,"",""));
		
		assertEquals(1, logSpace.getSchedules("userDept").size());
		assertEquals(1, logSpace.getScheduleNames("userDept").size());
		assertEquals("mySchedule", logSpace.getSchedule("mySchedule").name);
	}
	public void testShouldSaveAndReadReport() throws Exception {
		
		HostLogSummary hostLogSummary = new HostLogSummary();
		hostLogSummary.filename = "/Users/neil/workspace/master/buildT/logscape/work/stuff/things/logspace.log";
		System.out.println(hostLogSummary.getXMLRow());
		
		
		logSpace.saveSchedule(new Schedule("admin", "mySchedule", "reportName", "","script", "copyAction", "logAction", "lastRun","1","", "", "", "",false, false, "10 10 * * *", "all", "", true,"",""));
		assertTrue(logSpace.getSchedules("").size() > 0);
		assertNotNull(logSpace.getSchedule("mySchedule"));
		assertEquals("mySchedule", logSpace.getSchedule("mySchedule").name);
	}
	public void testShouldDeleteReport() throws Exception {
		logSpace.saveSchedule(new Schedule("admin", "mySchedule", "reportName","","script", "copyAction", "logAction", "lastRun", "1", "", "", "", "", false, false, "10 10 * * *", "all", "", true,"",""));
		assertNotNull(logSpace.getSchedule("mySchedule"));
		logSpace.deleteSchedule("mySchedule");
		assertNull(logSpace.getSchedule("mySchedule"));
	}
}
