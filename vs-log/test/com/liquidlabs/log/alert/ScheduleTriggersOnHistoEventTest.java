package com.liquidlabs.log.alert;

import com.liquidlabs.admin.AdminSpace;
import com.liquidlabs.admin.DataGroup;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.functions.Count;
import com.liquidlabs.log.search.functions.Function;
import com.liquidlabs.log.space.*;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.space.VSpaceProperties;
import com.liquidlabs.space.lease.Lease;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.ServiceInfo;
import com.liquidlabs.vso.resource.ResourceSpace;
import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class ScheduleTriggersOnHistoEventTest {
	
	private LookupSpace lookupSpace;
	private AdminSpace adminSpace;
	private LogSpace logSpace;
	private ORMapperFactory factory;
	private AggSpaceImpl aggSpace;
	private SpaceServiceImpl bucketSpaceService;
	private ResourceSpace resourceSpace;
	private AggSpace remoteAggSpace;
	private SpaceServiceImpl logEventSpace;
	private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
	
	@Before
	public void setUp() throws Exception {
		System.setProperty(Lease.PROPERTY, "1");
		setupSoLogSpaceCanRunProperly();
		lookupSpace = mock(LookupSpace.class);
		when(lookupSpace.unregisterService(any(ServiceInfo.class))).thenReturn(true);

		adminSpace = mock(AdminSpace.class);
        resourceSpace = mock(ResourceSpace.class);
		
		factory = new ORMapperFactory();
		
		bucketSpaceService = new SpaceServiceImpl(lookupSpace, factory, AggSpace.NAME, factory.getProxyFactory().getScheduler(), false, false, false);
		aggSpace = new AggSpaceImpl("providerId", 
				bucketSpaceService, 
				new SpaceServiceImpl(lookupSpace, factory, AggSpace.NAME, factory.getProxyFactory().getScheduler(), false, false, false),
				new SpaceServiceImpl(lookupSpace, factory, AggSpace.NAME, factory.getProxyFactory().getScheduler(), false, false, true),
				factory.getProxyFactory().getScheduler());
		aggSpace.start();
		
		logEventSpace = new SpaceServiceImpl(lookupSpace, factory, LogSpace.NAME, factory.getProxyFactory().getScheduler(), false, false, true);
		
		URI clientAddress = factory.getClientAddress();
		
		remoteAggSpace = factory.getProxyFactory().getRemoteService(AggSpace.NAME, AggSpace.class, new String[] {  clientAddress.toString() });
		logSpace = new LogSpaceImpl(logEventSpace, logEventSpace, adminSpace, remoteAggSpace, null, null, null, resourceSpace,lookupSpace);
		logSpace.start();
	}
	
	@After
	public void tearDown() throws Exception {
		logSpace.stop();
		aggSpace.stop();
		factory.stop();
	}
	
	@Test
	public void testShouldModifyLastRun() throws Exception {
		System.out.println("-----------------");
		Schedule schedule =
                new Schedule("user", "mySchedule", "", "","", "", "", "", "", "", "", "", "",false, false, "* * * * *", "all", "",true,"","");
		logSpace.saveSchedule(schedule);
		schedule.run(logSpace, aggSpace, logEventSpace, adminSpace, scheduler, resourceSpace);
		Schedule savedSchedule = logSpace.getSchedule(schedule.name);
		assertFalse(savedSchedule.lastRun.equals(""));
		System.out.println("-----------------");
		
	}
	
	public void testname() throws Exception {
		Schedule schedule = new Schedule("bob", "bob", "", "","", "", "", "", "", "", "", "", "",false, false, "* * * * *", "all", "",true,"","");
		logSpace.saveSchedule(schedule);
		ScheduleExecution scheduleExecution = new ScheduleExecution("bob");
		scheduleExecution.updateLastRun();
		logEventSpace.store(scheduleExecution, -1);
		ScheduleExecution result = logEventSpace.findById(ScheduleExecution.class, "bob");
		assertNotNull(result);
	}
	
	@Test
	public void testShouldRunAScheduled() throws Exception {
		
		System.out.println("Saving Report");
		
		logSpace.saveSearch(new Search("reportName", "owner",Arrays.asList( "pattern"), "fileFilter",Arrays.asList(  1), 100, "vars"), null);
		
		// the trigger doesnt make sense cause the histo is not using real count function....breakpoint on the trigger evaluator shows these
		// values work
		String trigger = "pattern = 0.0";
		TestSchedule schedule = new TestSchedule("mySchedule", "reportName", "","", trigger, "copyAction", "logAction", "0 * * * *");
		System.out.println("Executing Report");

        when(adminSpace.getDataGroup(anyString(), anyBoolean())).thenReturn(new DataGroup());

		schedule.run(logSpace, remoteAggSpace, bucketSpaceService,adminSpace, scheduler, resourceSpace);
		
		String lastRequest = schedule.lastRequest;
		
		writeHistoEvents(lastRequest);
		
		ExpressionTrigger histoTrigger = (ExpressionTrigger) schedule.triggerListener;
		Thread.sleep(5000);
        assertNotNull("Trigger Not Created", histoTrigger);
		assertEquals(1, histoTrigger.bucketsReceived);
		assertEquals(1, histoTrigger.triggerCount);
		assertTrue("Trigger did not fire", histoTrigger.firedOnceToPreventSpam);
		
		assertNotNull(schedule.lastRun);
	}
	
	private void writeHistoEvents(String subscriber) {
		System.out.println("Write Events");

		List<Function> function = new ArrayList<Function>();
		Count count = new Count("C","GroupByGroup", "ApplyToGroup");
		MatchResult matchResult = new MatchResult("group1","group2");
		count.execute(FieldSets.getBasicFieldSet(), new String[] { "one", "two" }, "*", DateTimeUtils.currentTimeMillis(), matchResult, "raw line data", 0, 1);
		function.add(count);
		
		Bucket bucket1 = new Bucket(0, DateTimeUtils.currentTimeMillis(), function , 0, "*", "sourceURI", subscriber, "");
		aggSpace.write(bucket1, false, "", 0,0);
		Bucket bucket2 = new Bucket(0, DateTimeUtils.currentTimeMillis(), function , 0, "*", "sourceURI", subscriber, "");
		aggSpace.write(bucket2, false, "", 0,0);
		Bucket bucket3 = new Bucket(0, DateTimeUtils.currentTimeMillis(), function , 0, "*", "sourceURI", subscriber, "");
		aggSpace.write(bucket3, false, "", 0,0);
	}
	
	public static class TestSchedule extends Schedule {

		public TestSchedule(String string, String string2, String string3, String string4, String trigger, String string5, String string6, String cron) {
			super("admin", string, string2, "", "", string5, string6, "lastRun", trigger, "", "", "", "", false, false, cron, "all", "",true,"","");
		}
		public String subscriber() {
			return subcriber;
		}
	}
	private void setupSoLogSpaceCanRunProperly() {
		VSOProperties.setResourceType("Management");
		new File(VSpaceProperties.baseSpaceDir(), "touch.file").delete();
	}
}
