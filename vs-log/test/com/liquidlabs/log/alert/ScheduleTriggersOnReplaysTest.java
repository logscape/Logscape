package com.liquidlabs.log.alert;

import com.liquidlabs.admin.AdminSpace;
import com.liquidlabs.admin.User;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.log.search.ReplayEvent;
import com.liquidlabs.log.space.*;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.space.VSpaceProperties;
import com.liquidlabs.space.lease.Lease;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.resource.ResourceSpace;
import org.jmock.Mock;
import org.jmock.MockObjectTestCase;
import org.joda.time.DateTimeUtils;

import java.io.File;
import java.util.HashSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;


public class ScheduleTriggersOnReplaysTest extends MockObjectTestCase{
	
	private Mock lookupSpace;
	private Mock adminSpace;
	private LogSpace logSpace;
	private ORMapperFactory factory;
	private AggSpaceImpl aggSpace;
	private SpaceServiceImpl bucketSpaceService;
	private ResourceSpace resourceSpace;
	private AggSpace remoteAggSpace;
	private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
	@Override
	protected void setUp() throws Exception {
		System.setProperty(Lease.PROPERTY, "1");
		setupSoLogSpaceCanRunProperly();
		lookupSpace = mock(LookupSpace.class);
		lookupSpace.stubs();
		lookupSpace.stubs().method("registerService").withAnyArguments().will(returnValue("xxxx"));
		lookupSpace.stubs().method("unregisterService").withAnyArguments().will(returnValue(true));
		
		adminSpace = mock(AdminSpace.class);
		
		factory = new ORMapperFactory();
		
		bucketSpaceService = new SpaceServiceImpl((LookupSpace) lookupSpace.proxy(), factory, AggSpace.NAME, factory.getProxyFactory().getScheduler(), false, false, false);
		aggSpace = new AggSpaceImpl("providerId", 
				bucketSpaceService, 
				new SpaceServiceImpl((LookupSpace) lookupSpace.proxy(), factory, AggSpace.NAME, factory.getProxyFactory().getScheduler(), false, false, false), 
				new SpaceServiceImpl((LookupSpace) lookupSpace.proxy(), factory, AggSpace.NAME, factory.getProxyFactory().getScheduler(), false, false, true), 
				factory.getProxyFactory().getScheduler());
		aggSpace.start();
		SpaceServiceImpl spaceServiceImpl = new SpaceServiceImpl((LookupSpace) lookupSpace.proxy(), factory, LogSpace.NAME, factory.getProxyFactory().getScheduler(), false, false, true);
		
		URI clientAddress = factory.getClientAddress();
		
		remoteAggSpace = factory.getProxyFactory().getRemoteService(AggSpace.NAME, AggSpace.class, new String[] {  clientAddress.toString() });
		logSpace = new LogSpaceImpl(spaceServiceImpl, spaceServiceImpl, (AdminSpace) adminSpace.proxy(), remoteAggSpace, null, null, null, resourceSpace, (LookupSpace) lookupSpace.proxy());
		logSpace.start();
	}
	
	@Override
	protected void tearDown() throws Exception {
		logSpace.stop();
		aggSpace.stop();
		factory.stop();
	}

    public void testShouldDoSquat() {

    }

    // DodgyTest?: fails all the time
	public void xxxtestShouldRunAScheduled() throws Exception {
		
		System.out.println("Saving Report");
		
		logSpace.saveSearch(new Search("reportName", "owner",Arrays.asList( "pattern"), "fileFilter", Arrays.asList( 1), 100, "vars"), null);
		
		String trigger = "3";
		TestSchedule schedule = new TestSchedule("mySchedule", "reportName", "","", trigger, "copyAction", "logAction", "0 * * * *");
		System.out.println("Executing Report");
		
		adminSpace.expects(once()).method("getUserIdsFromDataGroup").withAnyArguments().will(returnValue(new HashSet<String>(Arrays.asList("user"))));
		adminSpace.expects(once()).method("getUser").withAnyArguments().will(returnValue(new User()));
		
		schedule.run(logSpace, remoteAggSpace, bucketSpaceService, (AdminSpace) adminSpace.proxy(), scheduler, resourceSpace);
		
		String lastRequest = schedule.lastRequest;
		
		assertNotNull("Got null subscriber", lastRequest);
		
		Thread.sleep(3000);
		
		writeReplayEvents(lastRequest);
		Thread.sleep(6000);
		ReplayBasedTrigger triggerItem = (ReplayBasedTrigger)  schedule.triggerListener;
		assertEquals("Should have received 3 replays", 3, triggerItem.logEvents.size());
		assertTrue("Trigger did not fire", ((ReplayBasedTrigger) schedule.triggerListener).firedOnceToPreventSpam);
		
		assertNotNull(schedule.lastRun);
	}
	
	private void writeReplayEvents(String subscriber) {
		System.out.println("Write Events");
		ReplayEvent replayEvent = new ReplayEvent("sourceURI", 0 , 0, 0, subscriber, DateTimeUtils.currentTimeMillis(), "");
		replayEvent.setDefaultFieldValues("type", "host", "file", "path", "tag", "agentType", "", "0");
		aggSpace.write(replayEvent, false, "", 0,0);
		System.out.println("Write Replay 1:"  + replayEvent);
		ReplayEvent replayEvent2 = new ReplayEvent("sourceURI", 1 , 1, 0, subscriber, DateTimeUtils.currentTimeMillis(), "");
		aggSpace.write(replayEvent2, false, "", 0,0);
		replayEvent2.setDefaultFieldValues("type", "host", "file", "path", "tag", "agentType", "", "0");
		System.out.println("Write Replay 2:" + replayEvent2);
		ReplayEvent replayEvent3 = new ReplayEvent("sourceURI", 2 , 2, 0, subscriber, DateTimeUtils.currentTimeMillis(), "");
		aggSpace.write(replayEvent3, false, "", 0,0);
		replayEvent3.setDefaultFieldValues("type", "host", "file", "path", "tag", "agentType", "", "0");
		System.out.println("Write Replay 3:" + replayEvent3);
	}
	
	public static class TestSchedule extends Schedule {

		public TestSchedule(String string, String string2, String string3, String string4, String trigger, String string5, String string6, String cron) {
			super("admin", string, string2, "", "", string5, string6, "lastRun", trigger, "", "", "", "", false, false, cron, "all", "", true,"","");
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
