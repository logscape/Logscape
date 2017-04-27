package com.liquidlabs.vso.work;

import java.util.List;
import java.util.concurrent.Executors;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.joda.time.DateTimeUtils;
import org.joda.time.format.DateTimeFormat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.transport.serialization.Convertor;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.ServiceInfo;

public class WorkAllocationUnitTest {
	
	private LookupSpace lookupSpace;
	private WorkAllocatorImpl workAllocator;
	private TestWorkListener workListener;
	Mockery context = new Mockery();
	
	@Before
	public void setUp() throws Exception {
		
		
		lookupSpace = context.mock(LookupSpace.class);
//		lookupSpace.stubs();
//		lookupSpace.stubs().method("unregisterService").will(returnValue(true));
		SpaceServiceImpl space = new SpaceServiceImpl(lookupSpace, new ORMapperFactory(), WorkAllocator.NAME, Executors.newScheduledThreadPool(2), false, false, true);
		
		context.checking(new Expectations() {
			{
				atLeast(0).of(lookupSpace).registerService(with(any(ServiceInfo.class)), with(any(long.class))); will(returnValue("string"));
				atLeast(0).of(lookupSpace).unregisterService(with(any(ServiceInfo.class))); will(returnValue(true));
					
			}
			
		});
		
		workAllocator = new WorkAllocatorImpl(space);
		workAllocator.start();
		workListener = new TestWorkListener();
		workAllocator.registerWorkListener(workListener, "agent", "listener", false);
		
	}
	
	
	@After
	public void tearDown() throws Exception {
		workAllocator.stop();
	}
	
	@Test
	public void testShouldUpdateWorkAssignmentSafely2() throws Exception {
		String script = "System.out.println(\"*****************************************************************************\")" + 
		"    	System.out.println(\"**********************  STARTING [Dashboard]  *******************************\")" + 
		"    	System.out.println(\"************                                        ************************\")\r\n" + 
		"		int port = new NetworkUtils().determinePort(Integer.getInteger(\"web.app.port\", 8080));\r\n" + 
		"    	System.out.println(\"***    http://\" + hostname + \":\" + port + \" **********\")" + 
		"    	System.out.println(\"*****************************************************************************\")\r\n" + 
		"		System.setProperty(\"upload.base.dir\", \"downloads\")\r\n" + 
		"		File file = new File(\"./system-bundles/dashboard-1.0/dashboard.war\");\r\n" + 
		"        if (!file.exists()) throw new RuntimeException(\"Could NOT open WAR file from:\" + new File(\".\").getAbsolutePath());\r\n" + 
		"\r\n" + 
		"    >>>ONE<<<    processMaker.java \"-cp:system-bundles/dashboard-1.0:.:system-bundles/*/lib/*.jar:\",  \"com.liquidlabs.dashboard.server.JettyMain\", \"-Dlog4j.debug=false\", \"-Dvs.agent.address=\" + ResourceAgent, \"-Dvs.agent.id=\" + resourceId,  \"-Dlookup.url=\" + lookupSpaceAddress, \"-Xverify:none\",\"-Xmx100M\", new Integer(port).toString(), file.getAbsolutePath()" +
		""; 
//		workAllocator.update("nothing", statement);
		String resourceId = "resource";
		workAllocator.assignWork("requestId", "agent-0", new WorkAssignment(resourceId, resourceId + "-0",0, "bundle", "service", script, 10));
		
		List<WorkAssignment> work = workAllocator.getWorkAssignmentsForQuery("resourceId equals agent-0");
		WorkAssignment workAss = work.get(0);
		System.out.println("Script:" + workAss.getScript());
		Assert.assertTrue(workAss.getScript().contains(">>>ONE<<<"));
		
		String script2 = "System.out.println(\"*****************************************************************************\")\r\n" + 
		"    	System.out.println(\"***    http://\" + hostname + \":\" + port + \" **********\")\r\n" + 
		"		System.setProperty(\"upload.base.dir\", \"downloads\")\r\n" + 
		"		File file = new File(\"./system-bundles/dashboard-1.0/dashboard.war\");\r\n" + 
		"        if (!file.exists()) throw new RuntimeException(\"Could NOT open WAR file from:\" + new File(\".\").getAbsolutePath());\r\n" + 
		"\r\n" + 
		"    >>>TWO<<<    processMaker.java \"-cp:system-bundles/dashboard-1.0:.:system-bundles/*/lib/*.jar:\",  \"com.liquidlabs.dashboard.server.JettyMain\", \"-Dlog4j.debug=false\", \"-Dvs.agent.address=\" + ResourceAgent, \"-Dvs.agent.id=\" + resourceId,  \"-Dlookup.url=\" + lookupSpaceAddress, \"-Xverify:none\",\"-Xmx100M\", new Integer(port).toString(), file.getAbsolutePath()" +
		""; 
		String updateStatement = 
			"status replaceWith WARNING and script replaceWith '" + script2 + "'";
		workAllocator.update(workAss.getId(), updateStatement);
		// used to blow up because no non-closed \' character
		List<WorkAssignment> work2 = workAllocator.getWorkAssignmentsForQuery("resourceId equals agent-0");
		WorkAssignment workAss2 = work2.get(0);
		System.out.println("Script:" + workAss2.getScript());
		Assert.assertTrue(workAss2.getScript().contains(">>>TWO<<<"));
	}
	
	
	@Test
	public void testShouldUpdateWorkAssignmentSafely() throws Exception {
		String statement = 
			"status replaceWith WARNING and errorMsg replaceWith '2/17/09 8:20 AM Failed to execute SLAAction[java.lang.RuntimeException: java.lang.RuntimeException: java.lang.RuntimeException: VScapeRemoteException[stcp://ip-10-250-87-79:11100] method[public abstract int com.liquidlabs.vso.resource.ResourceSpace.requestResources(java.lang.String,int,int,java.lang.String,java.lang.String,int,java.lang.String,java.lang.String)] exType[class java.lang.reflect.InvocationTargetException] cause[null]"	;
		workAllocator.update("nothing", statement);
		
		// used to blow up because no non-closed \' character
	}
	

	
	@Test
	public void testShouldSerializeWorkAssignmentOk() throws Exception {
		String resourceId = "resource";
		WorkAssignment workAssignment = new WorkAssignment(resourceId, resourceId + "-0",0, "bundle", "service", "this is a script", 10);
		workAssignment.setErrorMsg("'2/17/09 8:20 AM Failed to execute SLAAction[java.lang.RuntimeException: java.lang.RuntimeException: java.lang.RuntimeException: VScapeRemoteException[stcp://ip-10-250-87-79:11100] method[public abstract int com.liquidlabs.vso.resource.ResourceSpace.requestResources(java.lang.String,int,int,java.lang.String,java.lang.String,int,java.lang.String,java.lang.String)] exType[class java.lang.reflect.InvocationTargetException] cause[null]");
		String timestamp = DateTimeFormat.shortDateTime().print(DateTimeUtils.currentTimeMillis());
		workAssignment.setTimeStamp(timestamp);
		workAssignment.setTimestampMs(DateTimeUtils.currentTimeMillis());
		Convertor convertor = new Convertor();
		String string = convertor.getStringFromObject(workAssignment);
		WorkAssignment result = (WorkAssignment) convertor.getObjectFromString(WorkAssignment.class, string);
		Assert.assertNotNull(result);
	}
	
	@Test
	public void testShouldDoWork() throws Exception {
		String resourceId = "resource";
		workAllocator.assignWork("requestId", "agent-0", new WorkAssignment(resourceId, resourceId + "-0",0, "bundle", "service", "", 10));
		Thread.sleep(1000);
		Assert.assertEquals(1, workListener.startCount);
	}
	@Test
	public void testShouldUnAssignWork() throws Exception {
		String resourceId = "resource";
		WorkAssignment work = new WorkAssignment(resourceId, resourceId + "-0",0, "bundle", "service", "", 10);
		work.getAgentId();
		workAllocator.assignWork("requestId", "agent-0", work);
		workAllocator.unassignWork("requestId", work.getId());
		Thread.sleep(100);
		Assert.assertEquals(1, workListener.startCount);
		Assert.assertEquals(1, workListener.stopCount);
	}
	@Test
	public void testShouldUpdateWorkStatus() throws Exception {
		String resourceId = "resource";
		WorkAssignment work = new WorkAssignment(resourceId, resourceId + "-0",0, "bundle", "service", "", 10);
		work.getAgentId();
		workAllocator.assignWork("requestId", "agent-0", work);
		workAllocator.update(work.getId(), "invokableEP replaceWith 'stcp://alteredcarbon.local:12001'");
		List<WorkAssignment> results = workAllocator.getWorkAssignmentsForQuery("invokableEP equals 'stcp://alteredcarbon.local:12001'");
		List<WorkAssignment> results2 = workAllocator.getWorkAssignmentsForQuery("invokableEP contains altered");
		List<WorkAssignment> allWork = workAllocator.getWorkAssignmentsForQuery("id equals " + work.getId());
		Assert.assertTrue("Should have got 1 result for query:" + allWork, results.size() == 1);
		Thread.sleep(100);
		Assert.assertEquals(1, workListener.startCount);
		Assert.assertEquals(1, workListener.updateCount);
	}
	
	@Test
	public void testShouldStartStopWorkListenered() throws Exception {
		String resourceId = "resource";
		System.out.println("========================= Assign work ============" + resourceId);
		WorkAssignment work1 = new WorkAssignment(resourceId, resourceId + "-0",0, "bundle", "serviceONE", "", 10);
		workAllocator.assignWork("requestId", "agent-0", work1);
		Thread.sleep(100);
		Assert.assertEquals(1, workListener.startCount);
		
		workAllocator.unregisterWorkListener("agent");
		
		workAllocator.registerWorkListener(workListener, "agent", "listener", false);
		System.out.println("========================= Assign work listener ============");
		WorkAssignment work2 = new WorkAssignment(resourceId, resourceId + "-0",0, "bundle", "serviceTWO", "", 10);
		workAllocator.assignWork("requestId", "agent-0", work2);
		Thread.sleep(100);
		Assert.assertEquals(2, workListener.startCount);
	}
	
	public static class TestWorkListener implements WorkListener {
		int startCount = 0;
		int stopCount = 0;
		int updateCount = 0;

		public void start(WorkAssignment workAssignment) {
			System.out.println(">>>>>>>>> start");
			startCount++;
		}

		public void stop(WorkAssignment workAssignment) throws RuntimeException {
			System.out.println(">>>>>>>>> stop");
			stopCount++;
		}

		public void update(WorkAssignment workAssignment) {
			System.out.println(">>>>>>>>> update");
			updateCount++;
		}

		public String getId() {
			return "WorkListener";
		}
		
	}
}
