package com.liquidlabs.log.space;

import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.ReplayEvent;
import com.liquidlabs.log.search.TimeUID;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.space.lease.Lease;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.resource.ResourceSpace;
import org.jmock.Mock;
import org.jmock.MockObjectTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


public class LogSpaceReportTest extends MockObjectTestCase{
	
	private LogEvent info;
	private LogEvent warning;
	private LogEvent error;
	private Mock lookupSpace;
	private LogSpace logSpace;
	private ORMapperFactory factory;
	private long toTimeMs = System.currentTimeMillis();
	private String location;
	private ResourceSpace resourceSpace;
	
	public static class MyEventListener implements LogEventListener {

		private List<String> received = new ArrayList<String>();
		public String getId() {
			return "MyEventListener";
		}

		public void handle(LogEvent event) {
			received.add(event.getId());
		}
		
	}
	
	class MyReplayListener implements LogReplayHandler {
		private List<TimeUID> received = new ArrayList<TimeUID>();

		public String getId() {
			return "MyReplayHandler";
		}

		public void handle(ReplayEvent event) {
			received.add(event.getId());
		}

		public void handle(Bucket event) {
		}

		public int handle(String providerId, String subscriber, int size, Map<String, Object> histo) {
			return 1;
		}

		public int handle(List<ReplayEvent> events) {
			return 100;
		}

		public int status(String provider, String subscriber, String msg) {
			return 1;
		}

		@Override
		public void handleSummary(Bucket bucketToSend) {
			// TODO Auto-generated method stub
			
		}
		
	}
	
	@Override
	protected void setUp() throws Exception {
		System.setProperty(Lease.PROPERTY, "1");
		VSOProperties.setResourceType(VSOProperties.MANAGEMENT);
		info = new LogEvent("sourceURI", "INFO this is a message", "host", "file", 1, 1, "");
		warning = new LogEvent("sourceURI", "WARNING this is a warning message", "host", "file", 2, 2, "");
		error = new LogEvent("sourceURI", "ERROR this is an error message", "host", "file", 3, 3, "");
		lookupSpace = mock(LookupSpace.class);
		lookupSpace.stubs();
		lookupSpace.stubs().method("registerService").withAnyArguments().will(returnValue("xxxx"));
		lookupSpace.stubs().method("unregisterService").withAnyArguments().will(returnValue(true));
		factory = new ORMapperFactory();

		SpaceServiceImpl spaceServiceImpl = new SpaceServiceImpl((LookupSpace)lookupSpace.proxy(),factory, LogSpace.NAME, factory.getProxyFactory().getScheduler(), false, false, true);
		logSpace = new LogSpaceImpl(spaceServiceImpl,spaceServiceImpl,null, null, null, null, null, resourceSpace, (LookupSpace) lookupSpace.proxy());
		logSpace.start();
	}
	
	@Override
	protected void tearDown() throws Exception {
		logSpace.stop();
		factory.stop();
	}
	
	public void testShouldSaveAndReadReport() throws Exception {
		logSpace.saveSearch(new Search("name", "owner", Arrays.asList("pattern"), "logFile", Arrays.asList(100), 1, "vars"), null);
		Search report = logSpace.getSearch("name", null);
		assertNotNull(report);
		assertEquals("name", report.name);
		assertTrue(report.patternFilter.get(0).contains("pattern | _path.contains(logFile)"));
	}
	public void testShouldDeleteReport() throws Exception {
		logSpace.saveSearch(new Search("name", "owner", Arrays.asList( "pattern"), "logFile", Arrays.asList( 1), 1, "vars"), null);
		logSpace.deleteSearch("name", null);
		Search report = logSpace.getSearch("name", null);
		assertNull(report);
	}
	
	
	
}
