package com.liquidlabs.log.space;

import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.ReplayEvent;
import com.liquidlabs.orm.ORMapperClient;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.lookup.LookupSpace;
import org.jmock.Mock;
import org.jmock.MockObjectTestCase;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

public class AggSpaceTest extends MockObjectTestCase {

	private ORMapperFactory mapperFactory;
	private Mock scheduler;
	private Mock lookupSpace;
	private Mock orMapperClient;
	private AggSpace aggSpace;
	private String location = "LDN";

	@Override
	protected void setUp() throws Exception {
		lookupSpace = mock(LookupSpace.class);
		scheduler = mock(ScheduledExecutorService.class);
		scheduler.stubs();
		orMapperClient = mock(ORMapperClient.class);
		mapperFactory = new ORMapperFactory() {
			public ORMapperClient getORMapperClient(String name, Object serviceImpl, boolean isClustered, boolean persistent) {
				return (ORMapperClient) orMapperClient.proxy();
			}
		};
		orMapperClient.stubs().method("store");
		
		aggSpace = new AggSpaceImpl("providerId",
									new SpaceServiceImpl((LookupSpace)lookupSpace.proxy(),mapperFactory, LogSpace.NAME, mapperFactory.getProxyFactory().getScheduler(), false, false, false), 
									new SpaceServiceImpl((LookupSpace)lookupSpace.proxy(),mapperFactory, LogSpace.NAME, mapperFactory.getProxyFactory().getScheduler(), false, false, false), 
									new SpaceServiceImpl((LookupSpace)lookupSpace.proxy(),mapperFactory, LogSpace.NAME, mapperFactory.getProxyFactory().getScheduler(), false, false, true), 
									(ScheduledExecutorService) scheduler.proxy());
		
	}
	
	public void testShouldStart() throws Exception {
		lookupSpace.expects(once()).method("registerService").withAnyArguments().will(returnValue("stuff"));
		lookupSpace.expects(once()).method("registerService").withAnyArguments().will(returnValue("stuff"));
		lookupSpace.expects(once()).method("registerService").withAnyArguments().will(returnValue("stuff"));
		aggSpace.start();
	}
	
	public void testShouldStop() throws Exception {
		lookupSpace.expects(once()).method("registerService").withAnyArguments().will(returnValue("stuff"));
		lookupSpace.expects(once()).method("registerService").withAnyArguments().will(returnValue("stuff"));
		lookupSpace.expects(once()).method("registerService").withAnyArguments().will(returnValue("stuff"));

		lookupSpace.expects(once()).method("unregisterService").withAnyArguments().will(returnValue(true));
		lookupSpace.expects(once()).method("unregisterService").withAnyArguments().will(returnValue(true));
		lookupSpace.expects(once()).method("unregisterService").withAnyArguments().will(returnValue(true));
		aggSpace.start();
		aggSpace.stop();
	}
	
	public static class MyLogEventListener implements LogEventListener {

		public void handle(LogEvent event) {
		}

		public String getId() {
			return "myId";
		}
	}
	
	static class MyLogReplayListener implements LogReplayHandler {

		public String getId() {
			return "myId";
		}
		public void handle(ReplayEvent event) {
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
		public void handleSummary(Bucket bucketToSend) {
		}
	}
	
}
