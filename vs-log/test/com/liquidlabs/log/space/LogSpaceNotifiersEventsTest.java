package com.liquidlabs.log.space;

import com.liquidlabs.log.CancellerListener;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.space.lease.Lease;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.resource.ResourceSpace;
import org.jmock.Mock;
import org.jmock.MockObjectTestCase;

import java.util.List;


@SuppressWarnings("unused")
public class LogSpaceNotifiersEventsTest extends MockObjectTestCase{
	
	private LogEvent info;
	private LogEvent warning;
	private LogEvent error;
	private Mock lookupSpace;
	private LogSpace logSpace;
	private ORMapperFactory factory;
	private long toTimeMs = System.currentTimeMillis();
	private Mock aggSpace;
	private String location;
	private ResourceSpace resourceSpace;
	
	protected void setUp() throws Exception {
		System.setProperty(Lease.PROPERTY, "1");
		VSOProperties.setResourceType("Management");
		info = new LogEvent("sourceURI", "INFO this is a message", "host", "file", 1, 1, "");
		warning = new LogEvent("sourceURI", "WARNING this is a warning message", "host", "file", 2, 2, "");
		error = new LogEvent("sourceURI", "ERROR this is an error message", "host", "file", 3, 3, "");
		lookupSpace = mock(LookupSpace.class);
		lookupSpace.stubs();
		lookupSpace.stubs().method("registerService").withAnyArguments().will(returnValue("xxxx"));
		lookupSpace.stubs().method("unregisterService").withAnyArguments().will(returnValue(true));
		factory = new ORMapperFactory();

		SpaceServiceImpl spaceServiceImpl = new SpaceServiceImpl((LookupSpace)lookupSpace.proxy(),factory, LogSpace.NAME, factory.getProxyFactory().getScheduler(), false, false, true);
		
		aggSpace = mock(AggSpace.class);
		
		logSpace = new LogSpaceImpl(spaceServiceImpl,spaceServiceImpl,null, (AggSpace)aggSpace.proxy(), null, null, null, resourceSpace, (LookupSpace) lookupSpace.proxy());
		logSpace.start();
	}
	
	@Override
	protected void tearDown() throws Exception {
		logSpace.stop();
		factory.stop();
	}
	
	
	public void testShouldNotifyConfigListener() throws Exception {
		MyConfigListener myConfigListener = new MyConfigListener();
		logSpace.registerConfigListener(myConfigListener.getId(), myConfigListener);
		logSpace.setLiveLogFilters(new LogFilters(new String[] { "stuff1" }, null));
		Thread.sleep(50);
		assertEquals(".*?stuff1.*", myConfigListener.filters.includes()[0]);
		
		logSpace.setLiveLogFilters(new LogFilters(new String[] { "stuff2" }, null));
		Thread.sleep(50);
		logSpace.setLiveLogFilters(new LogFilters(new String[] { "stuff3" }, null));
		Thread.sleep(50);
		assertTrue(myConfigListener.filters != null);
		assertEquals(".*?stuff3.*", myConfigListener.filters.includes()[0]);
	}
	
	
	public void testRequestCancellerCanRenewLease() throws Exception {
		MyRequestCanceller myRequestCanceller = new MyRequestCanceller();
		String lease = logSpace.registerCancelListener(myRequestCanceller.getId(), myRequestCanceller, 100);
		Thread.sleep(5);
		logSpace.renewLease(lease, 100);
		
		// no exception is pass
		
	}

	public void testShouldListRequests() throws Exception {
		lookupSpace.stubs().method("registerService").will(returnValue("stuff"));
		logSpace.start();
		
		LogRequest request = new LogRequest("sub", 100, 200);
		logSpace.executeRequest(request);
		LogRequest request1 = new LogRequest("sub2", 100, 200);
		logSpace.executeRequest(request1);
		List<LogReplayRequestState> requests = logSpace.loadLogRequests();
		assertEquals(2, requests.size());
	}
	public void testShouldCancelRequests() throws Exception {
		
		aggSpace.expects(once()).method("cancel");
		lookupSpace.stubs().method("registerService").will(returnValue("stuff"));
		logSpace.start();
		
		LogRequest request = new LogRequest("A3.5 - Agent MEM/CPUWed Dec 3 09:41:28 GMT0900 2008", 100, 200);
		logSpace.executeRequest(request);
		logSpace.cancel(request.subscriber());
		List<LogReplayRequestState> cancelRelayRequest = logSpace.loadLogRequests(); 
		assertEquals("should have cancelled request - got size:" + cancelRelayRequest.size(), 0, cancelRelayRequest.size());
	}
	
	public static class MyConfigListener implements LogConfigListener {

		public LogFilters filters;

		public void addWatch(WatchDirectory result) {
		}
		public String getId() {
			return "100";
		}
		public void setFilters(LogFilters filters) {
			this.filters = filters;
		}
		public void removeWatch(WatchDirectory result) {
		}
		public void updateWatch(WatchDirectory result) {
		}
	}
	public static class MyRequestCanceller implements CancellerListener {
		public void cancel(String subscriberId) {
		}
		public String getId() {
			return "XXXXXX----Canceller--------XXX";
		}
	}
}
