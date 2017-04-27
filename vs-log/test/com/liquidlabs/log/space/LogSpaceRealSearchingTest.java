package com.liquidlabs.log.space;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.log.search.Query;
import com.liquidlabs.log.search.filters.Contains;
import com.liquidlabs.log.search.functions.Average;
import com.liquidlabs.log.search.functions.Count;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.space.lease.Lease;
import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.TransportFactoryImpl;
import com.liquidlabs.transport.proxy.ProxyFactoryImpl;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.resource.ResourceSpace;
import org.jmock.Mock;
import org.jmock.MockObjectTestCase;

import java.net.URISyntaxException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class LogSpaceRealSearchingTest extends MockObjectTestCase{
	
	private Mock lookupSpace;
	private LogSpaceImpl logSpace;
	private ORMapperFactory factory;
	private long toTimeMs = System.currentTimeMillis();
	private AggSpaceImpl aggSpace;
	private ProxyFactoryImpl proxyFactoryB;
	public CountDownLatch countDownLatch;
	private String hostname = "host";
	private ResourceSpace resourceSpace;
	
	@Override
	protected void setUp() throws Exception {
		System.setProperty(Lease.PROPERTY, "1");
		lookupSpace = mock(LookupSpace.class);
		lookupSpace.stubs();
		lookupSpace.stubs().method("registerService").withAnyArguments().will(returnValue("xxxx"));
		lookupSpace.stubs().method("unregisterService").withAnyArguments().will(returnValue(true));
		factory = new ORMapperFactory();	
		aggSpace = new AggSpaceImpl("providerId",
										new SpaceServiceImpl((LookupSpace)lookupSpace.proxy(),factory, "BUCKET" + LogSpace.NAME, factory.getProxyFactory().getScheduler(), false, false, false), 
										new SpaceServiceImpl((LookupSpace)lookupSpace.proxy(),factory, "REPLAY" + LogSpace.NAME, factory.getProxyFactory().getScheduler(), false, false, false), 
										new SpaceServiceImpl((LookupSpace)lookupSpace.proxy(),factory, "REPLAY" + LogSpace.NAME, factory.getProxyFactory().getScheduler(), false, false, true), 
										factory.getProxyFactory().getScheduler());
		aggSpace.start();
		SpaceServiceImpl spaceServiceImpl = new SpaceServiceImpl((LookupSpace)lookupSpace.proxy(),factory, LogSpace.NAME, factory.getProxyFactory().getScheduler(), false, false, true);
		logSpace = new LogSpaceImpl(spaceServiceImpl,spaceServiceImpl, null, aggSpace, null, null, null, resourceSpace, (LookupSpace) lookupSpace.proxy());
		logSpace.start();
		transportFactory = new TransportFactoryImpl(Executors.newCachedThreadPool(), "test");
	}
	
	@Override
	protected void tearDown() throws Exception {
		logSpace.stop();
		aggSpace.stop();
		factory.stop();
		proxyFactoryB.stop();
	}

	
	public void testShouldReceiveSearchRequest() throws Exception {
		int amount = 10;
		countDownLatch = new CountDownLatch(amount);
		for (int i = 0; i < amount; i++) {
			LogRequestHandler logReplayer = getRemoteLogReplayer();
	 		logSpace.registerRequestHandler("myListener" + i, logReplayer, hostname);
		}
		
		LogRequest request = makeRequest(".*TradeService\\.(\\w+).*\\|(\\d+)ms.* | verbose(true) ");
		request.setVerbose(true);
		System.out.println("Writing SEARCH request:" + request.subscriber());
		logSpace.executeRequest(request);
		countDownLatch.await(10, TimeUnit.SECONDS);
		
		assertEquals(amount, searchCount);
	}

	private LogRequestHandler getRemoteLogReplayer() throws URISyntaxException {
		proxyFactoryB = new ProxyFactoryImpl(transportFactory, TransportFactoryImpl.getDefaultProtocolURI("", "localhost", 22222, "readTest"), Executors.newCachedThreadPool(), "");
		proxyFactoryB.registerMethodReceiver("myReplayer", new MyLogReplayer());
		transportFactory.start();
		proxyFactoryB.start();
		
		return proxyFactoryB.getRemoteService("myReplayer", LogRequestHandler.class, proxyFactoryB.getAddress().toString() );
	}

	private LogRequest makeRequest(String pattern) {
		LogRequest replayRequest = new LogRequest("subscriber", 0, toTimeMs);
		replayRequest.setSearch(true);
		Query query = new Query(1, 1, pattern, pattern, true);
		query.addFilter(new Contains("tag", Arrays.asList("stuff", "stuff2")));
		query.addFunction(new Average("tag", "100", "200"));
		query.addFunction(new Count("tag", "100", "200"));
		replayRequest.addQuery(query);
		replayRequest.addQuery(query);
		replayRequest.addQuery(query);
		replayRequest.addQuery(query);
		replayRequest.addQuery(query);
		return replayRequest;
	}
	
	public int searchCount;
	private TransportFactory transportFactory;
	public class MyLogReplayer implements LogRequestHandler {

		public String getId() {
			return "myReplayer";
		}

        @Override
        public void cancel(LogRequest request) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        public void replay(LogRequest request) {
		}

		public void search(LogRequest request) {
			//Thread.dumpStack();
			System.out.println(new Date() + "Got Replay Request:" + request);
			searchCount++;
			countDownLatch.countDown();
			
		}

		@Override
		public Map<String, Double> volumes() {
			return null;
		}
	}
	

}
