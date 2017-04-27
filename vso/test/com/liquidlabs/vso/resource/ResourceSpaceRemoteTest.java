package com.liquidlabs.vso.resource;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.jmock.Mock;
import org.jmock.MockObjectTestCase;
import org.joda.time.DateTimeUtils;
import org.joda.time.format.DateTimeFormat;

import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.transport.proxy.ProxyFactoryImpl;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.agent.ResourceProfile;
import com.liquidlabs.vso.lookup.LookupSpace;

public class ResourceSpaceRemoteTest extends MockObjectTestCase {
	
	
	private Mock lookupSpace;
	private ResourceSpaceImpl resourceSpace;
	public ProxyFactoryImpl proxyFactory;
	private ResourceSpace remoteResourceSpace;
	private ScheduledExecutorService scheduler;

	protected void setUp() throws Exception {
		lookupSpace = mock(LookupSpace.class);
		lookupSpace.stubs();
		lookupSpace.expects(atLeastOnce()).method("registerUpdateListener");
		lookupSpace.expects(atLeastOnce()).method("registerService");
		
		ORMapperFactory mapperFactory = new ORMapperFactory();
		SpaceServiceImpl resService = new SpaceServiceImpl((LookupSpace) lookupSpace.proxy(),mapperFactory, ResourceSpace.NAME + "_RES", Executors.newScheduledThreadPool(2), false, false, false);
		SpaceServiceImpl allocService = new SpaceServiceImpl((LookupSpace) lookupSpace.proxy(),mapperFactory, ResourceSpace.NAME + "_ALLOC", Executors.newScheduledThreadPool(2), false, false, false);
		SpaceServiceImpl allResourcesEver = new SpaceServiceImpl((LookupSpace) lookupSpace.proxy(),new ORMapperFactory(), ResourceSpace.NAME + "_ALL", Executors.newScheduledThreadPool(2), false, false, true);

		resourceSpace = new ResourceSpaceImpl(resService, allocService, allResourcesEver);
		mapperFactory.getProxyFactory().registerMethodReceiver(ResourceSpace.NAME, resourceSpace);

		resourceSpace.start();
		
		lookupSpace.expects(once()).method("getServiceAddresses").withAnyArguments().will(returnValue(new String[] { resService.getClientAddress().toString() }));
		
		ORMapperFactory mapperFactory2 = new ORMapperFactory();
		proxyFactory = mapperFactory2.getProxyFactory();
		proxyFactory.start();
		
		try {
			remoteResourceSpace = ResourceSpaceImpl.getRemoteService("TestSetup", (LookupSpace) lookupSpace.proxy(), proxyFactory);
		} catch (Throwable e) {
			e.printStackTrace();
		}
		
		System.out.println("=========================== " + getName() + " =======================================");
	}
	protected void tearDown() throws Exception {
		resourceSpace.stop();
	}
	
	
	private void createProfiles(int port, int count) throws Exception {
		for(int i = 0; i<count;i++) {
			ResourceProfile profile = new ResourceProfile("url", i, scheduler);
			profile.setHostName("localhost");
			profile.setPort(port);
			profile.setDeployedBundles("myBundle");
			resourceSpace.registerResource(profile, -1);
		}
	}
	
	public CountDownLatch countDownLatch;
	public void testShouldHandleAllocating500Resources() throws Exception {
		final int count = 100;
		createProfiles(80, count);
		countDownLatch = new CountDownLatch(count);
		
		RemoteAllocListener allocListener = new RemoteAllocListener("ALLOC", proxyFactory, countDownLatch);
		System.err.println("=============== waiting ===========");
		//Thread.sleep(60 * 1000);
		
		String lease = remoteResourceSpace.registerAllocListener(allocListener, "ALLOC", "ALLOC");
		ExecutorService executor = com.liquidlabs.common.concurrent.ExecutorService.newFixedThreadPool(10, "SENDER");
		for (int i = 0; i < count; i++) {
			final int c = i;
			Runnable task = new Runnable() {
				public void run() {
						System.out.println(" ------------------------- Request:" + c);
						remoteResourceSpace.requestResources("requestId", 1, 10, "mflops > 0", "work", -1, "ALLOC", "");
				}
			};
			executor.submit(task);
		}
		
		
		boolean await = countDownLatch.await(60, TimeUnit.SECONDS);
		
		assertTrue("Didnt get all requests, countDown got to:" + countDownLatch, await);
		
	}
	
	public void testShouldHandleHandleReturning500Resources() throws Exception {
		int count = 500;
		createProfiles(80, count);
	
		remoteResourceSpace.requestResources("requestId", count, 10, "mflops > 0", "work", -1, "ALLOC", "");
		
		Thread.sleep(1000);
		
		long last = DateTimeUtils.currentTimeMillis();
		List<String> findResourceIdsBy = remoteResourceSpace.findResourceIdsBy("");
		for (String string : findResourceIdsBy) {
			ResourceProfile resourceDetails = remoteResourceSpace.getResourceDetails(string);
			long now = DateTimeUtils.currentTimeMillis();
			System.out.println(DateTimeFormat.longTime().print(DateTimeUtils.currentTimeMillis()) + " Got:" + resourceDetails.getId() + " elapsed:" + (now - last));
			last = now;
		}
		
		
	}
	
	
	
	public static class RemoteAllocListener implements AllocListener {
		int addCount = 0;
		int releaseCount = 0;
		int removeCount = 0;
		private final ProxyFactoryImpl pf;
		private final String name;
		private final CountDownLatch countDownLatch;
		
		public RemoteAllocListener(String name, ProxyFactoryImpl pf, CountDownLatch countDownLatch) {
			this.name = name;
			this.pf = pf;
			this.countDownLatch = countDownLatch;
		}

		public void add(String requestId, List<String> resourceIds, String owner, int priority) {
			addCount++;
			System.out.println("Add:" + addCount);
			countDownLatch.countDown();
			
		}

		public List<String> release(String requestId, List<String> resourceIds, int releaseCount) {
			releaseCount++;
			return null;
		}

		public void take(String requestId, String owner, List<String> resourceIds) {
			removeCount++;
		}

		public void pending(String requestId, List<String> resourceIds, String owner, int priority) {
		}

		public void satisfied(String requestId, String owner, List<String> resourceIds) {
		}

		public String getId() {
			return name;
		}
	}

}
