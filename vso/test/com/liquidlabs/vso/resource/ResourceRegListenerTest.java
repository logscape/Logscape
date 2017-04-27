package com.liquidlabs.vso.resource;

import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.space.lease.Lease;
import com.liquidlabs.vso.FunctionalTestBase;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.agent.ResourceAgentImpl;
import com.liquidlabs.vso.agent.ResourceProfile;
import com.liquidlabs.vso.lookup.LookupSpace;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.jmock.Mock;
import org.jmock.MockObjectTestCase;

public class ResourceRegListenerTest extends MockObjectTestCase  {
	
	private int registerCount;
	private int unregisterCount;
	String resourceId = "";
	private Mock lookupSpace;
	private ResourceSpaceImpl resourceSpace;
	
	protected void setUp() throws Exception {
		
		// make the reaper execute every second
		System.setProperty(Lease.PROPERTY, "1");
		lookupSpace = mock(LookupSpace.class);
		lookupSpace.stubs();
		lookupSpace.stubs().method("unregisterService").will(returnValue(true));
		ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
		SpaceServiceImpl resService = new SpaceServiceImpl((LookupSpace) lookupSpace.proxy(),new ORMapperFactory(), ResourceSpace.NAME + "_RES", scheduler, false, false, false);
		SpaceServiceImpl allocService = new SpaceServiceImpl((LookupSpace) lookupSpace.proxy(),new ORMapperFactory(), ResourceSpace.NAME + "_ALLOC", scheduler, false, false, false);
		SpaceServiceImpl allResourcesEver = new SpaceServiceImpl((LookupSpace) lookupSpace.proxy(),new ORMapperFactory(), ResourceSpace.NAME + "_ALL", scheduler, false, false, true);

		resourceSpace = new ResourceSpaceImpl(resService, allocService, allResourcesEver);
		resourceSpace.start();
		
	}
	protected void tearDown() throws Exception {
		resourceSpace.stop();
	}
	
	public void testShouldReceiveEvents() throws Exception {
		
		ResourceRegisterListener rrListener = new ResourceRegisterListener(){
			public void register(String aResourceId, ResourceProfile resourceProfile) {
				System.out.println(getName() + " ************* register called!:" + aResourceId);
				registerCount++;
				resourceId = aResourceId;
			}
			public void unregister(String aResourceId, ResourceProfile resourceProfile) {
				System.out.println(getName() + " ************** unregister called!");
				unregisterCount++;
				resourceId = aResourceId;
			}
			public String getId() {
				return "RRListener";
			}
		};
		resourceSpace.registerResourceRegisterListener(rrListener , "myListener", "", -1);
		
		Thread.sleep(100);
		// now register a resource and check out callback is called
		ResourceProfile profile = new ResourceProfile();
		profile.setResourceId("rreg");
		resourceSpace.registerResource(profile, 10);
		
		Thread.sleep(100);
		
		assertTrue("Register Count NOT 1, was:" + registerCount, registerCount > 0);
		assertEquals(resourceId, profile.getResourceId());
	}
	
	
	public void testShouldReceiveNotReceiveEventsWhenExpired() throws Exception {
	    final CountDownLatch latch = new CountDownLatch(2);
		
		ResourceRegisterListener rrListener = new ResourceRegisterListener(){
			public void register(String aResourceId, ResourceProfile resourceProfile) {
				latch.countDown();
				resourceId = aResourceId;
			}
			public void unregister(String aResourceId, ResourceProfile resourceProfile) {
				System.out.println(getName() + " ************** unregister called!");
				unregisterCount++;
				resourceId = aResourceId;
			}
			public String getId() {
				return "RRListener";
			}
		};
		
		// fire 2 events
		resourceSpace.registerResourceRegisterListener(rrListener , "myListener", "", 2);
		
		Thread.sleep(100);
		
		// now register a resource and check out callback is called
		ResourceProfile prof1 = new ResourceProfile();
		prof1.setResourceId("1");
		ResourceProfile prof2 = new ResourceProfile();
		prof2.setResourceId("2");
		
		Thread.sleep(100);
		
		resourceSpace.registerResource(prof1, 10);
		resourceSpace.registerResource(prof2, 10);
		
		Thread.sleep(100);

		assertTrue(latch.await(10, TimeUnit.SECONDS));
	}
}
