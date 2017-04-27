package com.liquidlabs.vso.resource;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.jmock.Mock;
import org.jmock.MockObjectTestCase;

import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.agent.ResourceProfile;
import com.liquidlabs.vso.lookup.LookupSpace;

public class ResourceSpaceRequestTest extends MockObjectTestCase {
	
	
	private Mock lookupSpace;
	private ResourceSpaceImpl resourceSpace;
	private String location;
	private ScheduledExecutorService scheduler;

	protected void setUp() throws Exception {
		lookupSpace = mock(LookupSpace.class);
		lookupSpace.stubs();
		lookupSpace.stubs().method("unregisterService").will(returnValue(true));
		SpaceServiceImpl resService = new SpaceServiceImpl((LookupSpace) lookupSpace.proxy(),new ORMapperFactory(), ResourceSpace.NAME + "_RES", Executors.newScheduledThreadPool(2), false, false, false);
		SpaceServiceImpl allocService = new SpaceServiceImpl((LookupSpace) lookupSpace.proxy(),new ORMapperFactory(), ResourceSpace.NAME + "_ALLOC", Executors.newScheduledThreadPool(2), false, false, false);
		SpaceServiceImpl allResourcesEver = new SpaceServiceImpl((LookupSpace) lookupSpace.proxy(),new ORMapperFactory(), ResourceSpace.NAME + "_ALL", Executors.newScheduledThreadPool(2), false, false, true);

		resourceSpace = new ResourceSpaceImpl(resService, allocService, allResourcesEver);
		resourceSpace.start();
		
		
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
			resourceSpace.registerResource(profile, 180);
		}
	}
	
	public void testShouldHandleAllocating500Resources() throws Exception {
		createProfiles(80, 500);
		
//		T remoteService = proxyFactory.getRemoteService(serviceName, type, addresses, listener);
		
		for (int i = 0; i < 500; i++) {
			System.out.println(" ------------------------- Request:" + i);
			resourceSpace.requestResources("requestId", 1, 10, "mflops > 1000", "work", -1, "owner", "");
		}
		
	}
	
	public void testShouldAssignLowerPriorityResources() throws Exception {
		
		// SETUP
		createProfiles(80, 10);
		
		int satisfied = resourceSpace.requestResources("requestId", 1, 10, "mflops > 1000", "work", -1, "owner", "");
		
		assertEquals(0, satisfied);
	}

}
