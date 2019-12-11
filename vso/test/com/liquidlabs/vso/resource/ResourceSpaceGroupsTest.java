package com.liquidlabs.vso.resource;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.jmock.Mock;
import org.jmock.MockObjectTestCase;

import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.agent.ResourceProfile;
import com.liquidlabs.vso.lookup.LookupSpace;

public class ResourceSpaceGroupsTest extends MockObjectTestCase {
	
	
	private Mock lookupSpace;
	private ResourceSpaceImpl resourceSpace;
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

        createProfiles(999,100);
	}
	protected void tearDown() throws Exception {
		Thread.sleep(200);
		resourceSpace.stop();
	}
	
	public void testShouldStoreUpdateAndRemoveResourceGroup() throws Exception {
		List<ResourceGroup> resourceGroups1 = resourceSpace.getResourceGroups();
		String groupName = "rg 1";
		resourceSpace.registerResourceGroup(new ResourceGroup(groupName,"mflops > 0", "crap", "now"));
		
		List<ResourceGroup> resourceGroups2 = resourceSpace.getResourceGroups();
		
		assertTrue(resourceGroups2.size() == resourceGroups1.size() + 1);
		
		resourceSpace.registerResourceGroup(new ResourceGroup(groupName,"mflops == 0", "crap", "now"));
		
		ResourceGroup group = resourceSpace.findResourceGroup(groupName);
		assertEquals("mflops == 0", group.getResourceSelection());
		
		resourceSpace.unRegisterResourceGroup(groupName);
		
		assertNull(resourceSpace.findResourceGroup(groupName));
		
	}


    public void testShouldGetMutliHostsBloom() throws Exception {
        resourceSpace.registerResourceGroup(new ResourceGroup("CC","mflops > 150","does stuff", "now"));
        resourceSpace.registerResourceGroup(new ResourceGroup("DD","mflops < 150","does stuff", "now"));

//        String multi = "hosts:AA,hosts:BB,group:CC,group:DD";

        BloomMatcher bloom = resourceSpace.expandGroupIntoBloomFilter("hosts:2,4,.*10.*,hosts:.*3.*,group:CC");
        assertTrue(bloom.isMatch("localhost-2"));
        assertTrue(bloom.isMatch("localhost-60"));
        assertTrue(bloom.isMatch("localhost-3"));
        assertTrue(bloom.isMatch("localhost-10"));
        assertFalse(bloom.isMatch("localhost-1"));

    }



    public void testShouldGetHostsBloom() throws Exception {
        resourceSpace.registerResourceGroup(new ResourceGroup("myGroup","mflops > 150","does stuff", "now"));
        BloomMatcher bloom = resourceSpace.expandGroupIntoBloomFilter("group:myGroup");
        assertTrue(bloom.isMatch("localhost-60"));
        assertFalse(bloom.isMatch("localhost-1"));

    }
//    TODO - fix test
//    public void testShouldGetHostsBloomHostsString() throws Exception {
//        resourceSpace.registerResourceGroup(new ResourceGroup("myGroup","mflops > 150","does stuff", "now"));
//        BloomMatcher bloom = resourceSpace.expandGroupIntoBloomFilter("hosts:localhost-6,localhost-5");
//        assertNotNull(bloom);
//        assertTrue(bloom.isMatch("localhost-60"));
//        assertTrue(bloom.isMatch("localhost-50"));
//        assertFalse(bloom.isMatch("localhost-1"));
//
//    }
    public void testShouldGetHostsList() throws Exception {
        resourceSpace.registerResourceGroup(new ResourceGroup("myGroup","mflops > 150","does stuff", "now"));
        Set<String> hosts = resourceSpace.expandGroupIntoHostnames("myGroup");
        System.out.println("Hosts:" + hosts.size());
        assertTrue(hosts.size() > 0);
    }


    public void testShouldExpandResourceGroupIntoSelectionString() throws Exception {
		resourceSpace.registerResourceGroup(new ResourceGroup("myGroup","mflops > 0 AND resourceId containsAny 'one-bit,two'","does stuff", "now"));
		String expandedGroup = resourceSpace.expandResourceGroups("group('myGroup') AND mflop > 0");
		System.err.println(expandedGroup);
		assertTrue(expandedGroup.contains("containsAny 'one-bit,two'"));
	}	
	
	
	private void createProfiles(int port, int count) throws Exception {
		for(int i = 0; i<count;i++) {
			ResourceProfile profile = new ResourceProfile("url", i, scheduler);
			profile.setHostName("localhost-" + i);
			profile.setPort(port);
			profile.setDeployedBundles("myBundle");
            profile.setMflops(100 + i);
			resourceSpace.registerResource(profile, 180);
		}
	}
}
