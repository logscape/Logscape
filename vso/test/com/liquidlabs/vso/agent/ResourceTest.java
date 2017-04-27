package com.liquidlabs.vso.agent;

import com.liquidlabs.common.net.URI;
import java.util.concurrent.Executors;

import org.jmock.Mock;
import org.jmock.MockObjectTestCase;

import com.liquidlabs.space.ClientLeaseManager;
import com.liquidlabs.vso.resource.ResourceSpace;

public class ResourceTest extends MockObjectTestCase {

	
	private Resource resource;
	private ResourceProfile resourceProfile;
	private Mock resourceSpace;
	@Override
	protected void setUp() throws Exception {
		
		
		resourceProfile = new ResourceProfile();
		
		resourceSpace = mock(ResourceSpace.class);
		resourceSpace.stubs();
		
		ClientLeaseManager clientLeaseManager = new ClientLeaseManager();
		clientLeaseManager.setScheduler(Executors.newScheduledThreadPool(1));
		resource = new Resource(clientLeaseManager, resourceProfile, new URI("tcp://stuff"),0);
		resource.init((ResourceSpace) resourceSpace.proxy(), -1);
	}
	
	public void testStringFormat() throws Exception {
		System.out.println(String.format("%1.0f", 0.05000001));
	}
	
	
	public void testCRCShouldBeCool() throws Exception {
	
		long crc = resource.getCRC(resourceProfile);
		
		assertEquals(crc, resource.getCRC(resourceProfile));
	}
	
	public void testShouldAddAndRemoveWorkAssignmentFromResourceProfile() throws Exception {
		String id1 = ",alteredcarbon.local-12045:vs-log-1.0:LogTailer";
		String id2 = ",alteredcarbon.local-12045:ice-1.0:GridEngine";
		
		// PRIME it
		resource.updateResourceSpace(true);
		
		// TEST
		resourceProfile.addWorkAssignmentId(id1);
		assertTrue("Should have sent initial update", resource.updateResourceSpace(false));
		resourceProfile.addWorkAssignmentId(id2);
		assertTrue("Should have sent initial update", resource.updateResourceSpace(false));
		
	}

	public void testShouldALWAYSUpdateResourceProfileCPUWithChange() throws Exception {
		
		assertTrue("Should have sent initial update", resource.updateResourceSpace(false));
		
		resourceProfile.setCpuUtilisation(99);
		Thread.sleep(100);
		assertTrue("Should have sent an update for added bundle1", resource.updateResourceSpace(false));
		
		resourceProfile.setCpuUtilisation(98);
		Thread.sleep(100);
		assertTrue("Should have sent an update for added bundle1", resource.updateResourceSpace(false));
		
		resourceProfile.setCpuUtilisation(55);
		Thread.sleep(100);
		assertTrue("Should have sent an update for added bundle1", resource.updateResourceSpace(false));
		
		Thread.sleep(3000);
		assertFalse("Should NOT have sent an update for added bundle1", resource.updateResourceSpace(false));
		
	}
	
	public void testShouldNotUpdateResourceSpaceWithoutChange() throws Exception {
		
		assertTrue("Should have sent initial update", resource.updateResourceSpace(false));
		assertFalse("Nothing should have changed 1", resource.updateResourceSpace(false));
		Thread.sleep(5000);
		assertFalse("Nothing shouldhave changed 2", resource.updateResourceSpace(false));
		Thread.sleep(1000);
		
	}
	public void testShouldALWAYSUpdateResourceSpaceWithChange() throws Exception {
		
		assertTrue("Should have sent initial update", resource.updateResourceSpace(false));
		resource.addDeployedBundle("xxxx1");
		Thread.sleep(100);
		assertTrue("Should have sent an update for added bundle1", resource.updateResourceSpace(false));
		Thread.sleep(3000);
		assertFalse("Should NOT have sent an update for added bundle1", resource.updateResourceSpace(false));
		
	}
}
