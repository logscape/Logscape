package com.liquidlabs.space.lease;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.liquidlabs.space.impl.SpaceFactory;

public class LeaseManagerTest  {
	
	
	private LeaseManagerImpl leaseManager;

	@Before
	public void setUp() throws Exception {
		leaseManager = new LeaseManagerImpl("stuff", new SpaceFactory(null, null).createSimpleSpace("partition"), null);
	}
	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void shouldOnlyRenewMyOwnLease() throws Exception {
		String writeKey = leaseManager.obtainWriteLease("key", "value", 1000);
		leaseManager.assignLeaseOwner(writeKey,"BundleSvcAlloc11000-envy14");
		
		int renewLeaseForOwner1 = leaseManager.renewLeaseForOwner("BundleSvcAlloc11000-envy14",-1);
		assertEquals(1, renewLeaseForOwner1);
		
		int renewLeaseForOwner = leaseManager.renewLeaseForOwner("BundleSvcAlloc12000-envy14",-1);
		assertEquals(0, renewLeaseForOwner);
		
	}
	
	@Test
	public void testShouldAssignLeaseItemToOwner() throws Exception {
		String writeKey = leaseManager.obtainWriteLease("key", "value", 1000);
		Lease lease = leaseManager.getLease(writeKey);
		assertNotNull(lease);
		assertEquals("", lease.owner);
		
		leaseManager.assignLeaseOwner(writeKey,"owner");
		Lease lease2 = leaseManager.getLease(writeKey);
		assertEquals("owner", lease2.owner);
	}
	
	@Test
	public void testShouldRenewForOwner() throws Exception {
		String writeKey = leaseManager.obtainWriteLease("key", "value", 1000);
		leaseManager.assignLeaseOwner(writeKey,"owner");
		long firstTimeout = leaseManager.getLease(writeKey).timeoutSeconds;
		int updateCount = leaseManager.renewLeaseForOwner("owner", 1100);
		long secondTimeout = leaseManager.getLease(writeKey).timeoutSeconds;
		assertEquals(1, updateCount);
		assertTrue(firstTimeout + 100 == secondTimeout);
	}
	
	@Test
	public void testShouldRenewMultipleForOwnerWithSameType() throws Exception {
		String lease1 = leaseManager.obtainWriteLease("key1", "value", 1000);
		leaseManager.assignLeaseOwner(lease1,"owner");
		
		String lease2 = leaseManager.obtainWriteLease("key2", "value", 1000);
		leaseManager.assignLeaseOwner(lease2, "owner");
		
		long firstTimeout = leaseManager.getLease(lease1).timeoutSeconds;
		int updateCount = leaseManager.renewLeaseForOwner("owner", 1100);
		long secondTimeout1 = leaseManager.getLease(lease1).timeoutSeconds;
		long secondTimeout2 = leaseManager.getLease(lease2).timeoutSeconds;
		assertEquals(2, updateCount);
		assertTrue(firstTimeout + 100 == secondTimeout1);
		assertTrue(firstTimeout + 100 == secondTimeout2);
	}
	
	@Test
	public void testShouldRenewMultipleForOwnerWithDiFFType() throws Exception {
		String lease1 = leaseManager.obtainWriteLease("key1", "value", 1000);
		leaseManager.assignLeaseOwner(lease1,"owner");
		
		String lease2 = leaseManager.obtainTakeLeaseTxn("key2", "value", 1000);
		leaseManager.assignLeaseOwner(lease2, "owner");
		
		long firstTimeout = leaseManager.getLease(lease1).timeoutSeconds;
		int updateCount = leaseManager.renewLeaseForOwner("owner", 1100);
		assertEquals(2, updateCount);
		long secondTimeout1 = leaseManager.getLease(lease1).timeoutSeconds;
		long secondTimeout2 = leaseManager.getLease(lease2).timeoutSeconds;
		assertEquals(firstTimeout + 100, secondTimeout1);
		assertEquals(firstTimeout + 100, secondTimeout2);
	}
	
	@Test
	public void testShouldRenewMultipleForOwnerWithDiFFType2() throws Exception {
		String lease1 = leaseManager.obtainWriteLease("key1", "value", 1000);
		leaseManager.assignLeaseOwner(lease1,"owner");
		
		String lease2 = leaseManager.obtainUpdateLease(new String[] { "one" }, new String[] { "value" }, 1000, "xxx");
		leaseManager.assignLeaseOwner(lease2, "owner");
		
		long firstTimeout = leaseManager.getLease(lease1).timeoutSeconds;
		int updateCount = leaseManager.renewLeaseForOwner("owner", 1100);
		long secondTimeout1 = leaseManager.getLease(lease1).timeoutSeconds;
		long secondTimeout2 = leaseManager.getLease(lease2).timeoutSeconds;
		assertEquals(2, updateCount);
		assertTrue(firstTimeout + 100 == secondTimeout1);
		assertTrue(firstTimeout + 100 == secondTimeout2);
	}

}
