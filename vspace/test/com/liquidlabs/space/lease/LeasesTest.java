package com.liquidlabs.space.lease;

import org.jmock.MockObjectTestCase;

import com.liquidlabs.space.Space;
import com.liquidlabs.space.lease.Leases.LeaseSpaceWriteLease;
import com.liquidlabs.space.lease.Leases.TakeLease;
import com.liquidlabs.space.lease.Leases.WriteLease;
import com.liquidlabs.transport.serialization.ObjectTranslator;

public class LeasesTest extends MockObjectTestCase {
	
	private Leases leases;
	String d = Space.DELIM;
	ObjectTranslator query = new ObjectTranslator();

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		leases = new Leases();
	}
	
	
	public void testLeaseDeserialisationWorks() throws Exception {
		String leaseString = query.getStringFromObject(new LeaseSpaceWriteLease("itemKey", "itemValue", 1000));
		
//		String leaseString = "com.liquidlabs.space.lease.Leases$LeaseSpaceWriteLease|stcp://alteredcarbon.local:15050-103|_URL_ENCODED__EVENT_%26stcp%3A%2F%2Falteredcarbon.local%3A15050-103%26com.liquidlabs.log.space.LogEvent-55ed3e0b-e115-40e0-a446-835d0190189d%26com.liquidlabs.log.space.LogEvent%40%2FUsers%2Fneil%2Fvscape-install%2FvscapeLU%2F.%2Fagent.log%40alteredcarbon.local%4055ed3e0b-e115-40e0-a446-835d0190189d%401668%401%40_URL_ENCODED_2008-10-24_PL%26US_15%253A26%253A29%252C645_PL%26US_WARN_PL%26US_pool-5-thread-2_PL%26US_%2528SpaceReaper.java%253A72%2529%2509_PL%26US_-_PL%26US_Failed_PL%26US_to_PL%26US_execute_PL%26US_lease%253AType_PL%26US_mismatch_PL%26US_between%255Bcom.liquidlabs.space.lease.Leases%2524WriteLease%255D_PL%26US_AND_PL%26US_%255Bcom.liquidlabs.space.lease.Leases%2524LeaseSpaceWriteLease%255D_PL%26US_RAW%255Bcom.liquidlabs.space.lease.Leases%2524LeaseSpaceWriteLease%257Cstcp%253A%252F%252Falteredcarbon.local%253A15005-189%257C_EVENT_%2526stcp%253A%252F%252Falteredcarbon.local%253A15005-189%2526com.liquidlabs.vso.agent.ResourceProfile-alteredcarbon.local-12050-1%2526com.liquidlabs.vso.agent.ResourceProfile%2540%2540%2540altere%255D%40stcp%3A%2F%2Falteredcarbon.local%3A12000%40%26WRITE%26stcp%3A%2F%2Falteredcarbon.local%3A15050|_lease_-stcp://alteredcarbon.local:15050-103LeaseSpaceWriteLease|_lease_|LeaseSpaceWriteLease|1224858401|_lease_|";
		Lease leaseForValues = leases.getLeaseForValues(leaseString);
		assertNotNull(leaseForValues);
		
		
	}
	
	public void testWriteLeaseExpiredIsLookedUp() throws Exception {
		
		WriteLease writeLease = new WriteLease("itemKey", "itemValue", 100);
		String writeLeaseString = query.getStringFromObject(writeLease);
		
		
		Lease lease = leases.getLeaseForValues(writeLeaseString);
		assertTrue("lease is not correct type", lease instanceof Leases.WriteLease);
		assertEquals(Leases.WriteLease.TYPE, lease.leaseType);
		assertEquals("itemKey", lease.itemKey);
		assertEquals("itemValue", lease.itemValue);
		
	}
	public void testTakeLeaseConversion() throws Exception {
		
		TakeLease takeLease = new TakeLease("itemKey", "itemValue", 100);
		String leaseString = query.getStringFromObject(takeLease);
		
		
		Lease lease = leases.getLeaseForValues(leaseString);
		assertTrue("lease is not correct type", lease instanceof Leases.TakeLease);
		assertEquals(Leases.TakeLease.TYPE, lease.leaseType);
		assertEquals("itemKey", lease.itemKey);
		assertEquals("itemValue", lease.itemValue);
	}
	public void testTakeLeaseCreationFromExistingItem() throws Exception {
		TakeLease lease = new Leases.TakeLease("itemKey","itemValue1,itemValue2", 100);
		assertEquals("itemValue1,itemValue2", lease.getItemValue());
		System.out.println("LeaseLooksLike:" + lease.toString());		
	}
	public void testWriteLeaseCreationFromExistingItem() throws Exception {
		WriteLease lease = new Leases.WriteLease("itemKey","itemValue1,itemValue2", 100);
		assertEquals("itemKey", lease.getItemKey());
		assertEquals("itemValue1,itemValue2", lease.getItemValue());
		System.out.println("LeaseLooksLike:" + lease.toString());		
	}

}
