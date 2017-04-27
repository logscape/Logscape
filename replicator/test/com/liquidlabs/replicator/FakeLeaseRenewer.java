/**
 * 
 */
package com.liquidlabs.replicator;

import com.liquidlabs.space.lease.LeaseRenewer;
import com.liquidlabs.space.lease.Renewer;

public class FakeLeaseRenewer implements LeaseRenewer {
	public void cancelLeaseRenewal(String leaseKey) {
	}
	public void add(Renewer leasor, int renewFrequency, String leaseKey) {
	}
}