package com.liquidlabs.space.lease;

public interface LeaseRenewer {

	void add(Renewer leasor, int renewFrequency, String leaseKey);

	void cancelLeaseRenewal(String leaseKey);

}
