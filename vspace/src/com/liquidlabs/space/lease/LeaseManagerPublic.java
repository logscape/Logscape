package com.liquidlabs.space.lease;


public interface LeaseManagerPublic {
	void renewLease(String leaseKey, long expires);
	void assignLeaseOwner(String leaseKey, String owner);
	int renewLeaseForOwner(String owner, int timeoutSeconds);
	void cancelLease(String leaseKey);
}
