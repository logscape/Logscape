package com.liquidlabs.space.lease;

public interface Leasor {
	public void renewLease(String leaseKey, int expires) throws Exception;
	public void cancelLease(String leaseKey);
}
