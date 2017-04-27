package com.liquidlabs.space.lease;

import java.util.List;

public interface LeaseManager extends LeaseManagerPublic {
	String obtainWriteLease(String key, String value, long timeoutSeconds);
	String obtainNotifyLease(String key, long expires);
	String obtainTakeLeaseTxn(String key, String value, long timeoutSeconds);
	String obtainLeaseWriteLease(String key, String value, long timeoutSeconds);
	String obtainUpdateLease(String[] strings, String[] strings2, long timeoutSeconds, String leaseKey);
	int purgeForItem(String key);
	int purgeForItems(List<String> key);
	List<String> getLeaseKeysForOwner(String owner);

}
