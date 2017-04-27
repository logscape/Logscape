package com.liquidlabs.space.lease;

import org.joda.time.format.DateTimeFormat;

import com.liquidlabs.space.Space;
import com.liquidlabs.transport.serialization.Convertor;

/**
 * Stored in the space to determine when an item has expired
 */
public abstract class Lease {
	
	static Convertor convertor = new Convertor();
	
	public static long LEASE_KEY = System.currentTimeMillis();
	public static final String KEY = "_lease_";
	public static final String PROPERTY = "vscape.lease.interval";
	public static final Integer DEFAULT_INTERVAL = 3;
	String leaseToken = KEY;
	
	String leaseKey;
	long timeoutSeconds;
	long duration;
	String timeoutSecondsString;
	String itemKey;
	String leaseType;
	String type = KEY;
	
	String itemValue = "";
	public String owner = "";
	
	public Lease(){
	}
	
	/**
	 * Generic constructor that calculated expired time from "now"
	 * Should be used when a Lease is initially created and the time calculation is required
	 * @param itemKey
	 * @param itemValue
	 * @param timeout
	 * @param leaseType
	 */
	public Lease(String itemKey, String itemValue, long timeout, String leaseType) {
		this.itemKey = itemKey;
		this.itemValue = itemValue.replaceAll(Space.DELIM,"@");
		setTimeoutFromNow(timeout);
		this.leaseType = leaseType;
		this.duration = timeout;
		setLeaseKey();
	}
	@Override
	public String toString() {
		return String.format("Lease Type[%s] LeaseKey[%s] ItemKey[%s] Owner[%s] Duration[%d] TimeoutSeconds[%s]", type, leaseKey, itemKey, owner, duration, timeoutSecondsString);
	}

	public void setTimeoutFromNow(long timeout) {
		Long expires = System.currentTimeMillis()/1000 + timeout;
		// ensure value hasnt wrapped
		if (expires < 0 && timeout > 0) {
			expires = timeout;
		}
		this.timeoutSeconds = expires;
		this.timeoutSecondsString = "expiresAt=" + DateTimeFormat.shortDateTime().print(expires * 1000);
	}
	public long getDuration() {
		return duration;
	}
	
	public abstract void execute(Space dataSpace, Space leaseSpace);
	
	private void setLeaseKey() {
		this.leaseKey = getLeaseKey(itemKey);
	}
	public void setLeaseKey(String key){
		this.leaseKey = KEY + "-" + key;
	}
	public String getLeaseKey(String key){
		return KEY + "-" + key + leaseType;
	}

	public String getToken() {
		return leaseToken;
	}
	public void setToken(String token) {
		this.leaseToken = token;
	}
	public Long getTimeoutSeconds() {
		return timeoutSeconds;
	}
	public String getItemKey() {
		return itemKey;
	}
	public String getItemValue() {
		return itemValue.replaceAll("@", Space.DELIM);
	}

	public String getLeaseKey() {
		return leaseKey;
	}

}
