package com.liquidlabs.space.lease;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.liquidlabs.space.SLoggerConfig;
import com.liquidlabs.space.Space;
import com.liquidlabs.space.VSpaceProperties;
import com.liquidlabs.space.lease.Leases.TakeLease;
import com.liquidlabs.space.lease.Leases.UpdateLease;
import com.liquidlabs.space.lease.Leases.WriteLease;
import com.liquidlabs.transport.serialization.ObjectTranslator;

public class LeaseManagerImpl implements LeaseManager {
	public static long CLOCK_DRIFT = 0;
	private static final Logger LOGGER = Logger.getLogger(LeaseManagerImpl.class);
	DateTimeFormatter longTime = DateTimeFormat.mediumTime();
	
	private final Space leasedSpace;
	Leases leases = new Leases();
	ObjectTranslator query = new ObjectTranslator();
	private final String partitionName;
	private final SpaceReaper spaceReaper;

	public LeaseManagerImpl(String partitionName, Space leasedSpace, SpaceReaper spaceReaper) {
		this.partitionName = partitionName;
		this.leasedSpace = leasedSpace;
		this.spaceReaper = spaceReaper;
	}
	
	public static void setClockDrift(long delta) {
		CLOCK_DRIFT = delta;
	}
	public boolean shouldObtainLease(long timeoutSeconds) {
		return timeoutSeconds >= 0 && timeoutSeconds <  Integer.MAX_VALUE;
	}
	
	public void createLease(String leaseKey, Lease leaseObject, long timeout){
		writeLease(leaseObject);
	}
	
	public int purgeForItem(String key) {
		String expression = String.format("itemKey equals '%s'", key);
		String[] writeTemplate = query.getQueryStringTemplate(WriteLease.class, expression);
		 String[] takeTemplate = query.getQueryStringTemplate(TakeLease.class, expression);
		 String[] updateTemplate = query.getQueryStringTemplate(UpdateLease.class, expression);
		 String[] takeMultiple = leasedSpace.takeMultiple(new String[] { writeTemplate[0], takeTemplate[0], updateTemplate[0] }, -1, -1, -1);
		 return takeMultiple.length;
	}
	public int purgeForItems(List<String> key) {
		if (key.size() == 0) return 0;
		String keys = com.liquidlabs.common.collection.Arrays.toString(key);
		String[] writeTemplate = query.getQueryStringTemplate(WriteLease.class, "itemKey equalsAny " + keys);
		String[] takeTemplate = query.getQueryStringTemplate(TakeLease.class, "itemKey equalsAny " + keys);
		String[] updateTemplate = query.getQueryStringTemplate(UpdateLease.class, "itemKey equalsAny " + keys);
		String[] takeMultiple = leasedSpace.takeMultiple(new String[] { writeTemplate[0], takeTemplate[0], updateTemplate[0] }, -1, -1, -1);
		return takeMultiple.length;
		
	}
	
	public void cancelLease(String leaseKey) {
		try {
			String[] take = leasedSpace.take(new String[] { leaseKey });
			if (LOGGER.isDebugEnabled()) LOGGER.debug(SLoggerConfig.VS_LEASE + "CancelLease[" + leaseKey + "] itemCount:" + take.length + " item:" + take[0]);
		} catch (Throwable t) {
			if (t.getMessage().contains("NULL")) {
				LOGGER.warn("CancelLease on Key error:" + leaseKey + " msg:" + t.getMessage());
			} else {
				throw new RuntimeException(t);
			}
		}
	}
	public void renewLease(String leaseKey, long expires) {
		Lease lease = getLease(leaseKey);
		if (lease == null) {
//			LOGGER.error("Missing:" + leaseKey + " LEASE KEYS>>>>>>>>" + leasedSpace.keySet());
			throw new RuntimeException(String.format("Failed to find lease for key [%s] Space[%s] SpaceKeysCount[%d]", leaseKey, partitionName, leasedSpace.keySet().size()));
		}

		lease.setTimeoutFromNow(expires);
		
		if (LOGGER.isDebugEnabled()) LOGGER.debug(SLoggerConfig.VS_LEASE + "RenewLease:" + leaseKey + " new Timeout:" + lease.timeoutSecondsString);
		writeLease(lease);
	}

	public Lease getLease(String leaseKey) {
		if (leaseKey == null) return null;
		String leaseValue = leasedSpace.read(leaseKey);
		if (leaseValue == null) return null;
		return leases.getLeaseForValues(leaseValue);
	}
	public String obtainNotifyLease(String id, long expires) {
		if (!shouldObtainLease(expires)) return "none";
		Lease lease = new Leases.NotifyLease(id, "notifyLeaseValue", expires);
		writeLease(lease);
		
		if (LOGGER.isDebugEnabled()) LOGGER.debug(SLoggerConfig.VS_LEASE + " ObtainNotifyLease:" + lease.getLeaseKey() + " exp:" + lease.timeoutSecondsString);
		return lease.getLeaseKey();
	}

	public String obtainWriteLease(String key, String value, long timeoutSeconds) {
		
		if (!shouldObtainLease(timeoutSeconds)) return null;
		// taking out a new lease, wipe out preexisting
		if (timeoutSeconds > 5) purgeLeasesAgainstItem(key);
		
		
		Lease lease = new Leases.WriteLease(key, value, timeoutSeconds);
		writeLease(lease);
		if (LOGGER.isDebugEnabled()) LOGGER.debug(SLoggerConfig.VS_LEASE + "ObtainWriteLease:" + lease.getLeaseKey() + " exp:" + lease.timeoutSecondsString);
		return lease.getLeaseKey();
	}
	public String obtainLeaseWriteLease(String key, String value, long timeoutSeconds) {
		
		if (!shouldObtainLease(timeoutSeconds)) return null;
		
		// taking out a new lease, wipe out preexisting
		purgeLeasesAgainstItem(key);
		
		Lease lease = new Leases.LeaseSpaceWriteLease(key, value, timeoutSeconds);
		writeLease(lease);
		if (LOGGER.isDebugEnabled()) LOGGER.debug(SLoggerConfig.VS_LEASE + "ObtainLeaseWriteLease:" + lease.getLeaseKey() + " exp:" + lease.timeoutSecondsString);
		return lease.getLeaseKey();
	}

	public String obtainTakeLeaseTxn(String key, String value, long timeoutSeconds) {

		if (!shouldObtainLease(timeoutSeconds)) return null;
		
		// taking out a new lease, wipe out preexisting
		int itemsRemoved = purgeLeasesAgainstItem(key);
		
		Lease lease = new Leases.TakeLease(key, value, timeoutSeconds);
		writeLease(lease);
		if (LOGGER.isDebugEnabled()) LOGGER.debug(SLoggerConfig.VS_LEASE + "ObtainTakeLease:" + lease.getLeaseKey() + " exp:" + lease.timeoutSecondsString);
		return lease.getLeaseKey();
	}


	public String obtainUpdateLease(String[] keys, String[] values, long timeoutSeconds, String leaseKey) {

		if (!shouldObtainLease(timeoutSeconds)) return null;
		
		// taking out a new lease, wipe out preexisting
		for (String key : keys) {
			int itemsRemoved = purgeLeasesAgainstItem(key);
		}
	
		if (keys.length == 0 && values.length > 0) {
			LOGGER.warn("Got 0 length keys, value:"+ values[0]);
		}
		if (values.length == 0 && keys.length > 0) {
			LOGGER.warn("Got 0 length value, lkey:"+ keys[0]);
		}
		
		Lease lease = new Leases.UpdateLease(keys, values, timeoutSeconds);
		
		String aleaseKey = leaseKey == null ? lease.getLeaseKey() : leaseKey;
		lease.setLeaseKey(aleaseKey);
		
		writeLease(lease);
		if (LOGGER.isDebugEnabled()) LOGGER.debug(SLoggerConfig.VS_LEASE + "ObtainUpdateLease:" + lease.getLeaseKey() + " exp:" + longTime.print(lease.getTimeoutSeconds() * 1000));
		
		
		return lease.getLeaseKey();
	}
	private void writeLease(Lease lease) {
		if (lease.duration < VSpaceProperties.getShortLeaseThresholdSeconds()) {
			spaceReaper.addImmediateLease(lease);
		} else {
			leasedSpace.write(lease.getLeaseKey(), query.getStringFromObject(lease), -1);
		}
	}
	private int purgeLeasesAgainstItem(String key) {

		String expession = String.format("itemKey equals '%s'", key);
		String[] writeTemplate = query.getQueryStringTemplate(WriteLease.class, expession);
		String[] takeTemplate = query.getQueryStringTemplate(TakeLease.class, expession);
		String[] updateTemplate = query.getQueryStringTemplate(UpdateLease.class, expession);
		String[] takeMultiple = leasedSpace.takeMultiple(new String[] { writeTemplate[0], takeTemplate[0], updateTemplate[0] }, -1, -1, -1);
		return takeMultiple.length;
	}
	private String dumpLeaseKeyWithContains(String contains) {
		int i = 0;
		StringBuilder resultString = new StringBuilder();
		String[] keys = leasedSpace.getKeys(new String[] { "all:" }, -1);
		for (String string : keys) {
			if (string.contains(contains)) {
				LOGGER.info(i++ + " - " + string + leasedSpace.read(string));
				resultString.append(string).append(",");
			}
		}
		return resultString.toString();
	}
	public void assignLeaseOwner(String leaseKey, String owner) {
		if (leaseKey == null) return;
		Lease lease = getLease(leaseKey);
		lease.owner = owner;
		writeLease(lease);
	}
	public int renewLeaseForOwner(String owner, int timeout) {
		String expression = String.format("owner equals '%s'", owner);
		 String[] writeTemplate = query.getQueryStringTemplate(WriteLease.class, expression);
		 String[] takeTemplate = query.getQueryStringTemplate(TakeLease.class, expression);
		 String[] updateTemplate = query.getQueryStringTemplate(UpdateLease.class, expression);
		 String[] rawLeases = leasedSpace.readMultiple(new String[] { writeTemplate[0], takeTemplate[0], updateTemplate[0] }, -1);
		 int count = 0;
		 for (String leaseValue : rawLeases) {
			 Lease lease = leases.getLeaseForValues(leaseValue);
			 lease.setTimeoutFromNow(timeout);
			 writeLease(lease);
			 count++;
		 }
		 return count;
	}
	public List<String> getLeaseKeysForOwner(String owner) {
		String expression = String.format("owner equals '%s'", owner);
		 String[] writeTemplate = query.getQueryStringTemplate(WriteLease.class, expression);
		 String[] takeTemplate = query.getQueryStringTemplate(TakeLease.class, expression);
		 String[] updateTemplate = query.getQueryStringTemplate(UpdateLease.class, expression);
		 String[] rawLeases = leasedSpace.readMultiple(new String[] { writeTemplate[0], takeTemplate[0], updateTemplate[0] }, -1);
		 
		 ArrayList<String> results = new ArrayList<String>();
		 int count = 0;
		 for (String leaseValue : rawLeases) {
			 Lease lease = leases.getLeaseForValues(leaseValue);
			 count++;
			 results.add(lease.getLeaseKey());
		 }
		 return results;
	}
}
