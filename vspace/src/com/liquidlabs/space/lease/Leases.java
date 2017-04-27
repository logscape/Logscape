package com.liquidlabs.space.lease;

import java.util.HashMap;
import java.util.Map;

import com.liquidlabs.space.Space;
import com.liquidlabs.transport.serialization.Convertor;
import com.liquidlabs.transport.serialization.ObjectTranslator;

public class Leases {
	public Map<String, Lease> leases = new HashMap<String, Lease>(){
		private static final long serialVersionUID = 1L;
	{
		put(TakeLease.TYPE, new TakeLease());
		put(NotifyLease.TYPE, new NotifyLease());
		put(WriteLease.TYPE, new WriteLease());		
		put(UpdateLease.TYPE, new UpdateLease());		
		put(LeaseSpaceWriteLease.TYPE, new LeaseSpaceWriteLease());
	}};
	ObjectTranslator query = new ObjectTranslator();
	Convertor convertor = new Convertor();
	
	@SuppressWarnings("unchecked")
	public Lease getLeaseForValues(String values){
		Class type = Lease.class;
		if (values == null) throw new RuntimeException("Given 'null' value = for lease");
		if (values.startsWith(TakeLease.class.getName())) {
			type = TakeLease.class;
		} 
		if (values.startsWith(NotifyLease.class.getName())) {
			type = NotifyLease.class;
		} 
		if (values.startsWith(LeaseSpaceWriteLease.class.getName())) {
			type = LeaseSpaceWriteLease.class;
		}
		if (values.startsWith(WriteLease.class.getName())) {
			type = WriteLease.class;
		} 
		if (values.startsWith(UpdateLease.class.getName())) {
			type = UpdateLease.class;
		} 
		return (Lease) query.getObjectFromFormat(type, values);
	}
	
	public static class UpdateLease extends Lease {
		public static String TYPE = "UpdateLease";
		private String[] allKeys;
		private String[] allValues;

		public UpdateLease() {
		}
		public UpdateLease(String[] allKeys, String[] allVaues, long timeoutSeconds) {
			super(allKeys[0], allVaues[0], timeoutSeconds, TYPE);
			this.allKeys = allKeys;
			this.allValues = allVaues;
		}


		/**
		 * Rollback the contents back into the dataSpace and remove the Lease
		 */
		public void execute(Space dataSpace, Space leaseSpace) {
			for(int i = 0; i < allKeys.length ; i ++){
				dataSpace.write(allKeys[i], allValues[i], -1);
			}
			leaseSpace.take(this.getLeaseKey(), -1, -1);
		}
	}
	
	public static class TakeLease extends Lease {
		public static String TYPE = "TakeLease";
		public TakeLease(){
			this.leaseType = TYPE;
		}
		/**
		 * CTor that accounts for the time offset calculation 
		 * @param itemValue 
		 */
		public TakeLease(String itemKey, String itemValue, long timeoutSeconds) {
			super(itemKey, itemValue, timeoutSeconds, TYPE);
		}
		
		/**
		 * Rollback the contents back into the dataSpace and remove the take from the leaseSpace
		 */
		public void execute(Space dataSpace, Space leaseSpace) {
			dataSpace.write(getItemKey(), getItemValue(), -1);
			leaseSpace.take(this.getLeaseKey(), -1, -1);
		}
	}
	public static class WriteLease extends Lease {
		public static String TYPE = "WriteLease";
		public WriteLease(){
			this.leaseType = TYPE;
		}
		public WriteLease(String itemKey, String itemValue, long timeoutSeconds) {
			super(itemKey, itemValue, timeoutSeconds, TYPE);
		}
		
		/**
		 * fires when a write has expired and removes the data and its lease.
		 */
		public void execute(Space dataSpace, Space leaseSpace) {
			dataSpace.take(getItemKey(), -1, -1);
			leaseSpace.take(this.getLeaseKey(), -1, -1);
		}
	}
	public static class LeaseSpaceWriteLease extends Lease {
		public static String TYPE = "LeaseSpaceWriteLease";
		public LeaseSpaceWriteLease(){
			this.leaseType = TYPE;
		}
		public LeaseSpaceWriteLease(String itemKey, String itemValue, long timeoutSeconds) {
			super(itemKey, itemValue, timeoutSeconds, TYPE);
		}
		
		public void execute(Space dataSpace, Space leaseSpace) {
			leaseSpace.take(getItemKey(), -1, -1);
			leaseSpace.take(this.getLeaseKey(), -1, -1);
		}
	}
	
	public static class NotifyLease extends Lease {
		public static String TYPE = "NotifyLease";
		public NotifyLease(){
			this.leaseType = TYPE;
		}
		
		public NotifyLease(String itemKey, String itemValue, long timeoutSeconds) {
			super(itemKey, itemValue, timeoutSeconds, TYPE);
		}
		/**
		 * When expiring removed the notifier and takes the lease item from the space
		 */
		public void execute(Space dataSpace, Space leaseSpace) {
			dataSpace.removeNotification(this.itemKey);
		}
	}
}
