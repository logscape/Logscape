package com.liquidlabs.space.raw;


import java.util.concurrent.Executors;

import junit.framework.TestCase;

import org.joda.time.DateTimeUtils;

import com.liquidlabs.space.Space;
import com.liquidlabs.space.lease.Lease;
import com.liquidlabs.space.lease.Leases;
import com.liquidlabs.space.lease.Leases.TakeLease;
import com.liquidlabs.space.lease.Leases.UpdateLease;
import com.liquidlabs.space.lease.Leases.WriteLease;
import com.liquidlabs.space.map.ArrayStateSyncer;
import com.liquidlabs.space.map.MapImpl;
import com.liquidlabs.space.notify.EventHandler;
import com.liquidlabs.space.notify.NotifyEventHandler;
import com.liquidlabs.transport.serialization.ObjectTranslator;

public class SpacePerfTest extends TestCase {

	private SpaceImpl space;
	private long timeout = 9999;
	private String partition = "partition";
	EventHandler eventHandler = new NotifyEventHandler("xx", "partitionName", 10 * 1024, Executors.newScheduledThreadPool(1));
	private ArrayStateSyncer stateSyncer;

	protected void setUp() throws Exception {
		super.setUp();
		space = new SpaceImpl(partition, new MapImpl("333", partition, 10 * 1024, true, stateSyncer), eventHandler);
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}
	
	public void testGetKeysForFunctionalMatch3() throws Exception {
		String value = "com.liquidlabs.flow.Invocation,707,807?,com.liquidlabs.flow.testApp.Pricer,5-26-36-625,public abstract void com.liquidlabs.flow.testApp.Pricer.price(int),sessionX0,NEW,".replaceAll(",", Space.DELIM);

		for (int i = 0; i < 4096; i++){
			space.write("value" + i, value + i, timeout);
		}
		String template = "com.liquidlabs.flow.Invocation|||||equals:public abstract void com.liquidlabs.flow.testApp.Pricer.price(int)|equals:sessionX0|equals:OLD|".replaceAll("\\|", Space.DELIM);
		int work = 100;
		long start = System.currentTimeMillis();
		for (int i = 0; i < work; i++){
			String[] keys = space.readKeys(new String[] { template }, Integer.MAX_VALUE);
		}
		
//		Time:2781
//		Throughput:27
		System.out.println("Time:" + (System.currentTimeMillis() - start) + " spaceSize:" + space.size());
		System.out.println("TimePerItem:" + (System.currentTimeMillis() - start)/ (work * 1.0));
	}
	
	public void testTakePerformanceLikeLeasingRequires() throws Exception {
		
		ObjectTranslator query = new ObjectTranslator();
		int work = 100;
		long before = DateTimeUtils.currentTimeMillis();
		for (int i = 0; i < work; i++) {
			String key = "key" + i;
			System.out.println(key);
			Lease lease = new Leases.WriteLease(key, "value", 10);
			space.write(lease.getLeaseKey(), query.getStringFromObject(lease), -1);
			String[] writeTemplate = query.getQueryStringTemplate(WriteLease.class, "itemKey equals " + key);
			String[] takeTemplate = query.getQueryStringTemplate(TakeLease.class, "itemKey equals " + key);
			String[] updateTemplate = query.getQueryStringTemplate(UpdateLease.class, "itemKey equals " + key);
			String[] takeMultiple = space.takeMultiple(new String[] { writeTemplate[0], takeTemplate[0], updateTemplate[0] }, -1, -1, -1);
			System.out.println("take:" + takeMultiple.length);
		}
		System.out.println("Elapsed:" + (DateTimeUtils.currentTimeMillis() - before));
	}

	

}
