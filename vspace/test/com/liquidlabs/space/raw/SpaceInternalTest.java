package com.liquidlabs.space.raw;


import java.util.concurrent.Executors;

import junit.framework.TestCase;

import com.liquidlabs.space.Space;
import com.liquidlabs.space.map.ArrayStateSyncer;
import com.liquidlabs.space.map.MapImpl;
import com.liquidlabs.space.notify.EventHandler;
import com.liquidlabs.space.notify.NotifyEventHandler;


public class SpaceInternalTest extends TestCase {
	
	private SpaceImpl space;
	private String partition = "partition";
	private String sourceId;
	private ArrayStateSyncer stateSyncer;
	
	protected void setUp() throws Exception {
		super.setUp();
		EventHandler eventHandler = new NotifyEventHandler("xx", "xx", 1024, Executors.newScheduledThreadPool(1));
		space = new SpaceImpl(partition , new MapImpl("333", partition, 1024, true, stateSyncer), eventHandler );
	}
	
	protected void tearDown() throws Exception {
		super.tearDown();
	}
	
	public void testShouldReplaceValuesRight() throws Exception {
		String mergeUpdateValue = space.mergeUpdateValue("A|B|C".replaceAll("\\|", Space.DELIM), "|replace:DD".replaceAll("\\|", Space.DELIM));
		assertEquals("A|DD|C".replaceAll("\\|", Space.DELIM), mergeUpdateValue);
	}
	public void testShouldMergeValuesRight() throws Exception {
		String mergeUpdateValue = space.mergeUpdateValue("A|B|C".replaceAll("\\|", Space.DELIM), "|concat:DD".replaceAll("\\|", Space.DELIM));
		assertEquals("A|BDD|C".replaceAll("\\|", Space.DELIM), mergeUpdateValue);
	}
	public void testShouldPrependValuesRight() throws Exception {
		assertEquals("A|DDB|C".replaceAll("\\|", Space.DELIM), space.mergeUpdateValue("A|B|C".replaceAll("\\|", Space.DELIM), "|prepend:DD".replaceAll("\\|", Space.DELIM)));
	}
	public void testShouldAddValues() throws Exception {
		assertEquals("A|102|C".replaceAll("\\|", Space.DELIM), space.mergeUpdateValue("A|99|C".replaceAll("\\|", Space.DELIM), "|+=:3".replaceAll("\\|", Space.DELIM)));
	}
	public void testShouldMultiplyValuesRight() throws Exception {
		assertEquals("A|100|C".replaceAll("\\|", Space.DELIM), space.mergeUpdateValue("A|50|C".replaceAll("\\|", Space.DELIM), "|*=:2".replaceAll("\\|", Space.DELIM)));
	}
	public void testShouldDivValuesRight() throws Exception {
		assertEquals("A|25|C".replaceAll("\\|", Space.DELIM), space.mergeUpdateValue("A|50|C".replaceAll("\\|", Space.DELIM), "|/=:2".replaceAll("\\|", Space.DELIM)));
	}
	
	public void testShouldMatchLease() throws Exception {
		space.write("lease", "_lease_|40|someKeyA".replaceAll("\\|", Space.DELIM), 9999);
//		List<String> readKeys = space.readKeys("equals:" + Lease.KEY);
		String[] readKeys = space.readKeys(new String[] { "|<=:40".replaceAll("\\|", Space.DELIM) }, 99);
		assertTrue(readKeys.length > 0);
		
	}
	
}