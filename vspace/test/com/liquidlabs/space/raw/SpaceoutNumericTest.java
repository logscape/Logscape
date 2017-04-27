package com.liquidlabs.space.raw;

import java.util.Arrays;
import java.util.concurrent.Executors;

import junit.framework.TestCase;

import com.liquidlabs.space.Space;
import com.liquidlabs.space.map.ArrayStateSyncer;
import com.liquidlabs.space.map.MapImpl;
import com.liquidlabs.space.notify.EventHandler;
import com.liquidlabs.space.notify.NotifyEventHandler;

public class SpaceoutNumericTest extends TestCase {

	private SpaceImpl space;
	private long timeout = 9999;
	private String partition = "partition";
	private ArrayStateSyncer stateSyncer;

	protected void setUp() throws Exception {
		super.setUp();
		EventHandler eventHandler = new NotifyEventHandler("xx", "xx", 1024, Executors.newScheduledThreadPool(1));
		space = new SpaceImpl(partition, new MapImpl("333", partition, 1024, true, stateSyncer), eventHandler);
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}

	public void testGetKeysForFunctionalMatch() throws Exception {
		space.write("lonlt3151", "lonlt3151.uk.db1.com|500|40".replaceAll("\\|", Space.DELIM), timeout );
		space.write("lonlt3152", "lonlt3152.uk.db1.com|600|50".replaceAll("\\|", Space.DELIM), timeout);
		space.write("lonlt3161", "lonlt3161.uk.db2.com|900|60".replaceAll("\\|", Space.DELIM), timeout);
		space.write("lonlt3171", "lonlt3171.uk.db3.com|1000|70".replaceAll("\\|", Space.DELIM), timeout);
		space.write("lonlt3181", "lonlt3181.uk.db4.com|1100|90".replaceAll("\\|", Space.DELIM), timeout);
		String[] keys = space.readKeys(new String[] { "|>=:900".replaceAll("\\|", Space.DELIM) }, 99 );
		assertEquals(3, keys.length);
		assertTrue(Arrays.toString(keys).contains("lonlt3161"));
	}
	
	public void testShouldMatchOnlyOne() throws Exception {
		space.write("A", "HAL2-11100|SLAContainer|".replaceAll("\\|", Space.DELIM), timeout);
		space.write("BA", "resource|HAL2:15011|HAL2|100|1|10240|1193768822|".replaceAll("\\|", Space.DELIM), timeout);
		String[] keys = space.readKeys(new String[] { "equals:resource|||>=:100|".replaceAll("\\|", Space.DELIM) }, 99 );
		assertEquals(1, keys.length);
	}
}
