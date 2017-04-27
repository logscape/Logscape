package com.liquidlabs.space.raw;


import java.util.concurrent.Executors;

import junit.framework.TestCase;

import com.liquidlabs.space.Space;
import com.liquidlabs.space.map.ArrayStateSyncer;
import com.liquidlabs.space.map.MapImpl;
import com.liquidlabs.space.notify.EventHandler;
import com.liquidlabs.space.notify.NotifyEventHandler;

public class SpaceTakeTest extends TestCase {

	private SpaceImpl space;
	private long timeout = 5;
	private String partition = "partition";
	private long expires = -1;
	EventHandler eventHandler = new NotifyEventHandler("xx", "xx", 1024, Executors.newScheduledThreadPool(1));
	private ArrayStateSyncer stateSyncer;

	protected void setUp() throws Exception {
		super.setUp();
		space = new SpaceImpl(partition, new MapImpl("333", partition, 1024, true, stateSyncer), eventHandler);
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}
	public void testShouldBeAbleToTakeItem() throws Exception {
		space.write("someValue", "XXXXX", timeout);
		String take = space.take("someValue", timeout, expires );
		assertEquals("XXXXX", take);
	}
	public void testShouldReturnNullWhenNoItemFound() throws Exception {
		space.write("someValue", "XXXXX", timeout);
		String take = space.take("someValue", timeout, expires );
		assertEquals("XXXXX", take);
		String take2 = space.take("someValue", timeout, expires);
		assertNull(take2);

	}
	public void testShouldBeAbleToTakeMultiple() throws Exception {
		space.write("someValue1", "X|1|Y".replaceAll("\\|", Space.DELIM), timeout);
		space.write("someValue2", "X|2|Y".replaceAll("\\|", Space.DELIM), timeout);
		String[] takes = space.takeMultiple(new String[]{"||Y".replaceAll("\\|", Space.DELIM)}, 99);
		assertEquals(2, takes.length);
		String[] takes2 = space.takeMultiple(new String[]{"||Y".replaceAll("\\|", Space.DELIM)}, 99);
		assertEquals(0, takes2.length);
	}
	public void testShouldBeAbleToTakeMultipleWithLimit() throws Exception {
		space.write("someValue1", "X|1|Y".replaceAll("\\|", Space.DELIM), timeout);
		space.write("someValue2", "X|2|Y".replaceAll("\\|", Space.DELIM), timeout);
		space.write("someValue3", "X|3|Y".replaceAll("\\|", Space.DELIM), timeout);
		String[] takes = space.takeMultiple(new String[]{"||Y".replaceAll("\\|", Space.DELIM)},2, timeout, expires);
		assertEquals(2, takes.length);
		String[] takes2 = space.takeMultiple(new String[]{"||Y".replaceAll("\\|", Space.DELIM)}, 99);
		assertEquals(1, takes2.length);
	}
}
