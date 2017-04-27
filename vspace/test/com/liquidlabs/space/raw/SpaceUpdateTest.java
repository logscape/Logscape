package com.liquidlabs.space.raw;


import java.util.concurrent.Executors;

import junit.framework.TestCase;

import com.liquidlabs.space.Space;
import com.liquidlabs.space.map.ArrayStateSyncer;
import com.liquidlabs.space.map.MapImpl;
import com.liquidlabs.space.notify.EventHandler;
import com.liquidlabs.space.notify.NotifyEventHandler;

public class SpaceUpdateTest extends TestCase {

	private SpaceImpl space;
	private long expires = 9999;
	private long timeout = 9999;
	private int limit = 9999;
	private String partition = "partition";
	private ArrayStateSyncer stateSyncer;

	protected void setUp() throws Exception {
		super.setUp();
		EventHandler eventHandler = new NotifyEventHandler("xx","xx", 1024, Executors.newScheduledThreadPool(1));
		space = new SpaceImpl(partition, new MapImpl("srcUID", partition, 1024, true, stateSyncer), eventHandler);
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}
	public void testShouldUpdate() throws Exception {
		space.write("someValue", "A|XXXXX".replaceAll("\\|", Space.DELIM), expires);
		space.update("someValue", "A|replace:YY".replaceAll("\\|", Space.DELIM), timeout);
		String read = space.read("someValue");
		assertEquals("A|YY".replaceAll("\\|", Space.DELIM), read);
	}
	public void testShouldUpdateAndFindByTemplate() throws Exception {
		space.write("someValue", "XXXXX", expires);
		space.update("someValue", "replace:YY", timeout);
		String[] read = space.readMultiple(new String[] {"YY"}, 99);
		assertTrue("Should have found update item", read.length > 0);
		assertEquals("YY", read[0]);
	}
	public void testShouldUpdateTemplate() throws Exception {
		space.write("someValue", "A|XXXXX".replaceAll("\\|", Space.DELIM), expires);
		space.update("someValue", "|replace:YYYYY".replaceAll("\\|", Space.DELIM), timeout);
		String read = space.read("someValue");
		assertEquals("A|YYYYY".replaceAll("\\|", Space.DELIM), read);
		
	}
}
