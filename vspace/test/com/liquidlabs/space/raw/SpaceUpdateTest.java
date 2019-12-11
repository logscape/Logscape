package com.liquidlabs.space.raw;


import java.util.concurrent.Executors;

import junit.framework.TestCase;

import com.liquidlabs.space.Space;
import com.liquidlabs.space.map.ArrayStateSyncer;
import com.liquidlabs.space.map.MapImpl;
import com.liquidlabs.space.notify.EventHandler;
import com.liquidlabs.space.notify.NotifyEventHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SpaceUpdateTest {

	private SpaceImpl space;
	private long expires = 9999;
	private long timeout = 9999;
	private int limit = 9999;
	private String partition = "partition";
	private ArrayStateSyncer stateSyncer;

	@Before
	public void setUp() throws Exception {
		EventHandler eventHandler = new NotifyEventHandler("xx","xx", 1024, Executors.newScheduledThreadPool(1));
		space = new SpaceImpl(partition, new MapImpl("srcUID", partition, 1024, true, stateSyncer), eventHandler);
	}

	@After
	public void tearDown() throws Exception {
	}
	@Test
	public void testShouldUpdate() throws Exception {
		space.write("someValue", "A|XXXXX".replaceAll("\\|", Space.DELIM), expires);
		space.update("someValue", "A|replace:YY".replaceAll("\\|", Space.DELIM), timeout);
		String read = space.read("someValue");
		assertEquals("A|YY".replaceAll("\\|", Space.DELIM), read);
	}
	@Test
	public void testShouldUpdateAndFindByTemplate() throws Exception {
		space.write("someValue", "XXXXX", expires);
		space.update("someValue", "replace:YY", timeout);
		String[] read = space.readMultiple(new String[] {"YY"}, 99);
		assertTrue("Should have found update item", read.length > 0);
		assertEquals("YY", read[0]);
	}
	@Test
	public void testShouldUpdateTemplate() throws Exception {
		space.write("someValue", "A|XXXXX".replaceAll("\\|", Space.DELIM), expires);
		space.update("someValue", "|replace:YYYYY".replaceAll("\\|", Space.DELIM), timeout);
		String read = space.read("someValue");
		assertEquals("A|YYYYY".replaceAll("\\|", Space.DELIM), read);
		
	}
}
