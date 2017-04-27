package com.liquidlabs.space.raw;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.liquidlabs.space.Space;
import com.liquidlabs.space.map.ArrayStateSyncer;
import com.liquidlabs.space.map.MapImpl;
import com.liquidlabs.space.notify.EventHandler;
import com.liquidlabs.space.notify.NotifyEventHandler;

public class SpaceReadTest  {

	private SpaceImpl space;
	private long timeout = 9999;
	private String partition = "partition";
	String readValue;
	String[] readValues = new String[0];
	EventHandler eventHandler = new NotifyEventHandler("xx", "part", 1024, Executors.newScheduledThreadPool(1));
	private CountDownLatch countDownLatch;
	private ArrayStateSyncer stateSyncer;

	@Before
	public void setUp() throws Exception {
		space = new SpaceImpl(partition, new MapImpl("333", partition, 1024, true, stateSyncer), eventHandler);
		eventHandler.start();
		countDownLatch = new CountDownLatch(1);
		
	}

	@After
	public void tearDown() throws Exception {
		space.stop();
		eventHandler.stop();
	}
	
	@Test
	public void testWaitingSuccessReadKey() throws Exception {
		Thread thread = new Thread(){
			@Override
			public void run() {
				System.out.println(getName() + ": Waiting for Read Value");
				readValue = space.read("someKeyA", 10000);
				System.out.println(getName() + ": Got Read Value");
				countDownLatch.countDown();
			}
		};
		thread.start();
		space.write("someKeyA","values", 100000);
		countDownLatch.await(3, TimeUnit.SECONDS);
		thread.interrupt();
		thread.join();

		Assert.assertEquals("Should have received a value", "values", readValue);
	}
	
	@Test
	public void testShouldReadAll() throws Exception {
		space.write("XsomeKeyA1","values", 100000);
		space.write("XsomeKeyA2","values", 100000);
		space.write("XsomeKeyA3","values", 100000);
		space.write("XsomeKeyA4","values", 100000);
		String[] keys = space.readKeys(new String[] { "all:" }, -1);
		Assert.assertTrue("Got:" + keys.length, keys.length == 4);
	}
	
	@Test
	public void testShouldReadAND() throws Exception {
		space.write("XsomeKeyA1","100", -1);
		String[] keys = space.readKeys(new String[] { "==:100| AND:>:0".replaceAll("\\|", Space.DELIM)}, -1);
		Assert.assertEquals("Got:" + keys.length, 1, keys.length);
	}
	
	@Test
	public void testShouldReadOR() throws Exception {
		space.write("XsomeKeyA1","100", -1);
		String[] keys = space.readKeys(new String[] { "==:100| OR:>:0".replaceAll("\\|", Space.DELIM) }, -1);
		Assert.assertEquals("Got:" + keys.length, 1, keys.length);
	}
	


	@Test
	public void testSingleWaitingReadWithTimeout() throws Exception {
		long start = System.currentTimeMillis()/1000;
		String read = space.read("someKeyA", 3);
		long end = System.currentTimeMillis()/1000;
		Assert.assertTrue(end-start > 2);
		Assert.assertNull(read);
	}
	
	@Test
	public void testWaitingNotMatchingReadKey() throws Exception {
		Thread thread = new Thread("testWaitingNotMatchingReadKey"){
			@Override
			public void run() {
				System.out.println(getName() + ": Waiting for Read Value");
				readValue = space.read("someKeyA", 20);
				System.out.println(getName() + ": Got Read Value");
				countDownLatch.countDown();
			}
		};
		thread.start();
		space.write("someKeyB","values", 100000);
		countDownLatch.await(3, TimeUnit.SECONDS);
		thread.interrupt();
		thread.join();
		Assert.assertEquals("Should have NOT received a value and timed out", null, readValue);
	}

	@Test
	public void testWaitingSuccessReadTemplate() throws Exception {
		Thread thread = new Thread("testWaitingSuccessReadTemplate"){
			@Override
			public void run() {
				System.out.println(getName() + ": Waiting for Read Values TEMPLATE");
				readValues = space.readMultiple(new String[] {"A,"}, timeout, 10000);
				System.out.println(getName() + ": Got Read Values TEMPLATE" + readValues.length);
				countDownLatch.countDown();
			}
		};
		thread.start();
		space.write("someKeyA","A,values", 5 * 60);
		countDownLatch.await(30, TimeUnit.SECONDS);
		thread.interrupt();
		thread.join();
		Assert.assertEquals("Should have received a value", 1, readValues.length);
	}
}
