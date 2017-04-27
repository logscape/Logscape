package com.liquidlabs.space.notify;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.transport.proxy.events.Event;
import com.liquidlabs.transport.proxy.events.EventListener;
import com.liquidlabs.transport.proxy.events.Event.Type;

public class NotifyEventHandlerTest extends TestCase implements EventListener {
	
	
	private NotifyEventHandler notifyEventHandler;
	AtomicInteger count = new AtomicInteger();
	private CountDownLatch countDownLatch;
	
	protected void setUp() throws Exception {
		System.setProperty("allow.read.events","true");
		ExecutorService.setTestMode();
		super.setUp();
		
		notifyEventHandler = new NotifyEventHandler("sourceId", "xx", 1024, Executors.newScheduledThreadPool(1));
		notifyEventHandler.start();
		countDownLatch = new CountDownLatch(1000);
	}
	
	protected void tearDown() throws Exception {
		notifyEventHandler.stop();
		super.tearDown();
	}
	public void notify(Event event) {
			this.count.addAndGet(1);
			countDownLatch.countDown();
	}
	public String getId() {
		return getName();
	}
	
	public void testValueReadNotifyWorks() throws Exception {
		
		countDownLatch = new CountDownLatch(1000);
		notifyEventHandler.notify(new String[0], new String[] { "value"} , this, new Type[] { Type.READ } , -1);
		for (int i = 0; i < 1000; i++) {
			notifyEventHandler.handleEvent(new Event("key", "value", Type.READ));
		}
		System.out.println("Countdown at:" + countDownLatch.getCount());
		countDownLatch.await(10, TimeUnit.SECONDS);
		assertEquals("expected 1000 events", 1000, count.get());		
	}
	
	public void testKeyWriteNotifyWorks() throws Exception {
		countDownLatch = new CountDownLatch(1000);
		notifyEventHandler.notify(new String[] { "key" } , new String[0] , this, new Type[] { Type.WRITE } , -1);
		for (int i = 0; i < 1000; i++) {
			notifyEventHandler.handleEvent(new Event("key", "value", Type.WRITE));
		}
		countDownLatch.await(10, TimeUnit.SECONDS);
		assertEquals(1000, count.get());		
	}
	public void testKeyWriteSomeNotifyWorks() throws Exception {
		countDownLatch = new CountDownLatch(1000);
		notifyEventHandler.notify(new String[0], new String[] { "contains:aValue1"} , this, new Type[] { Type.WRITE } , -1);
		for (int i = 0; i < 1000; i++) {
			notifyEventHandler.handleEvent(new Event("key" + i, "aValue1" + i, Type.WRITE));
		}
		boolean finished = countDownLatch.await(20, TimeUnit.SECONDS);
		assertEquals("Expected 1000 events, finished:" + finished, 1000, count.get());		
	}
	
	public void testListenerAddedProperly() throws Exception {
		notifyEventHandler.notify(new String[] { "key" } , new String[0] , this, new Type[] { Type.WRITE } , -1);
		assertEquals(1, notifyEventHandler.writeEventListeners.size());
	}
	public void testListenerIsRemovedProperly() throws Exception {
		notifyEventHandler.notify(new String[] { "key" } , new String[0] , this, new Type[] { Type.WRITE } , -1);
		assertEquals(1, notifyEventHandler.writeEventListeners.size());
		notifyEventHandler.removeListener(getName());
		assertEquals(0, notifyEventHandler.writeEventListeners.size());
	}
}
