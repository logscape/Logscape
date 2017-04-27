package com.liquidlabs.space.raw;

import java.util.concurrent.Executors;

import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.space.Space;
import com.liquidlabs.space.map.ArrayStateSyncer;
import com.liquidlabs.space.map.MapImpl;
import com.liquidlabs.space.notify.NotifyEventHandler;
import com.liquidlabs.transport.proxy.events.DefaultEventListener;
import com.liquidlabs.transport.proxy.events.Event;
import junit.framework.TestCase;


public class SpaceNotificationsTest extends TestCase {

	private SpaceImpl space;
	private long timeout = 9999;
	private String partition = "partition";
	private NotifyEventHandler eventHandler;
	private ArrayStateSyncer stateSyncer;

	protected void setUp() throws Exception {
		ExecutorService.setTestMode();
		super.setUp();
		eventHandler = new NotifyEventHandler("xx", "xx", 1024, Executors.newScheduledThreadPool(1));
		eventHandler.start();
		space = new SpaceImpl(partition, new MapImpl("333", partition, 1024, true, stateSyncer), eventHandler);
		pause();
	}

	protected void tearDown() throws Exception {
		eventHandler.stop();
		super.tearDown();
	}
	
	public void testShouldBeAbleToRemoveNotification() throws Exception {
		
		DefaultEventListener listener = new DefaultEventListener();
		String listenerKey = space.notify(new String[0] , new String[] { "A||C".replaceAll("\\|", Space.DELIM) }, listener , new Event.Type[] { Event.Type.WRITE }, 99);
		space.write("someKeyA", "A|B|C".replaceAll("\\|", Space.DELIM), timeout);
		pause();
		assertEquals(1, listener.getEvents().size());
		
		space.removeNotification(listenerKey);
		
		space.write("someKeyA", "A|B|C".replaceAll("\\|", Space.DELIM), timeout);
		pause();
		assertEquals(1, listener.getEvents().size());
	}

	public void testSingleWriteEventIsCollected() throws Exception {
		DefaultEventListener listener = new DefaultEventListener();
		space.notify(new String[0] , new String[] { "A||C".replaceAll("\\|", Space.DELIM) }, listener , new Event.Type[] { Event.Type.WRITE }, -1);
		space.write("someKeyA", "A|B|C".replaceAll("\\|", Space.DELIM), timeout);
		pause();
		assertEquals(1, listener.getEvents().size());
	}

	public void testMultipleWriteEventIsCollected() throws Exception {
		DefaultEventListener listener = new DefaultEventListener();
		space.notify(new String[0], new String[] { "A||C".replaceAll("\\|", Space.DELIM) }, listener , new Event.Type[] { Event.Type.WRITE }, -1);
		space.write("someKeyA", "A|B|C".replaceAll("\\|", Space.DELIM), timeout);
		space.write("someKeyB", "A|C|C".replaceAll("\\|", Space.DELIM), timeout);
		space.write("someKeyC", "A|C|DoNotMatch".replaceAll("\\|", Space.DELIM), timeout);
		pause();
		assertEquals(2, listener.getEvents().size());
	}

    // @DodgyTest
	public void xxxtestSingleReadEventIsCollected() throws Exception {
		DefaultEventListener listener = new DefaultEventListener();
		space.notify(new String[0], new String[] { "A||C".replaceAll("\\|", Space.DELIM) }, listener , new Event.Type[] { Event.Type.READ }, -1);
		space.write("someKeyA", "A|B|C".replaceAll("\\|", Space.DELIM), timeout);
		space.read("someKeyA");
		pause();
		assertEquals(1, listener.getEvents().size());
	}
	public void testSingleTakeEventIsCollected() throws Exception {
		DefaultEventListener listener = new DefaultEventListener();
		space.notify(new String[0] , new String[] { "A||C".replaceAll("\\|", Space.DELIM) }, listener , new Event.Type[] { Event.Type.TAKE }, -1);
		space.write("someKeyA", "A|B|C".replaceAll("\\|", Space.DELIM), timeout);
		space.take("someKeyA", timeout, -1);
		pause();
		assertEquals(1, listener.getEvents().size());
	}
	
	public void testMultiTakeEventIsCollected() throws Exception {
		DefaultEventListener listener = new DefaultEventListener();
		space.notify(new String[0] , new String[] { "A||C".replaceAll("\\|", Space.DELIM) }, listener , new Event.Type[] { Event.Type.TAKE }, -1);
		space.write("someKeyA", "A|B|C".replaceAll("\\|", Space.DELIM), timeout);
		space.take(new String[] { "someKeyA" } );
		pause(100);
		assertEquals(1, listener.getEvents().size());
	}
	private void pause(int amount) throws InterruptedException {
		Thread.sleep(amount);
	}
	private void pause() throws InterruptedException {
		Thread.sleep(100);
	}
}
