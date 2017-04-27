package com.liquidlabs.util;

import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

import com.liquidlabs.common.plot.XYPoint;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.transport.Config;

import junit.framework.TestCase;

public class EventStoreTest extends TestCase {
	
	
	private static final String RESOURCE_ID = "resource";
	private static final String SERVICE = "service";
	private static final int PRIORITY = 9;
	private static final String APP = "one-1";
	private ORMapperFactory mapperFactory;
	private EventStoreImpl eventStore;

	@Override
	protected void setUp() throws Exception {
		mapperFactory = new ORMapperFactory(Config.TEST_PORT);
		eventStore = new EventStoreImpl(mapperFactory, false, false);
	}
	public void testShouldCountCompletedEventsProperly() throws Exception {
		long now = System.currentTimeMillis();
		AccountEvent event = new AccountEvent(APP, PRIORITY, SERVICE, RESOURCE_ID, now - 30, 10);
		event.setEndTime(now - 20);
		TreeMap<Long, XYPoint> map = new TreeMap<Long, XYPoint>();
		List<XYPoint> count = eventStore.count(now - 40, now, 10, Arrays.asList(event), map);
		assertEquals(1, count.size());
		
	}

	public void testShouldCountAccumualtedEvents() throws Exception {
		
		AccountEvent event = new AccountEvent(APP, PRIORITY, SERVICE, RESOURCE_ID, System.currentTimeMillis() - 1000, 10);
		event.stop(System.currentTimeMillis()-500);
		eventStore.store(event);
		
		List<AccountEvent> retrieveEventsFrom = eventStore.retrieveEventsFrom(APP,	SERVICE, System.currentTimeMillis() - 2000, System.currentTimeMillis());
		
		assertTrue(retrieveEventsFrom.size() == 1);
		assertTrue(retrieveEventsFrom.get(0).getEndTime() > 0);
	}
	
	public void testShouldNotRetrievePriorEvents() throws Exception {
		
		AccountEvent event = new AccountEvent(APP, PRIORITY, SERVICE, RESOURCE_ID, System.currentTimeMillis() - 2000, 10);
		event.stop(System.currentTimeMillis()-1000);
		eventStore.store(event);
		
		List<AccountEvent> retrieveEventsFrom = eventStore.retrieveEventsFrom(APP,	SERVICE, System.currentTimeMillis() - 500, System.currentTimeMillis());
		
		assertTrue(retrieveEventsFrom.size() == 0);
	}
	public void testShouldNotRetrievePostEvents() throws Exception {
		AccountEvent event = new AccountEvent(APP, PRIORITY, SERVICE, RESOURCE_ID, System.currentTimeMillis() - 2000, 10);
		event.stop(System.currentTimeMillis()-1000);
		eventStore.store(event);
		
		List<AccountEvent> retrieveEventsFrom = eventStore.retrieveEventsFrom(APP,	SERVICE, System.currentTimeMillis() - 500, System.currentTimeMillis());
		
		assertTrue(retrieveEventsFrom.size() == 0);
		
	}
	public void testShouldRetrievePartialPriorEvents() throws Exception {
		AccountEvent event = new AccountEvent(APP, PRIORITY, SERVICE, RESOURCE_ID, System.currentTimeMillis() - 2000, 10);
		event.stop(System.currentTimeMillis()-500);
		eventStore.store(event);
		
		List<AccountEvent> retrieveEventsFrom = eventStore.retrieveEventsFrom(APP,	SERVICE, System.currentTimeMillis() - 1000, System.currentTimeMillis());
		
		assertTrue(retrieveEventsFrom.size() == 1);
		
	}
	public void testShouldRetrievePartialPostEvents() throws Exception {
		AccountEvent event = new AccountEvent(APP, PRIORITY, SERVICE, RESOURCE_ID, System.currentTimeMillis() - 500, 10);
		event.stop(System.currentTimeMillis()+1000);
		eventStore.store(event);
		
		List<AccountEvent> retrieveEventsFrom = eventStore.retrieveEventsFrom(APP,	SERVICE, System.currentTimeMillis() - 1000, System.currentTimeMillis());
		
		assertTrue(retrieveEventsFrom.size() == 1);
		
	}
	
	public void testShouldGetEventsForAppService() throws Exception {
		
		AccountEvent event = new AccountEvent(APP, 9, SERVICE, RESOURCE_ID, System.currentTimeMillis() - 1000, 10);
		event.stop(System.currentTimeMillis()-500);
		eventStore.store(event);
		
		List<AccountEvent> retrieveEventsFrom = eventStore.retrieveEventsFrom(APP, SERVICE, System.currentTimeMillis() - 2000, System.currentTimeMillis()+10);
		assertEquals(1, retrieveEventsFrom.size());
	}
	

}
