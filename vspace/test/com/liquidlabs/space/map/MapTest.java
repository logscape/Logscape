package com.liquidlabs.space.map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.converters.collections.MapConverter;
import com.thoughtworks.xstream.mapper.Mapper;


public class MapTest  {
	
	private MapImpl map;
	private ArrayStateSyncer stateSyncer;

	@Before
	public void setUp() throws Exception {
		map = new MapImpl("xxx", "partition", 10240, true, stateSyncer);
	}
	
	@Test
	public void testShouldShrinkAgeQueueAndSplitQueueWhenItemsAreTaken() throws Exception {
		map.put("one", "oneV");
		map.put("two", "oneV");
		map.put("three", "oneV");
		map.put("four", "oneV");
		map.put("five", "oneV");
		map.remove("one");
		map.remove("three");
		map.remove("five");
//		assertEquals(2, map.postionAge.size());
		assertEquals(2, map.splitValues.size());
	}
	
	
	
	@Test
	public void testShouldHandleConcurrency() throws Exception {
		int numberOfWriters = 10;
		Thread[] writers = new Thread[numberOfWriters];
		final int itemsPerWriter = 1000;
		for(int i = 0; i < numberOfWriters; i++) {
			final int wId = i;
			writers[i] = new Thread(){
				public void run() {
					for (int j = 0 ; j < itemsPerWriter; j++){
						try {
							map.put(String.format("%d%d",wId,j), "stuff");
						} catch (MapIsFullException e) {
							e.printStackTrace();
						}
					}
				}
			};
			writers[i].start();
		}
		for(int i = 0; i < numberOfWriters; i++) {
			writers[i].join();
		}

		int failureCount = 0;
		boolean failure = false;
		// verify
		for(int i = 0; i < numberOfWriters; i++) {
			for (int j = 0 ; j < itemsPerWriter; j++){
				String string = map.get(String.format("%d%d",i,j));
				if (string == null) {
					failure = true;
					failureCount++;
				}
			}
		}
		assertFalse(String.format("Missing Value Count:%d",failureCount), failure);
		
	}
	@Test

	public void testItemIsInserted() throws Exception {
		map.put("A", "stuff");
		String string = map.get("A");
		assertEquals("stuff", string);
	}


	
	@Test
	public void testOverflowingTheMapReportsTheOldestItem() throws Exception {
		
		try {
			for (int i = 0; i < 10000; i++){
				map.put("a" + i, "value");		
			}
		} catch (Throwable t){
			assertTrue(t.getMessage().contains("No positions left!"));
		}
	}
	
	@Test
	public void testThatRemoveEntriesGetReused() throws Exception {
		for(int i =0; i < 100; i++){
			map.put("a" + i, "value");		
		}
		System.out.println("1>>" + map.getOldestKey());
		map.remove("a10");
		map.put("kXX", "vXX");
//		assertEquals(10, map.getPosition("kXX"));
		System.out.println("2>>" + map.getOldestKey());
	}
}
