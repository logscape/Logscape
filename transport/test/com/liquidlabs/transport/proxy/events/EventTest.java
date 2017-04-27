package com.liquidlabs.transport.proxy.events;

import com.liquidlabs.transport.proxy.events.Event.Type;

import junit.framework.TestCase;

public class EventTest extends TestCase {
	
	
	public void testToStringAndFromString() throws Exception {
		
		Event event = new Event("someKey", "some&value&stuff|withIt", Type.READ);
		String string = event.toString();
		System.out.println(string);
		
		Event event2 = new Event();
		event2.fromString(string);
		
		String string2 = event2.toString();
		System.out.println(string2);
		assertEquals(event.toString(), string2);
		
	}

}
