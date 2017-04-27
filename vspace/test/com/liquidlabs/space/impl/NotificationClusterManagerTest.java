package com.liquidlabs.space.impl;

import junit.framework.TestCase;

import com.liquidlabs.space.Space;
import com.liquidlabs.space.impl.NotificationClusterManager.RemoteNotifySubscriberInfo;
import com.liquidlabs.transport.proxy.events.Event;
import com.liquidlabs.transport.proxy.events.EventListener;
import com.liquidlabs.transport.proxy.events.Event.Type;
import com.liquidlabs.transport.serialization.ObjectTranslator;

public class NotificationClusterManagerTest extends TestCase {
	
	public void testShouldBeAbleToPassTemplateDelimFieldsAsString() throws Exception {
		EventListener listener = new EventListener(){
			public String getId() {
				return "id";
			}
			public void notify(Event event) {
			}
		};
		String[] trickyField = new String[] { "c|d|e".replaceAll("\\|", Space.DELIM), "f|g".replaceAll("\\|", Space.DELIM), "h|i".replaceAll("\\|", Space.DELIM) };
		RemoteNotifySubscriberInfo notifySubscriberInfo = new NotificationClusterManager.RemoteNotifySubscriberInfo("listenerId", new String [] { "a", "b"}, trickyField , new Event.Type[] { Type.READ }, -1, "remoteListenerId","src");

		// check that is can be converter to and from string
		ObjectTranslator query = new ObjectTranslator();
		String stringFromObject = query.getStringFromObject(notifySubscriberInfo);
		RemoteNotifySubscriberInfo objectFromFormat = query.getObjectFromFormat(RemoteNotifySubscriberInfo.class, stringFromObject);
		
		assertNotNull(objectFromFormat);
		assertEquals(2, objectFromFormat.keys.length);
		
	}

}
