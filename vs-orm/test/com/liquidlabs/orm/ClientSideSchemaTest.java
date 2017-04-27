package com.liquidlabs.orm;

import java.util.ArrayList;
import java.util.List;

import com.liquidlabs.common.NetworkUtils;
import junit.framework.TestCase;

import com.liquidlabs.transport.proxy.events.Event.Type;

public class ClientSideSchemaTest extends TestCase {
	
	private ORMapperClient orClient;

	@Override
	protected void setUp() throws Exception {
		orClient = new ORMapperFactory(NetworkUtils.determinePort(11111)).getORMapperClient();
	}

	
	
	List<String> payloadList = new ArrayList<String>();
	

	public void testGetsEvents() throws Exception {
		
		ORMEventListener eventListener = new ORMEventListener() {
			public String getId() {
				return "listener";
			}
			public void notify(String key, String payload, Type event, String source) {
				payloadList.add(key);
			}
			
		};
		orClient.registerEventListener(TestObject.class, "", eventListener, new Type[] { Type.READ, Type.WRITE }, -1);
		
		orClient.store(new TestObject("ONE", "EP", new ChildObject("childValue")));
		
		List<TestObject> findObjects = orClient.findObjects(TestObject.class, "", false, -1);
		assertTrue(findObjects.size() == 1);
		assertTrue(payloadList.size() == 1);
	}
	
	public static class TestObject {
		
		enum SCHEMA { stuff, ep, child }
		@Id
		String stuff = "stuffff";
		String ep;
		ChildObject child;
		
		public TestObject() {
			
		}
		public TestObject(String stuff, String ep, ChildObject child) {
			this.stuff = stuff;
			this.ep = ep;
			this.child = child;
		}
		public String toString() {
			return String.format("TestObject:%s %s %s", this.stuff, this.ep, this.child);
		}
	}
	public static class ChildObject {
		String child;
		public ChildObject() {
			
		}
		public ChildObject(String child) {
			this.child = child;
		}
		
		public String toString() {
			return "Child:" + child;
		}
	}
	

}
