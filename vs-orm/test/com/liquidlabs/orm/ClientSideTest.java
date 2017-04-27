package com.liquidlabs.orm;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.transport.proxy.events.Event.Type;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class ClientSideTest  {
	
	private ORMapperClient orClient;
	static int ppp = 43215;

	@Before
	public void setUp() throws Exception {
		System.setProperty("test.mode", "true");
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> SETUP");
		System.setProperty("allow.read.events","true");
		mapperFactory = new ORMapperFactory(NetworkUtils.determinePort(ppp++));
		orClient = mapperFactory.getORMapperClient();
	}
	@After
	public void tearDown() throws Exception {
		mapperFactory.stop();
		System.out.println("\n\n\n\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> TEARDOWN");
	}


    @Test
    public void testPreventSavingWithEmptyID() throws Exception {
        TestObject target = new TestObject();
        target.stuff = "";
        try {
            orClient.store(target);
            assertTrue("Should have failed to save", false);
        } catch (Throwable t) {
            t.printStackTrace();;
            assertTrue(true);
        }


    }

	@Test
	public void testReplaceWithAndSelect() throws Exception {
		TestObject target = new TestObject();
		orClient.store(target);
		orClient.update(TestObject.class, target.stuff, "ep replaceWith 'stcp://somewhere/stuff'", -1);
		List<TestObject> findObjects = orClient.findObjects(TestObject.class, "stuff equals " + target.stuff, false, 1);
		assertTrue(findObjects.size() == 1);
		String[] findIds = orClient.findIds(TestObject.class, "ep equals 'stcp://somewhere/stuff'", 1);
		assertTrue(findIds.length == 1);
		
	}
	
	List<String> payloadList = new ArrayList<String>();
	
	boolean blowup = true;

	private ORMapperFactory mapperFactory;
	
	@Test
	public void testGetsEventAfterBadListenerException() throws Exception {
		
		
		ORMEventListener badEventListener = new ORMEventListener() {
			public String getId() {
				return "listener";
			}
			public void notify(String key, String payload, Type event, String source) {
				if (blowup) {
					System.out.println(" xxxxxxxxxxxxx ception");
					throw new RuntimeException("boooooooooooooooom!");
				} else {
					System.out.println(" >>>>>>>>>>> got"+ key);
					payloadList.add(key);
				}
			}
			
		};
		orClient.registerEventListener(TestObject.class, "", badEventListener, new Type[] { Type.READ, Type.WRITE }, -1);
		
		orClient.store(new TestObject());
		
		List<TestObject> findObjects = orClient.findObjects(TestObject.class, "", false, -1);
		Thread.sleep(100);
		
		System.out.println(" ================= not going to fail =====================");
		// TEST = now see if the previously failed listener can be re-registered and still work
		blowup = false;
		orClient.registerEventListener(TestObject.class, "", badEventListener, new Type[] { Type.READ, Type.WRITE }, -1);
		
		findObjects = orClient.findObjects(TestObject.class, "", false, -1);
		Thread.sleep(100);
		
		
		assertTrue(findObjects.size() == 1);
		assertTrue(payloadList.size() == 1);
	}
	
	@Test
	public void testGetsEvents() throws Exception {
		
		ORMEventListener eventListener = new ORMEventListener() {
			public String getId() {
				return "listener";
			}
			public void notify(String key, String payload, Type event, String source) {
				System.out.println(" >>>>>>>>>>>>>> got"+ key);
				payloadList.add(key);
			}
			
		};
		orClient.registerEventListener(TestObject.class, "", eventListener, new Type[] { Type.READ, Type.WRITE }, -1);
		
		orClient.store(new TestObject());
		
		List<TestObject> findObjects = orClient.findObjects(TestObject.class, "", false, -1);
		Thread.sleep(100);
		assertTrue(findObjects.size() == 1);
		assertTrue(payloadList.size() == 2);
	}
	
	public static class TestObject {
		@Id
		String stuff = "stuffff";
		String ep;
		@Override
		public String toString() {
			return super.toString();
		}
	}
	

}
