package com.liquidlabs.orm;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.transport.proxy.events.Event.Type;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ClientSideSchemaTest {

    private ORMapperClient orClient;
    private ORMapperFactory orMapperFactory;

    @Before
    public void setUp() throws Exception {
        orMapperFactory = new ORMapperFactory(NetworkUtils.determinePort(11111));
        orClient = orMapperFactory.getORMapperClient();
    }

    @After
    public void teardown() {
        orMapperFactory.stop();
    }

    List<String> payloadList = new ArrayList<String>();

    @Test
    public void testGetsEvents() throws Exception {

        ORMEventListener eventListener = new ORMEventListener() {
            public String getId() {
                return "listener";
            }

            public void notify(String key, String payload, Type event, String source) {
                payloadList.add(key);
            }

        };
        orClient.registerEventListener(TestObject.class, "", eventListener, new Type[]{Type.READ, Type.WRITE}, -1);

        orClient.store(new TestObject("ONE", "EP", new ChildObject("childValue")));

        List<TestObject> findObjects = orClient.findObjects(TestObject.class, "", false, -1);

        Thread.sleep(100);
        assertEquals("No objects in the space", 1, findObjects.size());
        assertEquals("Empty payload list", 1, payloadList.size());
    }

    public static class TestObject {

        enum SCHEMA {stuff, ep, child}

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
