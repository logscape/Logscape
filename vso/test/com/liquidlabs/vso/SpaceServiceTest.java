package com.liquidlabs.vso;

import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.orm.Id;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.transport.Config;
import com.liquidlabs.transport.proxy.events.Event.Type;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.ServiceInfo;
import junit.framework.TestCase;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Executors;

import static junit.framework.Assert.*;

public class SpaceServiceTest  {

    Mockery mockery = new Mockery();
    private LookupSpace lookup;
    private ORMapperFactory mapperFactory;
    private SpaceServiceImpl space;

    @Before
    public void setUp() throws Exception {
        VSOProperties.setMaxNotifyFailures(1);
        ExecutorService.setTestMode();
        
        lookup = mockery.mock(LookupSpace.class);

        mockery.checking(new Expectations() {
            {
                one(lookup).unregisterService(with(any(ServiceInfo.class)));
                one(lookup).registerService(with(any(ServiceInfo.class)), with(any(Long.class)));
            }
        });


        this.mapperFactory = new ORMapperFactory(10204, "SERVICE", 10204);

        space = new SpaceServiceImpl(lookup, mapperFactory, "SERVICE", mapperFactory.getScheduler(), false, false, true);
        space.start();
        space.start(this, "test-1.0");
        count = 0;
    }

    @After
    public void tearDown() throws Exception {
        space.stop();
        mapperFactory.stop();
        VSOProperties.resetMaxNotifyFailures();
    }
    
    @Test
    public void testShouldNotifyThenUnRegOnError() throws Exception {
        Type[] types = new Type[]{Type.WRITE, Type.UPDATE};
        space.registerListener(Stuff.class, "", new Notifier<Stuff>() {

            public void notify(Type event, Stuff result) {
                System.out.println("Got:" + event);
                count++;
                throw new RuntimeException("make it unregister");
            }

        }, "notifierId", 10, types);

        space.store(new Stuff("id"), -1);

        Thread.sleep(100);

        Assert.assertEquals(1, count);

        space.store(new Stuff("id2"), -1);

        Thread.sleep(100);

        Assert.assertEquals(1, count);
    }


    public int count = 0;

    @Test
    public void testShouldNotify() throws Exception {

        Type[] types = new Type[]{Type.WRITE, Type.UPDATE};
        space.registerListener(Stuff.class, "", new Notifier<Stuff>() {

            public void notify(Type event, Stuff result) {
                System.out.println("Got:" + event);
                count++;
            }

        }, "notifierId", 10, types);

        space.store(new Stuff("id"), -1);

        Thread.sleep(100);

        Assert.assertEquals(1, count);

        space.store(new Stuff("id2"), -1);

        Thread.sleep(100);

        Assert.assertEquals(2, count);
    }

    @Test
    public void testShouldNotifyThenNotWhenLeaseIsUp() throws Exception {

        Type[] types = new Type[]{Type.WRITE};
        space.registerListener(Stuff.class, "", new Notifier<Stuff>() {

            public void notify(Type event, Stuff result) {
                System.out.println("Got:" + event + " count:" + count++);
            }

        }, "notifierId", 1, types);

        System.out.println("Store 1");
        space.store(new Stuff("id"), -1);

        Thread.sleep(100);

        Assert.assertEquals(1, count);

        Thread.sleep(2 * 1000);

        // should not receive it
        System.out.println("Store 2");
        space.store(new Stuff("id"), -1);
        
        Assert.assertEquals(1, count);
    }
    @Test
    public void testShouldStorePoundSign() throws Exception {

        String id = "dfhj" + Config.OBJECT_DELIM + "fj231Dgw" + Config.OBJECT_DELIM;
        space.store(new Stuff(id), -1);

        Thread.sleep(100);
        Stuff got = space.findById(Stuff.class, id);
        assertNotNull(got);
        assertEquals(id, got.id);
    }



    public static class Stuff {

        public Stuff() {
        }

        public Stuff(String string) {
            id = string;
        }

        @Id
        String id;
    }

}
