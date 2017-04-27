package com.liquidlabs.transport.proxy;

import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.Config;
import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.TransportFactoryImpl;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ConcurrentClient extends TestCase {
    private ProxyFactoryImpl proxyFactoryA;
    boolean enableOutput = false;
    private ProxyFactoryImpl proxyFactoryB;
    private URI proxyBAddress;
    private DummyService remoteService;

    long callCount;
    private ExecutorService executor;
    private TransportFactory transportFactory;
    private DummyService remoteService2;
    private CopyOnWriteArrayList<String> receivedOrder;


    @Override
    protected void tearDown() throws Exception {
        proxyFactoryA.stop();
        proxyFactoryB.stop();
        super.tearDown();
    }

    @Override
    protected void setUp() throws Exception {
        executor = Executors.newFixedThreadPool(5);
        transportFactory = new TransportFactoryImpl(executor, "");
        super.setUp();
        proxyFactoryA = new ProxyFactoryImpl(transportFactory, TransportFactoryImpl.getDefaultProtocolURI("", "localhost", Config.TEST_PORT, "multiITestA"), executor, "cc");
        proxyFactoryA.start();

        Thread.sleep(100);

        proxyFactoryB = new ProxyFactoryImpl(transportFactory, TransportFactoryImpl.getDefaultProtocolURI("", "localhost", Config.TEST_PORT + 10000, "multiITestB"), executor, "cc");

        receivedOrder = new CopyOnWriteArrayList<String>();
        DummyServiceImpl service = new DummyServiceImpl(receivedOrder);
        proxyFactoryB.registerMethodReceiver("methodReceiver", service);
        proxyFactoryB.start();
        proxyBAddress = proxyFactoryB.getAddress();
        DummyServiceImpl.callCount = 0;
        remoteService = proxyFactoryA.getRemoteService("methodReceiver", DummyService.class, new String[]{proxyBAddress.toString()});
        remoteService2 = proxyFactoryA.getRemoteService("methodReceiver", DummyService.class, new String[]{proxyBAddress.toString()});
    }

    synchronized public void incrementCallCount() {
        callCount++;
    }


    private void runOne(Executor executor, final String arg, final long pause, final Map results) {
        executor.execute(new Runnable() {
            public void run() {
                List<String> returnParams = new ArrayList<String>();
                results.put(arg, returnParams);
                for (int i = 0; i < 20; i++) {
                    returnParams.add(remoteService.twoWayWithPause(arg, pause));
                }
            }
        });
    }

    public void testMultipleConcurrentCallsOnSameProxy() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(50);
        Map<String, List<String>> resultMap = new ConcurrentHashMap<String, List<String>>();
        for (int i = 0; i < 50; i++) {
            runOne(executor, String.valueOf(i), 50 - i, resultMap);
        }
        executor.shutdown();
        executor.awaitTermination(4, TimeUnit.HOURS);
        for (int i = 0; i < 50; i++) {
            List<String> results = resultMap.get(String.valueOf(i));
            assertEquals(20, results.size());
            HashSet<String> set = new HashSet<String>(results);
            assertEquals(1, set.size());
            assertTrue(set.contains(String.valueOf(i)));
        }
    }

    public void testShouldWork() throws Exception {
        final CopyOnWriteArrayList<String> list = new CopyOnWriteArrayList<String>();
        final CountDownLatch latch = new CountDownLatch(1);
        Thread firstInvocationThread = new Thread() {
            @Override
            public void run() {
                latch.countDown();
                list.add(remoteService.twoWayWithPause("ONE", 1000l));
            }
        };

        firstInvocationThread.start();

        // make sure firts one has fired before firing seoond
        latch.await();
        Thread.sleep(500);

        list.add(remoteService2.twoWayWithPause("doSecond", 10l));
        firstInvocationThread.join();

        assertEquals("doSecond", list.get(0));
        assertEquals("ONE", receivedOrder.get(0));
    }

    public void testShouldDoConcurrentRequestsOnSameProxy() throws InterruptedException {
        final CopyOnWriteArrayList<String> list = new CopyOnWriteArrayList<String>();
        final CountDownLatch latch = new CountDownLatch(1);
        Thread firstInvocationThread = new Thread() {
            @Override
            public void run() {
                latch.countDown();
                list.add(remoteService.twoWayWithPause("ONE", 1000l));
            }
        };

        firstInvocationThread.start();

        // make sure firts one has fired before firing seoond
        latch.await();
        Thread.sleep(500);

        list.add(remoteService.twoWayWithPause("doSecond", 10l));
        firstInvocationThread.join();

        assertEquals("doSecond", list.get(0));
        assertEquals("ONE", receivedOrder.get(0));
    }

}