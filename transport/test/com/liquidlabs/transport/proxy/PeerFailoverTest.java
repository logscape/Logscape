package com.liquidlabs.transport.proxy;

import com.liquidlabs.transport.Config;
import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.TransportFactoryImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.junit.Assert.assertThat;

@SuppressWarnings("unused")
public class PeerFailoverTest {
    private ProxyFactoryImpl proxyFactoryA;

    private ProxyFactoryImpl proxyFactoryB;
    private DummyServiceImpl dummyServiceB;

    private ProxyFactoryImpl proxyFactoryC;
    private DummyServiceImpl dummyServiceC;

    ProxyClient<?> client;

    private DummyService proxyToService;

    private AddressUpdater addressUpdater;

    private TransportFactory transportFactoryA;

    private TransportFactoryImpl transportFactoryB;

    private TransportFactoryImpl transportFactoryC;

    private ExecutorService executorA;
    private ExecutorService executorB;
    private ExecutorService executorC;


    @After
    public void tearDown() throws Exception {
        transportFactoryA.stop();
        transportFactoryB.stop();
        transportFactoryC.stop();
        proxyFactoryA.stop();
        proxyFactoryB.stop();
        proxyFactoryC.stop();
        dummyServiceB.stop();
        dummyServiceC.stop();
    }

    @Before
    public void setUp() throws Exception {
        executorA = Executors.newFixedThreadPool(5);
        transportFactoryA = new TransportFactoryImpl(executorA, "test");
        proxyFactoryA = new ProxyFactoryImpl(transportFactoryA, TransportFactoryImpl.getDefaultProtocolURI("", "localhost", Config.TEST_PORT, "testServiceA"), executorA, "");
        proxyFactoryA.start();


        executorB = Executors.newFixedThreadPool(5);
        transportFactoryB = new TransportFactoryImpl(executorB, "test");
        proxyFactoryB = new ProxyFactoryImpl(transportFactoryB, TransportFactoryImpl.getDefaultProtocolURI("", "localhost", Config.TEST_PORT + 11000, "testServiceB"), executorB, "");
        dummyServiceB = new DummyServiceImpl("dummyServiceB");
        proxyFactoryB.registerMethodReceiver("methodReceiver", dummyServiceB);
        proxyFactoryB.start();

        executorC = Executors.newFixedThreadPool(5);
        transportFactoryC = new TransportFactoryImpl(executorC, "test");
        proxyFactoryC = new ProxyFactoryImpl(transportFactoryC, TransportFactoryImpl.getDefaultProtocolURI("", "localhost", Config.TEST_PORT + 12000, "testServiceC"), executorC, "");
        dummyServiceC = new DummyServiceImpl("dummyServiceC");
        proxyFactoryC.registerMethodReceiver("methodReceiver", dummyServiceC);
        proxyFactoryC.start();

        DummyServiceImpl.callCount = 0;

        AddressUpdater addressUpdater = new AddressUpdater() {

            public String getId() {
                return AddressUpdater.class.getName();
            }

            public void setProxyClient(ProxyClient<?> clientHandle) {
                client = clientHandle;
            }

            public void updateEndpoint(String address, String replicationAddress) {
            }

            public void removeEndPoint(String address, String replicationAddress) {
            }

            public void syncEndPoints(String[] addresses, String[] replicationLocations) {
            }

            public void setId(String clientId) {
                // TODO Auto-generated method stub

            }

        };

        // Register both remoteB and remote C
        proxyToService = proxyFactoryA.getRemoteService("methodReceiver", DummyService.class,
                new String[]{proxyFactoryB.getAddress().toString(), proxyFactoryC.getAddress().toString()}, addressUpdater);

        System.out.println("================================="
                + "============================================");
        Thread.sleep(100);
    }

    @Test
    public void testShouldRemoveAllAddressesAndTryToReplayToBadEntry() throws Exception {

        System.out.println(client.getClientAddress());

        dummyServiceB.instanceLatch = new CountDownLatch(1);
        dummyServiceC.instanceLatch = new CountDownLatch(1);
        // call on B
        proxyToService.twoWayWithPauseAndReplay("firstSecond", 10);

        // remove it so it replays and Calls onto C
        client.removeAddress(proxyFactoryB.getEndPoint().toString());
        client.removeAddress(proxyFactoryC.getEndPoint().toString());

        assertThat(dummyServiceB.instanceLatch.await(5, TimeUnit.SECONDS), is(true));
        assertThat(dummyServiceC.instanceLatch.await(5, TimeUnit.SECONDS), is(true));
    }

    @Test
    public void testShouldFailoverWhenConnectionRefused() throws Exception {

        dummyServiceB.instanceLatch = new CountDownLatch(1);
        dummyServiceC.instanceLatch = new CountDownLatch(1);
        // call on B
        proxyToService.twoWayWithPause("first", 10);

        // kill B
        proxyFactoryB.stop();
        transportFactoryB.stop();
        Thread.sleep(100);

        System.out.println("---------------------------- REtry");
        // call again and get auto-redirect to C
        proxyToService.twoWayWithPause("second", 10);

        System.out.println("---------------------------- REtry");
        

        assertThat(dummyServiceB.instanceLatch.await(5, TimeUnit.SECONDS), is(true));
        assertThat(dummyServiceC.instanceLatch.await(5, TimeUnit.SECONDS), is(true));
    }

    @Test
    public void testShouldFailoverWhenInvocationIsTimingOut() throws Exception {
        // ensure we are using both endPoints in correct order
        dummyServiceB.instanceLatch = new CountDownLatch(1);
        dummyServiceC.instanceLatch = new CountDownLatch(1);
        
        // set the timeout to be 1 SECOND
        client.setInvocationTimeout(1);

        // Make invocation WAIT for 1 SECOND - inside the Client will add on an
        // extra second
        try {
        	String twoWayWithPause = proxyToService.twoWayWithPause("firstSecond", 5 * 1000);
        } catch (Throwable t){
        	t.printStackTrace();
        }

        // should have timed out, retried with 1 second longer and ended up
        // being handled by BOTH B and C
        assertThat("Service-B Timed Out - did not get inv", dummyServiceB.instanceLatch.await(5, TimeUnit.SECONDS), is(true));
        assertThat("Service-C Timed Out - did not get inv", dummyServiceC.instanceLatch.await(5, TimeUnit.SECONDS), is(true));
    }

    @Test
    public void testShouldRemoveAddressAndUseRemainingItem() throws Exception {
        dummyServiceB.instanceLatch = new CountDownLatch(1);
        dummyServiceC.instanceLatch = new CountDownLatch(1);

        // make the first call to endPoint B
        proxyToService.twoWayWithPause("firstSecond", 10);
        assertThat(dummyServiceB.instanceLatch.await(5, TimeUnit.SECONDS), is(true));

        // remove it so it points back to C
        client.removeAddress(proxyFactoryB.getEndPoint().toString());

        // now invoke on C
        proxyToService.twoWayWithPause("firstSecond", 10);
        assertThat(dummyServiceC.instanceLatch.await(5, TimeUnit.SECONDS), is(true));
    }

    @Test
    public void testShouldRemoveAddressAndReplayMethod() throws Exception {
        dummyServiceB.instanceLatch = new CountDownLatch(1);
        dummyServiceC.instanceLatch = new CountDownLatch(1);

        System.out.println(client.getClientAddress());

        // call on B
        proxyToService.twoWayWithPauseAndReplay("firstSecond", 10);
        assertThat(dummyServiceB.instanceLatch.await(1, TimeUnit.SECONDS), is(true));

        // remove it so it REPLAYS and Calls onto C
        client.removeAddress(proxyFactoryB.getEndPoint().toString());

        // now check we point to C
        assertThat(dummyServiceC.instanceLatch.await(5, TimeUnit.SECONDS), is(true));
    }
    
    @Test
	public void shouldSurviveCacheableFailover() throws Exception {
        dummyServiceB.instanceLatch = new CountDownLatch(1);
        dummyServiceC.instanceLatch = new CountDownLatch(3);


        // set the timeout to be 1 SECOND
        client.setInvocationTimeout(1);

        // call on B
        proxyToService.twoWayCached();
        assertEquals(1, dummyServiceB.instanceCallCount);
        
        // kill B
        proxyFactoryB.stop();
        transportFactoryB.stop();
        Thread.sleep(1500);

        // cache has expired so hit C

        proxyToService.twoWayCached();
        assertEquals(1, dummyServiceC.instanceCallCount);

     }

    // DISABLED FOR TEAM-CITY !! 
    //@Test
    public void testShouldReuseBadAddressesMethod() throws Exception {
        dummyServiceB.instanceLatch = new CountDownLatch(1);
        dummyServiceC.instanceLatch = new CountDownLatch(3);


        // set the timeout to be 1 SECOND
        client.setInvocationTimeout(1);

        // call on B - and get timeout and have retry on C
        try {
            proxyToService.twoWayWithPause("ONE", 3 * 1000);
        } catch (Throwable t) {
            t.printStackTrace();
        }

        Thread.sleep(100);
        // call on C - and reused bad-ep
        proxyToService.twoWayWithPause("TWO", 1);


        // prove C it still works
        String twoWay = proxyToService.twoWay("firstSecond");
        Assert.assertNotNull(twoWay);

        assertThat(dummyServiceB.instanceLatch.await(5, TimeUnit.SECONDS), is(true));
        assertThat(dummyServiceC.instanceLatch.await(10, TimeUnit.SECONDS), is(true));
    }

//    @Test
//    public void testShouldWaitWhileDebugging() {
//        while (true) {
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
//    }

}
