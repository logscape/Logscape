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
public class PeerFailoverAddrUpdateTest {
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

	private AddressUpdater addressUpdater2;


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

        addressUpdater2 = new AddressUpdater() {

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
            }

        };

        // Register both remoteB 
        proxyToService = proxyFactoryA.getRemoteService("methodReceiver", DummyService.class, new String[]{proxyFactoryB.getAddress().toString() }, 
        					addressUpdater2);

        System.out.println("================================="
                + "============================================");
        Thread.sleep(100);
    }

    @Test
    public void testShouldFailoverWhenConnectionRefused() throws Exception {


    	
    	for (int i = 0; i < 5; i++) {
    		System.out.println("LOOP:" + i);
    		// call on B
    		proxyToService.twoWay("first");
    		
    		// Add C
    		client.refreshAddresses(proxyFactoryC.getAddress().toString());
    		
    		// kill B
    		proxyFactoryB.stop();
    		transportFactoryB.stop();
    		Thread.sleep(100);
    		
    		// call on C
    		proxyToService.twoWay("first");
    		
    		assertEquals(i +1, dummyServiceB.instanceCallCount);
    		assertEquals(i +1, dummyServiceC.instanceCallCount);
    		
    		// Create a NEW B
    		transportFactoryB = new TransportFactoryImpl(executorB, "test");
            proxyFactoryB = new ProxyFactoryImpl(transportFactoryB, TransportFactoryImpl.getDefaultProtocolURI("", "localhost", Config.TEST_PORT + 11000 + i + 10, "testServiceB"), executorB, "");
            proxyFactoryB.registerMethodReceiver("methodReceiver", dummyServiceB);
            proxyFactoryB.start();
            
            // Kill C - to revert to B again = dont kill C
            client.removeAddress(proxyFactoryC.getAddress().toString());
            client.refreshAddresses(proxyFactoryB.getAddress().toString());
            
    	}
    }
}
