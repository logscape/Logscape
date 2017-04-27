package com.liquidlabs.transport.proxy;

import com.liquidlabs.transport.Config;
import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.TransportFactoryImpl;
import junit.framework.TestCase;

import java.net.URISyntaxException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@SuppressWarnings("unused")
public class ProxyRemoteListenerFailFastReregisterTest extends TestCase {
	private ProxyFactoryImpl proxyFactoryA;

	private ProxyFactoryImpl proxyFactoryB;
	private DummyServiceImpl dummyServiceB;

	ProxyClient<?> client;

	private DummyService proxyToService;

	private AddressUpdater addressUpdater;

	private TransportFactory transportFactoryA;

	private TransportFactoryImpl transportFactoryB;

	private ExecutorService executorA;
	private ExecutorService executorB;
	

	@Override
	protected void tearDown() throws Exception {
		transportFactoryA.stop();
		proxyFactoryA.stop();
		killB();
		super.tearDown();
	}

	private void killB() {
		transportFactoryB.stop();
		proxyFactoryB.stop();
	}

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		executorA = Executors.newFixedThreadPool(5);
		transportFactoryA = new TransportFactoryImpl(executorA, "test");
		proxyFactoryA = new ProxyFactoryImpl(transportFactoryA,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", Config.TEST_PORT, "testService"), executorA, "");
		proxyFactoryA.start();


		setupServiceB();

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
				new String[] { proxyFactoryB.getAddress().toString() }, addressUpdater);

		System.out.println("=================================" + getName()
				+ "============================================");
		Thread.sleep(100);
	}

	
	int count = 0;
	CountDownLatch latch = new CountDownLatch(5);

	public void testShouldReconnectWhenOnlyOneEndPointAndServiceIsBounced() throws Exception {

        NotifyInterface myListener = new NotifyInterface(){
			public String getId() {
				return "idddddddd";
			}
			public void notify(String payload) {
				System.out.println(count + " Received:" + payload);
				if (count++ == 5) {
					System.out.println("boom!!!!!!");
					throw new RuntimeException("--BOOM--");
				}
                latch.countDown();
			}
		};
		
		// call on B - only receives 5 then blows up
		System.out.println("================= ROUND 1 =====================");
		proxyToService.callBackOnMeXtimes(myListener, 10);


        assertThat(latch.await(10, TimeUnit.SECONDS), is(true));

		latch = new CountDownLatch(5);
		System.out.println("================= ROUND 2 =====================");
		proxyToService.callBackOnMeXtimes(myListener, 10);

        assertThat(latch.await(10, TimeUnit.SECONDS), is(true));

	}
	
	private void setupServiceB() throws URISyntaxException, InterruptedException {
		executorB = Executors.newFixedThreadPool(5);
		transportFactoryB = new TransportFactoryImpl(executorB, "test");
		proxyFactoryB = new ProxyFactoryImpl(transportFactoryB,  transportFactoryB.getDefaultProtocolURI("", "localhost", Config.TEST_PORT + 11000, "testService"), executorB, "");
		dummyServiceB = new DummyServiceImpl("dummyServiceB");
		proxyFactoryB.registerMethodReceiver("methodReceiver", dummyServiceB);
		proxyFactoryB.start();
		Thread.sleep(1000);
	}

	
}
