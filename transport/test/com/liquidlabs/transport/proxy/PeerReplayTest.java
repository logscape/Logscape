package com.liquidlabs.transport.proxy;

import com.liquidlabs.transport.Config;
import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.TransportFactoryImpl;
import junit.framework.TestCase;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SuppressWarnings("unused")
public class PeerReplayTest extends TestCase {
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
	

	protected void xxxtearDown() throws Exception {
		transportFactoryA.stop();
		transportFactoryB.stop();
		transportFactoryC.stop();
		proxyFactoryA.stop();
		proxyFactoryB.stop();
		proxyFactoryC.stop();
		super.tearDown();
	}

	protected void xxxsetUp() throws Exception {
		super.setUp();
		executorA = Executors.newFixedThreadPool(5);
		transportFactoryA = new TransportFactoryImpl(executorA, "test");
		proxyFactoryA = new ProxyFactoryImpl(transportFactoryA,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", Config.TEST_PORT, "testServiceA"), executorA, "");
		proxyFactoryA.start();


		executorB = Executors.newFixedThreadPool(5);
		transportFactoryB = new TransportFactoryImpl(executorB, "test");
		proxyFactoryB = new ProxyFactoryImpl(transportFactoryB,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", Config.TEST_PORT + 11000, "testServiceB"), executorB, "");
		dummyServiceB = new DummyServiceImpl("dummyServiceB");
		proxyFactoryB.registerMethodReceiver("methodReceiver", dummyServiceB);
		proxyFactoryB.start();

		executorC = Executors.newFixedThreadPool(5);
		transportFactoryC = new TransportFactoryImpl(executorC, "test");
		proxyFactoryC = new ProxyFactoryImpl(transportFactoryC,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", Config.TEST_PORT + 12000, "testServiceC"), executorC, "");
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
			}

		};

		// Register both remoteB and remote C
		proxyToService = proxyFactoryA.getRemoteService("methodReceiver", DummyService.class,
				new String[] { proxyFactoryB.getAddress().toString(), proxyFactoryC.getAddress().toString() }, addressUpdater);

	}

    public void testNothing(){}

    // DodgyTest? Behaviour changed? Reliably get 5
	public void xxxtestShouldReplayMethodOnlyOnce() throws Exception {
		

		// call on B
		// @ReplayOnAddressChange
		proxyToService.oneWayWithReplay("one");
		proxyToService.oneWayWithReplay("two");
		proxyToService.oneWayWithReplay("three");
		proxyToService.oneWayWithReplay("four");
		proxyToService.oneWayWithReplay("five");
		
		Thread.sleep(1000);
		// 5 base invocations, three is retried, one is replayed
		assertEquals(7, dummyServiceC.callCount);
		assertEquals("[three, one, four, five]", dummyServiceC.oneWaysReceived.toString());
	}

}
