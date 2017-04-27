package com.liquidlabs.transport.proxy;

import com.liquidlabs.transport.Config;
import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.TransportFactoryImpl;
import junit.framework.TestCase;

import java.net.URISyntaxException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SuppressWarnings("unused")
public class PeerDisconnectedRestartedTest extends TestCase {
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

		System.out.println("=================================" + getName() + "============================================");
		Thread.sleep(100);
	}

	

	public void testShouldReconnectWhenOnlyOneEndPointAndServiceIsBounced() throws Exception {

			// call on B
			String first = proxyToService.twoWayWithPause("first", 1000);
			
			System.out.println("================= killing B ==================");
			
			// kill B
			killB();
			
			System.out.println("================== making bad call ===========");

			// make call while its down
			Thread.sleep(100);
			
			try {
			String bad = proxyToService.twoWayWithPause("bad", 1000);
			} catch (Throwable t){
				t.printStackTrace();
			}
			
			Thread.sleep(10 * 1000);
			
			System.out.println("================= starting B ==================");
			
			// start it up again
			setupServiceB();
			
			System.out.println("================= killing B ==================");
			
			// call again and get auto-redirect to C
			String second = proxyToService.twoWayWithPause("second", 1000);

			assertNotNull("Got null on First call", first);
			assertNotNull("Got null on Second call", second);
			
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
