package com.liquidlabs.transport.proxy;

import com.liquidlabs.common.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import junit.framework.TestCase;

import com.liquidlabs.transport.Config;
import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.TransportFactoryImpl;

public class PeerStreamingTest extends TestCase {
	private ProxyFactoryImpl proxyFactoryA;
	boolean enableOutput = false;
	private ProxyFactoryImpl proxyFactoryB;
	private URI proxyBAddress;
	private DummyService remoteService;
	TransportFactory transportFactory = new TransportFactoryImpl(Executors.newFixedThreadPool(5), "test");
	ExecutorService executor = Executors.newFixedThreadPool(5);
	private static final Logger LOGGER = Logger.getLogger(PeerStreamingTest.class);

	
	@Override
	protected void tearDown() throws Exception {
		proxyFactoryA.stop();
		proxyFactoryB.stop();
		super.tearDown();
	}
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		proxyFactoryA = new ProxyFactoryImpl(transportFactory,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", Config.TEST_PORT, "testServiceA"), executor, "");
		proxyFactoryA.start();
		
		Thread.sleep(100);
		
		proxyFactoryB = new ProxyFactoryImpl(transportFactory,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", Config.TEST_PORT+10000, "testServiceB"), executor, "");
		
		proxyFactoryB.registerMethodReceiver("methodReceiver", new DummyServiceImpl());
		proxyFactoryB.start();
		proxyBAddress = proxyFactoryB.getAddress();
		DummyServiceImpl.callCount = 0;
		remoteService = proxyFactoryA.getRemoteService("methodReceiver", DummyService.class, new String[] { proxyBAddress.toString() });
	}
	
	public void testStreamInvocationsWork() throws Exception {
		
		// prime the sockets
		remoteService.twoWay("");
		DummyServiceImpl.callCount = 0;
		
		NotifyInterface notifyInterface = new NotifyInterface(){
			public void notify(String payload) {
				System.err.println("NotifyInterface called:" + payload +  "-" + DummyServiceImpl.callCount++);
			}

			public String getId() {
				return "stuff";
			}
		};
		ContinuousEventListener eventListener = new ContinuousEventListener("notifyId", notifyInterface);
		proxyFactoryA.registerContinuousEventListener("notifyId", eventListener );
		
		
		LOGGER.info("\n\n\t\t -- Client Calling:" + new DateTime());
		remoteService.makeCallbackHappend(proxyFactoryA.getAddress().toString());
		
		Thread.sleep(500);
		assertEquals(11, DummyServiceImpl.callCount);
	}
}
