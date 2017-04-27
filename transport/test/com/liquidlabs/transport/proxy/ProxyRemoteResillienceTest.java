package com.liquidlabs.transport.proxy;

import java.net.URISyntaxException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.TransportFactoryImpl;
import com.liquidlabs.transport.serialization.Convertor;

/**
 * Tests the a message will get through even though the endpoint if Down_UP_Down while the client is trying to repeatedly send
 *
 */
public class ProxyRemoteResillienceTest {
	private ProxyFactoryImpl proxyFactoryA;
	boolean enableOutput = false;
	private ProxyFactoryImpl proxyFactoryB;
	private URI proxyBAddress;
	private DummyService remoteService;
	TransportFactory transportFactoryA = new TransportFactoryImpl(Executors.newFixedThreadPool(5), "test");
	TransportFactory transportFactoryB = null;
	ExecutorService executor = Executors.newFixedThreadPool(5);
	Convertor c = new Convertor();
	
	@Before
	public void setUp() throws Exception {
		transportFactoryA.start();
		
		proxyFactoryA = new ProxyFactoryImpl(transportFactoryA,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", 11111, "testServiceA"), executor, "");
		proxyFactoryA.start();
		
		proxyBAddress = new URI("stcp://localhost:22222");
		
		
//		makeRemoteProxyService();
		DummyServiceImpl.callCount = 0;
		remoteService = proxyFactoryA.getRemoteService("methodReceiver", DummyService.class, new String[] { proxyBAddress.toString() } );
	}
	@After
	public void tearDown() throws Exception {
		transportFactoryA.stop();
		proxyFactoryA.stop();
		proxyFactoryB.stop();
		Thread.sleep(50);
	}
	
	@Test
	public void testTwoWay() throws Exception {
		int ex = 0;
		int success = 0;
		boolean enabled = false;
		for (int i = 0; i <10; i++) {
			try {
				if (i == 2 || i == 6 || i == 10) {
					enabled = true;
					makeRemoteProxyService(i);
				}
				if (i == 4 || i ==8) {
					enabled = false;
					destroyRemoteService(i);
				}
				System.out.println("===============> MSG:" + i + " SUCCESS:" + success + " FAIL:" + ex);
				String result = remoteService.twoWay("MSG:" + i);
				System.out.println("SUCCESS:" + i +  " enabled:" + enabled);
				success++;
				Assert.assertEquals("SUCCESS Should only occur when ENABLED i:" + i, true, enabled);
			} catch (Throwable t) {
				t.printStackTrace();
				Assert.assertEquals("ERROR Should only occur when DISABLED i:" + i, false, enabled);
				
				ex++;
				System.out.println("FAILED:" + i + " enabled:" + enabled);
				t.printStackTrace();
			}
		}
		Assert.assertEquals("FAILED  WRONG!!", 6, ex);
		Assert.assertEquals("SUCCESS WRONG!!", 4, success);
	}
	private void makeRemoteProxyService(int i) throws URISyntaxException {
		System.out.println("ENABLE ========================== :" + i);
		this.transportFactoryB = new TransportFactoryImpl(Executors.newFixedThreadPool(5), "test");
		proxyFactoryB = new ProxyFactoryImpl(transportFactoryB,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", 22222, "testServiceB"), executor, "");
		proxyFactoryB.registerMethodReceiver("methodReceiver", new DummyServiceImpl());
		proxyFactoryB.start();
		
		proxyBAddress = proxyFactoryB.getAddress();
	}

	private void destroyRemoteService(int i) {
		System.out.println(" DISABLE ======================= :" + i);
		this.transportFactoryB.stop();
		proxyFactoryB.stop();
		
	}
}
