package com.liquidlabs.transport.proxy;

import com.liquidlabs.common.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import junit.framework.TestCase;

import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.TransportFactoryImpl;
import com.liquidlabs.transport.serialization.Convertor;

public class ProxyRemoteBigSendReceiveTest extends TestCase {
	private ProxyFactoryImpl proxyFactoryA;
	boolean enableOutput = false;
	private ProxyFactoryImpl proxyFactoryB;
	private URI proxyBAddress;
	private DummyService remoteService;
	TransportFactory transportFactory = new TransportFactoryImpl(Executors.newFixedThreadPool(5), "test");
	ExecutorService executor = Executors.newFixedThreadPool(5);
	Convertor c = new Convertor();
	private DummyServiceImpl dummyService;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		transportFactory.start();
		
		proxyFactoryA = new ProxyFactoryImpl(transportFactory,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", 11111, "testServiceA"), executor, "");
		proxyFactoryA.start();
		
		Thread.sleep(100);
		
		proxyFactoryB = new ProxyFactoryImpl(transportFactory,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", 22222, "testServiceB"), executor, "");
		dummyService = new DummyServiceImpl();
		proxyFactoryB.registerMethodReceiver("methodReceiver", dummyService);
		proxyFactoryB.start();
		
		Thread.sleep(100);
		proxyBAddress = proxyFactoryB.getAddress();
		DummyServiceImpl.callCount = 0;
		remoteService = proxyFactoryA.getRemoteService("methodReceiver", DummyService.class, new String[] { proxyBAddress.toString() } );
	}
	@Override
	protected void tearDown() throws Exception {
		transportFactory.stop();
		proxyFactoryA.stop();
		proxyFactoryB.stop();
		dummyService.stop();

		Thread.sleep(50);
	}
	
	public void testTwoWay() throws Exception {
		StringBuilder arg = new StringBuilder();
		for (int i = 0; i < 4 * 1024; i++) {
//		for (int i = 0; i < 1; i++) {
			arg.append("line:" + i + "\n");
		}
		for (int i = 0; i <100; i++) {
			String result = remoteService.twoWay(arg.toString());
			assertNotNull(result);
		}
//		System.out.println(result);
	}
}
