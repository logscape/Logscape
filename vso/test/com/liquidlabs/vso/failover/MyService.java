package com.liquidlabs.vso.failover;

import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.liquidlabs.common.jmx.JmxHtmlServerImpl;
import com.liquidlabs.transport.TransportFactoryImpl;
import com.liquidlabs.transport.proxy.ProxyFactoryImpl;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.LookupSpaceImpl;
import com.liquidlabs.vso.lookup.ServiceInfo;

public class MyService {

	/**
	 * Called from the script
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("Runnnnnnnnning....................................");
		try {
			MyService myService;
			myService = new MyService(args[0]);
			myService.waitABit();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}

	private ProxyFactoryImpl proxyFactory;
	private LookupSpace lookupSpace;
	private TransportFactoryImpl transport;
	private String location = "DEFAULT";

	public MyService(String lookupSpaceAddress) throws UnknownHostException, URISyntaxException {
		ExecutorService executor = Executors.newFixedThreadPool(5);
		transport = new TransportFactoryImpl(executor, "test");
		transport.start();
		proxyFactory = new ProxyFactoryImpl(transport, 20000, executor, "myService");
		proxyFactory.start();
		lookupSpace = LookupSpaceImpl.getRemoteService(lookupSpaceAddress, proxyFactory,"ctx");

		int port = (int) (20000 + (1000 * Math.random()));

		final ServiceInfo serviceInfo = new ServiceInfo("PUBLISHER", "tcp://localhost:" + port, JmxHtmlServerImpl.locateHttpUrL(), location, "");
		lookupSpace.registerService(serviceInfo, -1);

		// Want the unregister to fire when ResourceAgent.processRunner shuts us
		// down
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				System.out.println("MyService:............ TERMINATED................");
				lookupSpace.unregisterService(serviceInfo);
			}
		});
	}

	public void waitABit() {
		try {
			System.out.println("MyService:............ WAITING................");
			Thread.sleep(600 * 1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
