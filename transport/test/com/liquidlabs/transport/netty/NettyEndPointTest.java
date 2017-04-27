package com.liquidlabs.transport.netty;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.Receiver;
import com.liquidlabs.transport.protocol.Type;
import com.liquidlabs.transport.proxy.RetryInvocationException;
import junit.framework.TestCase;

import java.util.concurrent.Executors;

public class NettyEndPointTest extends TestCase {
	private int callCount;
	
	private NettyEndPointFactory epFactory;

	private NettyEndPoint firstEP;

	private NettyEndPoint secondEP;

	private boolean allowLocalRoute;

	@Override
	protected void setUp() throws Exception {
		
	//	System.setProperty("vso.client.port.restrict", "true");
		
		epFactory = new NettyEndPointFactory(Executors.newScheduledThreadPool(1), "");
		
		firstEP = (NettyEndPoint) epFactory.getEndPoint(new URI("tcp://0.0.0.0:11111/stuff"), new MyReceiverA());
		firstEP.start();
		
		secondEP = (NettyEndPoint) epFactory.getEndPoint(new URI("tcp://0.0.0.0:22222/stuff"), new MyReceiverB());
		secondEP.start();
		
		Thread.sleep(500);
	}
	
	protected void tearDown() throws Exception {
		System.out.println("Stopping.....");
		epFactory.stop();
		secondEP.stop();
		firstEP.stop();
	}
	
	
	public void testShouldNotHANG() throws Exception {


		String defaultIpFromRoutingTable = NetworkUtils.getDefaultIpFromRoutingTable("");

		for (int i = 0; i < 10; i++) {
			callCount = 0;
			firstEP.send("tcp", secondEP.getAddress(), new String(i + "-notify").getBytes(), Type.REQUEST, false, 10, "methodName", allowLocalRoute);
			Thread.sleep(500);
			assertTrue("CallCount was:" + callCount, callCount == 3);
		}
	}

	
	public class MyReceiverA implements Receiver {

		public byte[] receive(byte[] payload, String remoteAddress, String remoteHostname) {
			callCount++;
			System.out.println(Thread.currentThread().getName() + " " + callCount + "****************** A Received:" + new String(payload));
			try {
				Thread.sleep(100);
				if (callCount == 2) {
					System.out.println(Thread.currentThread().getName() + " " + callCount + "****************** A Sending listenerID:" + new String(payload));
					firstEP.send("tcp", secondEP.getAddress(), "returning - someListenerId".getBytes(), Type.REQUEST, false, 10, "methodName", allowLocalRoute);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (RetryInvocationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;
		}

		public void start() {
		}

		public void stop() {
		}
		public boolean isForMe(Object payload) {
			throw new RuntimeException("Not implemented");
		}
		public byte[] receive(Object payload, String remoteAddress, String remoteHostname) {
			throw new RuntimeException("Not implemented");
		}

		
	}
	public class MyReceiverB implements Receiver {
		
		public byte[] receive(byte[] payload, String remoteAddress, String remoteHostname) {
			callCount++;
			System.out.println(Thread.currentThread().getName() + " " +callCount + "*************** B Received:" + new String(payload));
			try {
				Thread.sleep(100);
				if (callCount == 1) {
					System.out.println(Thread.currentThread().getName() + " " +callCount + "*************** B Asking A for ListenerId");					
					secondEP.send("tcp",new URI("tcp://10.28.0.51:11111/stuff"), "getListenerId".getBytes(), Type.REQUEST, false, 10, "methodName", allowLocalRoute);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			return null;
		}
		
		public void start() {
		}
		
		public void stop() {
		}
		public boolean isForMe(Object payload) {
			throw new RuntimeException("Not implemented");
		}
		public byte[] receive(Object payload, String remoteAddress, String remoteHostname) {
			throw new RuntimeException("Not implemented");
		}

		
	}

}
