package com.liquidlabs.transport.tcp;

import com.liquidlabs.common.net.URI;
import java.util.concurrent.Executors;

import junit.framework.TestCase;

import com.liquidlabs.transport.EndPoint;
import com.liquidlabs.transport.EndPointFactory;
import com.liquidlabs.transport.Receiver;
import com.liquidlabs.transport.protocol.Type;
import com.liquidlabs.transport.proxy.RetryInvocationException;

public class TcpEndPointFactoryTest extends TestCase {
	
	
	private int callCount;
	private EndPointFactory epFactory;
	private EndPoint firstEP;
	private EndPoint secondEP;

	private boolean isReplyExpected = false;
	private long timeoutSeconds = 10;

	@Override
	protected void setUp() throws Exception {
		
		epFactory = new TcpEndPointFactory(Executors.newFixedThreadPool(10));
		
		firstEP = epFactory.getEndPoint(new URI("tcp://localhost:11111/stuff"), new MyReceiverA());
		firstEP.start();
		
		secondEP = epFactory.getEndPoint(new URI("tcp://localhost:22222/stuff"), new MyReceiverB());
		secondEP.start();
		
		Thread.sleep(500);
	}
	
	public void testShouldNotHANG() throws Exception {

		for (int i = 0; i < 10; i++) {
			callCount = 0;
			firstEP.send("tcp", secondEP.getAddress(), "notify".getBytes(), Type.REQUEST, isReplyExpected, timeoutSeconds, "methodName", true);
			Thread.sleep(500);
			assertTrue("CallCount was:" + callCount, callCount == 3);
		}
	}

	public class MyReceiverA implements Receiver {


		public byte[] receive(byte[] payload, String remoteAddress, String remoteHostname) {
			callCount++;
			System.out.println(callCount + "****************** A Received:" + new String(payload));
			try {
				Thread.sleep(100);
				if (callCount == 2) {
					System.out.println(callCount + "****************** A Sending listenerID:" + new String(payload));
					firstEP.send("tcp", secondEP.getAddress(), "returning - someListenerId".getBytes(), Type.REQUEST, isReplyExpected, timeoutSeconds, "methodName", true);
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
			System.out.println(callCount + "*************** B Received:" + new String(payload));
			try {
				if (callCount == 1) {
					System.out.println(callCount + "*************** B Asking A for ListenerId");					
					secondEP.send("tcp", firstEP.getAddress(), "getListenerId".getBytes(), Type.REQUEST, isReplyExpected, timeoutSeconds, "methodName", true);
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

}
