package com.liquidlabs.transport.tcp;

import com.liquidlabs.common.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import junit.framework.TestCase;

import com.liquidlabs.transport.Receiver;
import com.liquidlabs.transport.protocol.Type;
import com.liquidlabs.transport.tcp.TcpReceiver;
import com.liquidlabs.transport.tcp.TcpSender;

public class TcpSendReceiveTest extends TestCase {
	
	
	private TestReceiver testReceiver;
	private TcpReceiver tcpReceiver;
	private TcpSender tcpSender;
	List<byte[]> received = new ArrayList<byte[]>();
	private URI endPoint;
	private boolean isReplyExpected = false;
	private long timeoutSeconds = 10;


	@Override
	protected void setUp() throws Exception {
		super.setUp();
		testReceiver = new TestReceiver();
		endPoint = new URI("tcp://localhost:9999");
		tcpReceiver = new TcpReceiver(endPoint, testReceiver, Executors.newFixedThreadPool(3));
		tcpReceiver.start();
		tcpSender = new TcpSender(new URI("tcp://locahost:11999"));
		tcpSender.start();
	}
	@Override
	protected void tearDown() throws Exception {
		tcpReceiver.stop();
		tcpSender.stop();
	}
	
	public void testShouldStartSendStop() throws Exception {
		String payload = "ImSendingStuff";
		tcpSender.send("tcp", endPoint, payload.getBytes(), Type.REQUEST, isReplyExpected, timeoutSeconds, "methodName", true);
		Thread.sleep(500);
		tcpReceiver.stop();
		tcpSender.stop();
		assertTrue("nothing was received", received.size() > 0);
		assertEquals(payload, new String(received.get(0)));
		
	}
	public void testShouldSend3Packets() throws Exception {
		String payload = "ImSendingStuffssss";
		tcpSender.send("tcp", endPoint, payload.getBytes(), Type.REQUEST, isReplyExpected, timeoutSeconds, "methodName", true);
		tcpSender.send("tcp", endPoint, payload.getBytes(), Type.REQUEST, isReplyExpected, timeoutSeconds, "methodName", true);
		tcpSender.send("tcp", endPoint, payload.getBytes(), Type.REQUEST, isReplyExpected, timeoutSeconds, "methodName", true);
		tcpSender.send("tcp", endPoint, payload.getBytes(), Type.REQUEST, isReplyExpected, timeoutSeconds, "methodName", true);
		tcpSender.send("tcp", endPoint, payload.getBytes(), Type.REQUEST, isReplyExpected, timeoutSeconds, "methodName", true);
		Thread.sleep(500);
		tcpReceiver.stop();
		tcpSender.stop();
		assertTrue("nothing was received", received.size() > 0);
		assertEquals(payload, new String(received.get(0)));
		
	}
	
	public class TestReceiver implements Receiver {

		public byte[] receive(byte[] payload, String remoteAddress, String remoteHostname) {
			System.out.println("Received:" + new String(payload));
			received.add(payload);
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
