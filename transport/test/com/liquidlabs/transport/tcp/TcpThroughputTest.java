package com.liquidlabs.transport.tcp;

import com.liquidlabs.common.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import junit.framework.TestCase;

import org.joda.time.DateTime;

import com.liquidlabs.common.BinConvertor;
import com.liquidlabs.transport.Receiver;
import com.liquidlabs.transport.protocol.Type;
import com.liquidlabs.transport.tcp.TcpReceiver;
import com.liquidlabs.transport.tcp.TcpSender;

public class TcpThroughputTest extends TestCase {
	
	
	private TestReceiver testReceiver;
	private TcpReceiver tcpReceiver;
	private TcpSender tcpSender;
	List<byte[]> received = new ArrayList<byte[]>();
	private URI endPoint;
	private int receiveCount;
	private boolean isReplyExpected = false;
	private long timeoutSeconds = 10;


	@Override
	protected void setUp() throws Exception {
		super.setUp();
		testReceiver = new TestReceiver();
		endPoint = new URI("tcp://localhost:9999");
		tcpReceiver = new TcpReceiver(endPoint, testReceiver, Executors.newFixedThreadPool(3));
		tcpReceiver.start();
		tcpSender = new TcpSender(new URI("tcp://locahost:1111"));
		tcpSender.start();
	}
	@Override
	protected void tearDown() throws Exception {
		tcpReceiver.stop();
		tcpSender.stop();
	}
	
	public void XtestIntConversion() throws Exception {
		int value = Integer.MAX_VALUE;
		byte[] intToByteArray = BinConvertor.intToByteArray(value, new byte[4], 0);
		int byteArrayToInt = BinConvertor.byteArrayToInt(intToByteArray, 0);
		assertEquals(value, byteArrayToInt);
		
	}

	public void testShouldStartSendStop() throws Exception {
		String payload = "ImSendingStuff";
		DateTime start = new DateTime();
		int amount = 15000 * 10;
		for (int i = 0 ; i < amount ; i++) {
			tcpSender.send("tcp", endPoint, payload.getBytes(), Type.REQUEST, isReplyExpected, timeoutSeconds, "methodName", false);
		}
		Thread.sleep(500);
		tcpReceiver.stop();
		tcpSender.stop();
		DateTime end = new DateTime();
		long elapse = end.getMillis() - start.getMillis();
		double throughput = amount/(elapse/1000);
		System.err.println("Throughput:" + throughput + " elapse:" + elapse);
		assertTrue("nothing was received", receiveCount > 0);
		assertEquals("Expected:" + amount + " but got:" + receiveCount, amount, receiveCount);
		
	}
	
	public class TestReceiver implements Receiver {


		public byte[] receive(byte[] payload, String remoteAddress, String remoteHostname) {
			receiveCount++;
			if (receiveCount % 10000 == 0) {
				System.out.println(receiveCount + " Received:" + new String(payload));
			}
//			received.add(payload);
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
