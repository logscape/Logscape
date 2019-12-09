package com.liquidlabs.transport.netty;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.concurrent.NamingThreadFactory;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.Receiver;
import com.liquidlabs.transport.Sender;
import com.liquidlabs.transport.protocol.Type;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.*;

public class NettySendReplyTest {
	
	private Sender sender;
	private NettyReceiver receiver;
	private URI receiverAddress;
	int callCount;
	List<String> results = new ArrayList<String>();
	private ExecutorService exec1;
	private ExecutorService exec2;
	private NioClientSocketChannelFactory factory1;
	private NioServerSocketChannelFactory factory2;
    private CountDownLatch countDownLatch;
	private ScheduledExecutorService scheduler;

	@Before
	public void setUp() throws Exception {

		System.setProperty("port.scan.debug","true");
		
		System.setProperty("vso.client.port.restrict", "false");
		
		exec1 = Executors.newCachedThreadPool(new NamingThreadFactory("JB-Sender", true, Thread.NORM_PRIORITY + 1));
		exec2 = Executors.newCachedThreadPool(new NamingThreadFactory("WRPLY-Sender", true, Thread.NORM_PRIORITY + 1));
		factory1 = new NioClientSocketChannelFactory(exec1, exec2);
		factory2 = new NioServerSocketChannelFactory(exec1, exec2);

		scheduler = Executors.newScheduledThreadPool(2);


		sender = new NettySenderFactoryProxy(new URI("tcp://localhost:" + new NetworkUtils().determinePort(9000)), new NettySimpleSenderFactory(factory1, scheduler));

		sender.start();
		receiverAddress = new URI("tcp://localhost:" + new NetworkUtils().determinePort(10000));
        countDownLatch = new CountDownLatch(4);
        receiver = new NettyReceiver(receiverAddress, factory2, new LLProtocolParser(new MyReceiver()));
		receiver.start();
	}
	@After
	public void tearDown() throws Exception {
		scheduler.shutdown();
		sender.stop();
		receiver.stop();
	}

	@Test
	public void testShouldSendAMessage() throws Exception {
		boolean isReplyExpected = true;
		long timeoutSeconds = 10;
		byte[] send = sender.send("tcp", receiverAddress, "1 stuff".getBytes(), Type.SEND_REPLY, isReplyExpected, timeoutSeconds, "methodName", true);
		assertNotNull(send);

		send = sender.send("tcp", receiverAddress, "2 stuff".getBytes(), Type.SEND_REPLY, isReplyExpected, timeoutSeconds, "methodName", true);
		send = sender.send("tcp", receiverAddress, "3 stuff".getBytes(), Type.SEND_REPLY, isReplyExpected, timeoutSeconds, "methodName", true);
		send = sender.send("tcp", receiverAddress, "4 stuff".getBytes(), Type.SEND_REPLY, isReplyExpected, timeoutSeconds, "methodName", true);

		assertTrue("CallCount was:" + callCount, callCount == 4);
		assertEquals("1 stuff", results.get(0));
		assertEquals("2 stuff", results.get(1));
		assertEquals("3 stuff", results.get(2));
		assertEquals("4 stuff", results.get(3));
	}
	
	public class MyReceiver implements Receiver {

		synchronized public byte[] receive(byte[] payload, String remoteAddress, String remoteHostname) {
			results.add(new String(payload));
//			System.out.println(">>>>>>>>>>>> GOT:" + new String(payload));
			return ("Response send:" + callCount++).getBytes();
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
