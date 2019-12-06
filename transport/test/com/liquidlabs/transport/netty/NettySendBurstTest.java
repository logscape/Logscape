package com.liquidlabs.transport.netty;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.concurrent.NamingThreadFactory;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.Receiver;
import com.liquidlabs.transport.Sender;
import com.liquidlabs.transport.SenderFactory;
import com.liquidlabs.transport.TransportProperties;
import com.liquidlabs.transport.protocol.Type;
import junit.framework.TestCase;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.ServerSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.channel.socket.oio.OioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.oio.OioServerSocketChannelFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class NettySendBurstTest extends TestCase {

	public static final int STARTING_PORT = NetworkUtils.determinePort(13000);
	private NettyReceiver receiver;
	private URI receiverAddress;
	AtomicInteger callCount = new AtomicInteger(0);
	List<String> results = new ArrayList<String>();
	private ExecutorService exec1;
	private ExecutorService exec2;
	private ExecutorService exec3;
	private ServerSocketChannelFactory factory2;
	private SenderFactory nettySenderFactory;
	
	// test config settings
	private boolean logMsgs = false;
	private boolean logPooling = false;
	private boolean useNIOServer = true;
	private boolean useNIOClient = false;
	private boolean limitSOCKETPORTS = false;
	
	int msgCountToSend = 1000;
	int concurrentSenders = 2;
	final boolean isReplyExpected = true;

	@Override
	protected void setUp() throws Exception {
		if (limitSOCKETPORTS) {
			System.setProperty(TransportProperties.VSO_CLIENT_PORT_RESTRICT, "true");
		}
		
		exec1 = Executors.newCachedThreadPool(new NamingThreadFactory("JB-Sender", true, Thread.NORM_PRIORITY + 1));
		exec2 = Executors.newCachedThreadPool(new NamingThreadFactory("WRPLY-Sender", true, Thread.NORM_PRIORITY + 1));
		exec3 = Executors.newCachedThreadPool(new NamingThreadFactory("WRPLY-Sender", true, Thread.NORM_PRIORITY + 1));
	
		ClientSocketChannelFactory nettyClientFactory = null;
		if (useNIOServer) {
			factory2 = new NioServerSocketChannelFactory(exec1, exec2);
		} else {
			factory2 = new OioServerSocketChannelFactory(exec1, exec2);
		}
		if (useNIOClient) {
			nettyClientFactory = new NioClientSocketChannelFactory(exec1, exec2);
		} else {
			nettyClientFactory = new OioClientSocketChannelFactory(exec3);
		}
		
		nettySenderFactory = new NettyPoolingSenderFactory(nettyClientFactory, false);
		nettySenderFactory.start();
		
		receiverAddress = new URI("tcp://localhost:" + new NetworkUtils().determinePort(STARTING_PORT));
		receiver = new NettyReceiver(receiverAddress, factory2, new LLProtocolParser(new MyReceiver()), false);
		receiver.start();
	}
	
	@Override
	protected void tearDown() throws Exception {
		receiver.stop();
		nettySenderFactory.stop();
	}
	
	final CountDownLatch countDownLatch = new CountDownLatch(msgCountToSend);

	// TODO: FIX TEST IN THE BUILD
	public void testShouldSendAMessage() throws Exception {
//
//
//
//		final long timeoutSeconds = 10;
//		ExecutorService executor = Executors.newFixedThreadPool(concurrentSenders);
//		for (int i = 0; i < msgCountToSend; i++) {
//			final int id = i;
//			executor.submit(new Runnable() {
//				public void run() {
//					Sender sender2 = null;
//					try {
//						if (logMsgs) System.out.println(id + "---send:" + receiverAddress);
//						sender2 = nettySenderFactory.getSender(receiverAddress, logPooling, true, "");
//						byte[] send = sender2.send("tcp", receiverAddress, (id + "--MSG--").getBytes(), Type.REQUEST, isReplyExpected, timeoutSeconds, "methodName", false);
//						if (isReplyExpected && logMsgs) {
//							if (send != null) {
//								System.out.println("GOT REPLY:" + new String(send));
//							} else {
//								System.out.println("ERROR - NO REPLY");
//							}
//						}
//					} catch (Throwable t) {
//						t.printStackTrace();
//					} finally {
//						nettySenderFactory.returnSender(receiverAddress, sender2, false);
//					}
//
//				};
//			});
//		}
//		long start = System.currentTimeMillis();
//		boolean await = countDownLatch.await(60, TimeUnit.SECONDS);
//		long elapsed = System.currentTimeMillis() - start;
//		double elapsedSec = elapsed/1000.0;
//		System.out.println(String.format("%b Elapsed:%dms %f", await,elapsed, elapsedSec));
//		System.out.println("Rate:" + (msgCountToSend / elapsedSec ) );
				
	}
	
	public class MyReceiver implements Receiver {

		public byte[] receive(byte[] payload, String remoteAddress, String remoteHostname) {
			int count = callCount.incrementAndGet();
			if (logMsgs || count % 10000 == 0) {
				System.out.println("\t" + count + " Receiver got:" + new String(payload) + " ThreadName:" + Thread.currentThread().getName());
			}
			countDownLatch.countDown();
			if (isReplyExpected) return ("RESULT:" + new String(payload)).getBytes();
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
