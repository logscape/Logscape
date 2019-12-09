package com.liquidlabs.transport.netty;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.concurrent.NamingThreadFactory;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.Receiver;
import com.liquidlabs.transport.protocol.Type;
import junit.framework.TestCase;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class NettySendRAWTest extends TestCase {
	
	private NettySenderFactoryProxy sender;
	private NettyReceiver receiver;
	private URI receiverAddress;
	int callCount;
	List<String> results = new ArrayList<String>();
	private ExecutorService exec1;
	private ExecutorService exec2;
	private NioClientSocketChannelFactory factory1;
	private NioServerSocketChannelFactory factory2;
	private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

	protected void xxxsetUp() throws Exception {
		super.setUp();
		
		exec1 = Executors.newCachedThreadPool(new NamingThreadFactory("JB-Sender", true, Thread.NORM_PRIORITY + 1));
		exec2 = Executors.newCachedThreadPool(new NamingThreadFactory("WRPLY-Sender", true, Thread.NORM_PRIORITY + 1));
		factory1 = new NioClientSocketChannelFactory(exec1, exec2);
		factory2 = new NioServerSocketChannelFactory(exec1, exec2);

		
		sender = new NettySenderFactoryProxy(new URI("raw://localhost:" + new NetworkUtils().determinePort(9000)), new NettyPoolingSenderFactory(factory1, scheduler));
		sender.start();
		receiverAddress = new URI("raw://localhost:" +  new NetworkUtils().determinePort(10000));
		receiver = new NettyReceiver(receiverAddress, factory2, new StringProtocolParser(new MyReceiver()));
		receiver.start();
	}
	
	protected void xxxtearDown() throws Exception {
		scheduler.shutdown();
		sender.stop();
		receiver.stop();
	}

	// TODO: fix test
    public void testShould() {
                     // place holder
    }
    // DodgyTest? Doesn't work and do we even use it?
	public void xxxtestShouldSendAMessage() throws Exception {
		boolean isReplyExpected = false;
		long timeoutSeconds = 10;
		sender.send("raw", receiverAddress, "1 stuff\n".getBytes(), Type.REQUEST, isReplyExpected, timeoutSeconds, "methodName", true);
		sender.send("raw", receiverAddress, "2 stuff\n".getBytes(), Type.REQUEST, isReplyExpected, timeoutSeconds, "methodName", true);
		sender.send("raw", receiverAddress, "3 stuff\n".getBytes(), Type.REQUEST, isReplyExpected, timeoutSeconds, "methodName", true);
		sender.send("raw", receiverAddress, "4 stuff\n".getBytes(), Type.REQUEST, isReplyExpected, timeoutSeconds, "methodName", true);
		Thread.sleep(500);
		assertTrue("CallCount was:" + callCount, callCount > 0);
		assertTrue(results.get(0).contains("1 stuff"));
	}
	
	public class MyReceiver implements Receiver {

		public byte[] receive(byte[] payload, String remoteAddress, String remoteHostname) {
			System.err.println(callCount++ + " Receiver got:" + new String(payload));
			results.add(new String(payload));
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
