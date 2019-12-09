package com.liquidlabs.transport.netty;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.concurrent.NamingThreadFactory;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.Receiver;
import com.liquidlabs.transport.protocol.Type;
import org.hamcrest.collection.IsCollectionContaining;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class NettySendRecSecuredTest {
	
	private NettySenderFactoryProxy sender;
	private NettyReceiver receiver;
	private URI receiverAddress;
	int callCount;
	List<String> results = new ArrayList<String>();
	private ExecutorService exec1;
	private ExecutorService exec2;
	private ClientSocketChannelFactory factory1;
	private NioServerSocketChannelFactory factory2;
	private boolean allowLocalRoute = false;
    private CountDownLatch countDownLatch;

    private boolean isHandshake = false;
	private ScheduledExecutorService scheduler;

	@Before
	public void setUp() throws Exception {
		//if (true) return;
//		System.setProperty("cert.keystore.file","resources/ssl/.keystore");
		System.setProperty("endpoint.security.port","10000");

		System.setProperty("vso.client.port.restrict", "false");
        System.setProperty("endpoint.security.enabled", "false");

		exec1 = newCachedThreadPool(new NamingThreadFactory("JB-Sender", true, Thread.NORM_PRIORITY + 1));
		exec2 = newCachedThreadPool(new NamingThreadFactory("WRPLY-Sender", true, Thread.NORM_PRIORITY + 1));
		factory1 = new NioClientSocketChannelFactory(exec1, exec2, 2);
		factory2 = new NioServerSocketChannelFactory(exec1, exec2);

		String address = NetworkUtils.getIPAddress();
		System.out.println("addr:" + address);

		scheduler = newScheduledThreadPool(1);
		sender = new NettySenderFactoryProxy(new URI("stcp://" + address + ":" + new NetworkUtils().determinePort(9000)), new NettyPoolingSenderFactory(factory1, scheduler));
		sender.start();
		receiverAddress = new URI("stcp://" + address + ":" +  new NetworkUtils().determinePort(10000));
		System.out.println("Rec Address:" + receiverAddress);
        countDownLatch = new CountDownLatch(4);
        receiver = new NettyReceiver(receiverAddress, factory2, new LLProtocolParser(new MyReceiver(countDownLatch)));
		receiver.start();
	}
	
	@After
	public void tearDown() throws Exception {
		scheduler.shutdown();
		sender.stop();
		receiver.stop();

		System.setProperty("vso.client.port.restrict", "false");
		System.setProperty("endpoint.security.enabled", "false");
	}

	// TODO: FIX TEST IN THE BUILD
	@Test
	public void testShouldSendAMessage() throws Exception {
//    	if (true) return;

        String absolutePath = new File(".").getAbsolutePath();
        String got = new File(".").getAbsolutePath().replace(".", "");
        System.out.println("Path:" + absolutePath);

        boolean isReplyExpected = false;
		long timeoutSeconds = 10;
        System.err.println("1-------------");
        sender.send("stcp", receiverAddress, "1 stuff".getBytes(), Type.REQUEST, isReplyExpected, timeoutSeconds, "methodName", allowLocalRoute);

        System.err.println("2-------------");
		sender.send("stcp", receiverAddress, "2 stuff".getBytes(), Type.REQUEST, isReplyExpected, timeoutSeconds, "methodName", allowLocalRoute);

        System.err.println("3-------------");
		sender.send("stcp", receiverAddress, "3 stuff".getBytes(), Type.REQUEST, isReplyExpected, timeoutSeconds, "methodName", allowLocalRoute);

        System.err.println("4-------------");
		sender.send("stcp", receiverAddress, "4 stuff".getBytes(), Type.REQUEST, isReplyExpected, timeoutSeconds, "methodName", allowLocalRoute);

        assertThat("Expected to receive 4 messages but didn't", countDownLatch.await(10L, TimeUnit.SECONDS), is(true));
        assertThat(results, IsCollectionContaining.hasItem("1 stuff"));
		assertThat(results, IsCollectionContaining.hasItem("2 stuff"));
		assertThat(results, IsCollectionContaining.hasItem("3 stuff"));
		assertThat(results, IsCollectionContaining.hasItem("4 stuff"));
	}
	
	public class MyReceiver implements Receiver {

        private final CountDownLatch countDownLatch;

        public MyReceiver(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }

        synchronized public byte[] receive(byte[] payload, String remoteAddress, String remoteHostname) {
			results.add(new String(payload));
            System.err.println(Thread.currentThread().getName() + ">>>>>>>>>> Received:"+ new String(payload));
            System.out.println("SUCCESS:" + new String(payload));
            countDownLatch.countDown();
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
