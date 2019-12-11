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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class NettySendRecTest {
	
	private NettySenderFactoryProxy sender;
	private NettyReceiver receiver;
	private URI receiverAddress;
	int callCount;
	List<String> results = new CopyOnWriteArrayList<>();
	private ExecutorService exec1;
	private ExecutorService exec2;
	private ClientSocketChannelFactory factory1;
	private NioServerSocketChannelFactory factory2;
	private boolean allowLocalRoute = false;
    private CountDownLatch countDownLatch;
	private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

	@Before
	public void setUp() throws Exception {

		System.setProperty("vso.client.port.restrict", "false");
		
		exec1 = Executors.newCachedThreadPool(new NamingThreadFactory("JB-Sender", true, Thread.NORM_PRIORITY + 1));
		exec2 = Executors.newCachedThreadPool(new NamingThreadFactory("WRPLY-Sender", true, Thread.NORM_PRIORITY + 1));
		factory1 = new NioClientSocketChannelFactory(exec1, exec2, 4);
		factory2 = new NioServerSocketChannelFactory(exec1, exec2);

		String address = NetworkUtils.getIPAddress();

		sender = new NettySenderFactoryProxy(new URI("tcp://" + address + ":" + new NetworkUtils().determinePort(11111)), new NettyPoolingSenderFactory(factory1, scheduler));
		sender.start();

		receiverAddress = new URI("tcp://" + address + ":" +  new NetworkUtils().determinePort(22222));
		System.out.println("Receiver Address:" + receiverAddress);
        countDownLatch = new CountDownLatch(4);
        receiver = new NettyReceiver(receiverAddress, factory2, new LLProtocolParser(new MyReceiver(countDownLatch)));
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

		boolean isReplyExpected = false;
		long timeoutSeconds = 20;

		sender.send("tcp", receiverAddress, "1 stuff".getBytes(), Type.REQUEST, isReplyExpected, timeoutSeconds, "methodName", allowLocalRoute);
		sender.send("tcp", receiverAddress, "2 stuff".getBytes(), Type.REQUEST, isReplyExpected, timeoutSeconds, "methodName", allowLocalRoute);
		sender.send("tcp", receiverAddress, "3 stuff".getBytes(), Type.REQUEST, isReplyExpected, timeoutSeconds, "methodName", allowLocalRoute);

		sender.send("tcp", receiverAddress, "4 stuff".getBytes(), Type.REQUEST, isReplyExpected, timeoutSeconds, "methodName", allowLocalRoute);

        assertThat("Expected to receive 4 messages but didn't (waited 10 seconds) got:" + results, countDownLatch.await(timeoutSeconds, TimeUnit.SECONDS), is(true));
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
