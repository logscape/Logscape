package com.liquidlabs.transport.rabbit;

import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.EndPoint;
import com.liquidlabs.transport.Receiver;
import com.liquidlabs.transport.protocol.Type;
import com.liquidlabs.transport.proxy.RetryInvocationException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;

//import java.net.URI;

public class RabbitEndPointTest {
	private int callCount;
	private RabbitEndpointFactory epFactory;
	private EndPoint firstEP;
	private EndPoint secondEP;
	boolean rabbitEnabled = false;


//	public static Server myServer= new Server();

//	@ClassRule
//	public static GenericContainer rabbitMqContainer = new GenericContainer("rabbitmq:3.7.4")
//			.withExposedPorts(5672)
//			.withEnv(makeMap(new SimpleEntry("RABBITMQ_DEFAULT_USER", "root"),	new SimpleEntry("RABBITMQ_DEFAULT_PASS","toor"))).waitingFor(Wait.forListeningPort());


	@Before
	public void setUp() throws Exception {
		Thread.sleep(200);
	}

	@After
	public void tearDown() throws Exception {
		System.out.println("Stopping.....");
		if (epFactory != null) {
			epFactory.stop();
			secondEP.stop();
			firstEP.stop();
		}
	}

	@Test
	public void testRabbitWorks() throws Exception {
		if (!rabbitEnabled) return;
	    
        RConfig config = new RConfig("192.168.99.100", 5672, "guest", "guest");
        RSender sender = new RSender(config, "myQueue");
		sender.sendMessage("first");
		sender.sendMessage("second");
		sender.sendMessage("third");
		List<String> cache = new ArrayList<>();
		new RReceiver(cache, config, "myQueue").receive();
		Thread.sleep(1000);
		System.out.println(cache);
		
	}

	@Test
	public void testEndpointWorks() throws Exception {
		if (!rabbitEnabled) return;

		epFactory = new RabbitEndpointFactory("amqp://guest:guest@192.168.99.100:5672");

		firstEP = epFactory.getEndPoint(new URI("tcp://localhost:11113/stuff"), new MyReceiverA());
		firstEP.start();

		secondEP = epFactory.getEndPoint(new URI("tcp://localhost:22223/stuff"), new MyReceiverB());
		secondEP.start();

		for (int i = 0; i < 10; i++) {
			System.out.println("Sending message");
			callCount = 0;
			firstEP.send("tcp", secondEP.getAddress(), (i + "-notify").getBytes(), Type.REQUEST, false, 10, "methodName", false);
			Thread.sleep(500);
			assertThat("CallCount was:" + callCount, callCount == 3);
		}

		Thread.sleep(500);
	}

	
	public class MyReceiverA implements Receiver {

		public byte[] receive(byte[] payload, String remoteAddress, String remoteHostname) {
			callCount++;
			System.out.println(Thread.currentThread().getName() + " " + callCount + "****************** A Received:" + new String(payload));
			try {
				Thread.sleep(10);
				if (callCount == 2) {
					System.out.println(Thread.currentThread().getName() + " " + callCount + "****************** A Sending listenerID:" + new String(payload));
					firstEP.send("tcp", secondEP.getAddress(), "returning - someListenerId".getBytes(), Type.REQUEST, false, 10, "methodName", false);
				} else {
					System.out.println("A Nothing doing: " + callCount);
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
			System.out.println(Thread.currentThread().getName() + " " + callCount + "*************** B Received:" + new String(payload));
			try {
				Thread.sleep(100);
				if (callCount == 1) {
					System.out.println(Thread.currentThread().getName() + " " + callCount + "*************** B Asking A for ListenerId");
					secondEP.send("tcp", firstEP.getAddress(), "getListenerId".getBytes(), Type.REQUEST, false, 10, "methodName", false);
				} else {
					System.out.println("B Nothing doing: " + callCount);
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
