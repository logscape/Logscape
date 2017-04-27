package com.liquidlabs.transport.proxy;

import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.util.concurrent.Executors;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.Sender;
import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.protocol.Type;
import com.liquidlabs.transport.serialization.Convertor;

public class PeerHandlerTest {
	
	private PeerHandler peerHandler;
	Convertor convertor = new Convertor();
	Mockery context = new Mockery();
	private Sender sender;
	private TransportFactory transportFactory;

	
	@Before
	public void setUp() throws Exception {
		peerHandler = new PeerHandler(convertor, Executors.newScheduledThreadPool(1), Executors.newCachedThreadPool(), "addr", "");
		sender = context.mock(Sender.class);
		peerHandler.setSender(sender);
	}
	
	@Test
	public void testShouldSendReplyWhenSenderURIAndClientURIDO_NOT_Match() throws Exception {
		
		context.checking(new Expectations() {{
			one(sender).getAddress(); will(returnValue(new URI("stcp://localhost:120")));
		}});
		Invocation invocation = new Invocation(getClass().getMethod("hashCode"), null);
		invocation.setClientURI("stcp://localhost:8000/id=stuff");
		Assert.assertTrue(peerHandler.shouldSendReponse(invocation));
	}
	
	
	@Test
	public void testShouldHandleRemoteExceptionWithNullPtr() throws Exception {
		
		Method method = getClass().getMethod("toString");
		peerHandler.setSender(new MySender());
		RuntimeException remoteException = peerHandler.getRemoteException(new Invocation(method, convertor), new NullPointerException());
		
		Assert.assertTrue(remoteException.getMessage().contains("VScape"));
		remoteException.printStackTrace();
	}
	
	public class MySender implements Sender {

		public URI getAddress() {
			try {
				return new URI("stcp://localhost:8000");
			} catch (URISyntaxException e) {
				return null;
			}
		}

		public byte[] send(String protocol, URI endPoint, byte[] bytes, Type type, boolean isReplyExpected, long timeoutSeconds, String info, boolean allowlocalRoute) throws InterruptedException, RetryInvocationException {
			return new byte[0];
		}

		public void start() {
		}

		public void stop() {
		}

		public void dumpStats() {
		}
	}
}
