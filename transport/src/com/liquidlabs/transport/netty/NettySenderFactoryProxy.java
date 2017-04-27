package com.liquidlabs.transport.netty;

import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketException;

import com.liquidlabs.common.net.URI;

import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.UnresolvedAddressException;

import org.apache.log4j.Logger;

import com.liquidlabs.transport.AddressResolver;
import com.liquidlabs.transport.Sender;
import com.liquidlabs.transport.SenderFactory;
import com.liquidlabs.transport.protocol.Type;
import com.liquidlabs.transport.proxy.RetryInvocationException;

public class NettySenderFactoryProxy implements Sender {
	private static final String UDP_0 = "udp=0";
	private static final String UDP = "udp";
	static final Logger LOGGER = Logger.getLogger(NettySenderFactoryProxy.class);	
	private final URI address;
	private final SenderFactory senderFactory;
	AddressResolver addressResolver = new AddressResolver("hosts.txt");
	boolean poolingDebug = Boolean.getBoolean("netty.pool.debug");

	public NettySenderFactoryProxy(URI address, SenderFactory senderFactory) {
		this.address = address;
		this.senderFactory = senderFactory;
	}

	public URI getAddress() {
		return address;
	}

	public byte[] send(String protocol, URI uri, byte[] bytes, Type type, boolean isReplyExpected, long timeoutSeconds, String info, boolean allowLocalRoute) throws InterruptedException, RetryInvocationException {
		
		Sender sender = null;
		boolean discardSender = false;
		try {
			try {
				uri = addressResolver.resolve(uri);
				sender = getSender(uri, allowLocalRoute, info);
				return sender.send(protocol, uri, bytes, type, isReplyExpected, timeoutSeconds, info, allowLocalRoute);
				
			} catch (InterruptedException ie) {
				discardSender = true;
				throw ie;
			} catch (ClosedByInterruptException io) {
				discardSender = true;
				throw new InterruptedException(io.toString());
				// unwrap the runtimeEx
			} catch (Throwable t) {
				// catch the java.
				discardSender = true;
				if (t.getCause() != null) throw t.getCause();
				throw t;
			} 
		
		} catch (NoRouteToHostException nr) {
			LOGGER.error(nr.toString() + " " + uri);
			throw new RetryInvocationException("SendFailed.NoRouteToHostException:" + uri.toString() + " ex:" + nr.toString(), nr);
		} catch (UnresolvedAddressException e) {
			LOGGER.error("Cannot resolve:" + uri, e);
			throw new RetryInvocationException("SendFailed.UnresolvedAddressException:" + uri.toString() + " ex:" + e.toString(), e);
		} catch (ConnectException e) {
			String senderString = sender != null ? sender.toString() : "noSender";
			throw new RetryInvocationException("SendFailed.ConnectException:" + senderString + " URI:" + uri, e);
		} catch (SocketException e) {
			LOGGER.error("SocketException:" + uri, e);
			throw new RuntimeException("SendFailed.SocketException:" + uri.toString() + " ex:" + e.toString(), e);
		} catch (IOException e) {
			String senderString = sender != null ? sender.toString() : "noSender";
			LOGGER.warn(String.format("(Retry) IOException %s URI:%s LiveConnections:%d", e.getMessage(), uri.toString(), senderFactory.currentLiveConnectionCount()), e);
            Thread.sleep(500);
			throw new RetryInvocationException("SendFailed.IOEx:" + senderString, e);
		} catch (RetryInvocationException e) {
			throw e;
		} catch (InterruptedException e) {
			throw e;
		} catch (Throwable e) {
			String senderString = sender != null ? sender.toString() : "noSender";
			LOGGER.warn(String.format("Sender[%s] Ex[%s] Stats[%s] URI[%s] Info[%s] LiveConnections:%d Ex:%s", senderString, e.getMessage(), senderFactory.dumpStats(), uri.toString(), info, senderFactory.currentLiveConnectionCount(), e.toString()));
            Thread.sleep(500);
			throw new RetryInvocationException("SendFailed.Throwable:" + senderString, e);
		} finally {
			senderFactory.returnSender(uri, sender, discardSender);
		}
	}

	private Sender getSender(URI uri, boolean allowLocaleRout, String context) throws IOException, InterruptedException {
		return senderFactory.getSender(uri, poolingDebug || !allowLocaleRout, !allowLocaleRout, context);
	}

	public void start() {
	}

	public void stop() {
		senderFactory.stop();
	}

	public void dumpStats() {
		senderFactory.dumpStats();
	}

	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append("[NettySender:");
		buffer.append(" senderFactory: ");
		buffer.append(senderFactory);
		buffer.append("]");
		return buffer.toString();
	}
	
}
