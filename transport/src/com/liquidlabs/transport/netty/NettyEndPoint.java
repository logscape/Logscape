package com.liquidlabs.transport.netty;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.*;
import com.liquidlabs.transport.protocol.Type;
import com.liquidlabs.transport.proxy.RetryInvocationException;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.socket.ServerSocketChannelFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class NettyEndPoint implements EndPoint, LifeCycle {
	private static final Logger logger = Logger.getLogger(NettyEndPoint.class);


	private final URI address;
	private Receiver receiver;
	private Receiver givenReceiver;
	private final NettySenderFactoryProxy senderFactoryProxy;
	private MultiPeerSender multiPeerSender;
	volatile int sendMsg;
	volatile long sendBytes;

	private LLProtocolParser protocolParser;

	private final String service;

	public NettyEndPoint(String service, URI address, Receiver receiver, ServerSocketChannelFactory socketFactory, SenderFactory senderFactory, ScheduledExecutorService scheduler) {
		this.service = service;
		this.address = address;
		this.givenReceiver = receiver;
		this.senderFactoryProxy = new NettySenderFactoryProxy(address, senderFactory);
		this.multiPeerSender = new MultiPeerSender(address, senderFactoryProxy);
		System.out.println(service + " Opening PORT:" + address.getPort());
		try {
			if (address.toString().startsWith("stcp") || address.toString().startsWith("tcp")) {
				protocolParser = new LLProtocolParser(receiver);

                boolean isSecured = address.getPort() == TransportProperties.getSecureEndpointPort();
                if (isSecured) {
                    logger.info("SecureClientHandshake Against:" + address);
                }

                this.receiver = new NettyReceiver(address, socketFactory, protocolParser);
			} else if (address.toString().startsWith("raw")) {
				this.receiver = new NettyReceiver(address, socketFactory, new StringProtocolParser(receiver));
			} else {
				RuntimeException ex = new RuntimeException("Unknown protocol:" + address);
				logger.warn(ex.toString(), ex);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		if (TransportProperties.isConnectionDebug()) {
			scheduler.scheduleAtFixedRate(new Runnable() {
	
				public void run() {
					logger.info(NettyEndPoint.this.toString());
				}
			}, 1, 5, TimeUnit.MINUTES);
		}
		
		scheduler.scheduleAtFixedRate(new Runnable() {

			public void run() {
				if (protocolParser != null) {
					logger.info(" Stats:" + NettyEndPoint.this.service + "/" + NettyEndPoint.this.address + " SendMsg:" + sendMsg + " SendKb:" + toKb(sendBytes) + " RecvMsg:" + protocolParser.recvdMsgs + " RecvKb:" + toKb(protocolParser.recvdBytes) + " ]");
					NettyEndPoint.this.sendMsg = 0;
					NettyEndPoint.this.sendBytes = 0;
					NettyEndPoint.this.protocolParser.recvdMsgs = 0;
					NettyEndPoint.this.protocolParser.recvdBytes = 0;
				}
			}
			
		}, 1, 5, TimeUnit.MINUTES);
	}
	protected long toKb(long sendBytes2) {
		return sendBytes2/(1024);
	}
	public Receiver getReceiver() {
		return givenReceiver;
	}

	public URI getAddress() {
		return address;
	}

	public byte[] send(String protocol, URI endPoint, byte[] bytes, Type type, boolean isReplyExpected, long timeoutSeconds, String info, boolean allowLocalRoute) throws InterruptedException, RetryInvocationException {
		sendMsg++;
		sendBytes += bytes.length;
		return senderFactoryProxy.send(protocol, endPoint, bytes, type, isReplyExpected, timeoutSeconds, info, allowLocalRoute);
	}

	public byte[] receive(byte[] payload, String remoteAddress, String remoteHostname) throws InterruptedException {
		return null;
	}
	public boolean addPeer(URI peer) {
		return multiPeerSender.addPeer(peer);
	}
	public void removePeer(URI peer) {
		multiPeerSender.removePeer(peer);
	}
	public void addPeerListener(PeerListener peerListener) {
		multiPeerSender.addPeerListener(peerListener);
	}
	public void sendToPeers(byte[] data, boolean verbose) {
		sendMsg += multiPeerSender.peerCount();
		sendBytes += (multiPeerSender.peerCount() * data.length);
		multiPeerSender.sendToPeers(data, verbose);
	}
	public Collection<URI> getPeerNames() {
		return multiPeerSender.getPeerNames();
	}


	public void start() {
		senderFactoryProxy.start();
		receiver.start();
	}

	public void stop() {
		//logger.info(" ******************** Stopping:" + address);
		senderFactoryProxy.stop();
		receiver.stop();
	}

	public void dumpStats() {
	}
	public String toString() {
		
		return super.toString() +  " uri:" + address + " " + receiver.toString() + " sender:" + senderFactoryProxy.toString();
	}
	public static void main(String[] args) {
		NettyEndPointFactory nettyEndPointFactory = new NettyEndPointFactory(Executors.newScheduledThreadPool(10), "unknown-main");
		Receiver r = new Receiver() {

			public byte[] receive(byte[] payload, String remoteAddress, String remoteHostname) throws InterruptedException {
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
			
		};
		
		
		try {
			System.out.println("Starting ep on port:" + args[0]);
			EndPoint endPoint = nettyEndPointFactory.getEndPoint(new URI("stcp://localhost:" + args[0]), r);
			Thread.sleep(Integer.parseInt(args[1]) * 1000);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	public boolean isForMe(Object payload) {
		throw new RuntimeException("Not implemented");
	}
	public byte[] receive(Object payload, String remoteAddress, String remoteHostname) {
		throw new RuntimeException("Not implemented");
	}


}
