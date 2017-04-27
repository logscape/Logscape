package com.liquidlabs.transport.netty;


import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.Sender;
import com.liquidlabs.transport.SenderFactory;
import com.liquidlabs.transport.TransportProperties;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.DefaultChannelFuture;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class NettySimpleSenderFactory implements SenderFactory {
	
	private static final Logger LOGGER = Logger.getLogger(NettySimpleSenderFactory.class);
	
	private ChannelFactory factory;
	AtomicInteger given = new AtomicInteger();
	
	LifeCycle.State state = LifeCycle.State.STARTED;
	
	// return Queue
	LinkedBlockingQueue<NettySenderImpl> returnSenders = new LinkedBlockingQueue<NettySenderImpl>();

	private Thread returnThread;

	private boolean restrictClientPorts  = TransportProperties.isClientPortsRestricted();
	

	public NettySimpleSenderFactory(ClientSocketChannelFactory factory) {
		DefaultChannelFuture.setUseDeadLockChecker(false);
		this.factory = factory;
		
		LOGGER.info("Using ConnectionOutstandingLimit:" + TransportProperties.getConnectionOutstandingLimit());

		returnThread = new Thread("NettySimplePoolReturn") {
			public void run() {
				while (true) {
					try {
						NettySenderImpl take  = returnSenders.take();
						take.stop();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				
			}
		};
		returnThread.setDaemon(true);
		returnThread.start();
	}
	public int getGivenConnections() {
		return given.get();
	}

	public Sender getSender(URI uri, boolean logIt, boolean remoteOnly, String context) throws IOException, InterruptedException {
			if (this.state == State.STOPPED) throw new RuntimeException("Factory is shutdown");

			NettySenderImpl newSender = new NettySenderImpl(uri, factory, restrictClientPorts  = TransportProperties.isClientPortsRestricted(), false);
			newSender.start();
			if (LOGGER.isDebugEnabled()) LOGGER.debug("Using NewSender:" + newSender);
			given.incrementAndGet();
			return newSender;
	}
	
	public void returnSender(URI uri, Sender sender, boolean discardSender) {
		if (sender == null) return;
		given.decrementAndGet();
		NettySenderImpl s = (NettySenderImpl) sender;
		if (discardSender) {
			s.flush();
			s.stop();
		} else {
			returnSenders.add(s);
		}
//		s.stop();
		
	}
	
	public int currentLiveConnectionCount() {
		return 0;
	}

	public void start() {
		this.state = State.STARTED;
	}

	public void stop() {
		if (this.state == State.STOPPED) return;
		this.state = State.STOPPED;
		
	}
	public String dumpStats() {
		return toString();
	}
	public String toString() {
		return getClass().getSimpleName() + " returnSenders:" + this.returnSenders.size(); 
	}
	private String getHostName() {
		try {
			InetAddress localHost = InetAddress.getLocalHost();
			return localHost.getCanonicalHostName();
		} catch (Throwable t) {
			return "localhost";
		}
	}

}
