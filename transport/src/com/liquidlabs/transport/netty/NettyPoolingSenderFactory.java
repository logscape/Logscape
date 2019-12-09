package com.liquidlabs.transport.netty;


import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.collection.Multipool;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.Sender;
import com.liquidlabs.transport.SenderFactory;
import com.liquidlabs.transport.TransportProperties;
import javolution.util.FastMap;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.DefaultChannelFuture;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class NettyPoolingSenderFactory implements SenderFactory {
	
	private static final Logger LOGGER = Logger.getLogger(NettyPoolingSenderFactory.class);
	
	private ChannelFactory factory;
	AtomicInteger given = new AtomicInteger();
	AtomicInteger getters = new AtomicInteger();
	public boolean IS_STOPPED = false;
	
	Multipool<URI, NettySenderImpl> senders;
	FastMap<String, NettySenderImpl> allSenders = new FastMap<String, NettySenderImpl>();
	LifeCycle.State state = LifeCycle.State.STARTED;
	
	private boolean restrictClientPorts = new ClientPortRestrictedDetector().isBootPropertiesSetValueToTrue();
    private boolean handshake;

    public NettyPoolingSenderFactory(ClientSocketChannelFactory factory, boolean handshake, ScheduledExecutorService scheduler) {
        this.handshake = handshake;
        allSenders.shared();
		this.factory = factory;
		DefaultChannelFuture.setUseDeadLockChecker(false);
		if (restrictClientPorts) {
			LOGGER.info("Restricting CLIENT Port allocation");
		} else {
			LOGGER.info("NON-Restricted CLIENT Port allocation / Using OS-Ephemeral Client-PORTS");			
		}

		senders = new Multipool<>(scheduler);
		
		senders.registerListener(new Multipool.CleanupListener<NettySenderImpl>() {
			public void stopping(NettySenderImpl object) {
				allSenders.remove(object.id);
			}
		});
		
		LOGGER.info("Using ConnectionOutstandingLimit:" + TransportProperties.getConnectionOutstandingLimit());

        //new NettyPoolingSenderFactoryJMX(this);
	}

	public int getGivenConnections() {
		return given.get();
	}

	public Sender getSender(URI uri, boolean logIt, boolean remoteOnly, String context) throws IOException, InterruptedException {
		try {
			if (this.state == State.STOPPED) throw new InterruptedException("Factory is shutdown");

			getters.incrementAndGet();
			
			NettySenderImpl sender = null;
            try {
                sender =  senders.get(uri);
            } catch (Throwable t) {
                LOGGER.warn("Senders.getFailed:" + uri);
            }
			if (sender != null) {
				sender.validate();
				if (sender.getException() == null) {
					if (LOGGER.isDebugEnabled()) LOGGER.debug("ConnectionPoolHit >>ID:" + sender.hashCode() + "  " + uri + " c:" + context);
					given.incrementAndGet();
					allSenders.put(sender.id,sender);
					return sender;
				}
				sender.stop();
			}

            // failed to get one...
            slowDownIfTooManyOutstandingConnections();

            // try and wait for one from the pool
            sender = null;
            while ( ( sender = senders.get(uri)) == null && given.get() > TransportProperties.getConnectionOutstandingLimit()) {
                Thread.sleep(100);
            };
            if (sender != null) {
                sender.validate();
                if (sender.getException() == null) {
                    given.incrementAndGet();
                    allSenders.put(sender.id,sender);
                    return sender;
                }
            }

            NettySenderImpl newSender = new NettySenderImpl(uri, factory, restrictClientPorts);
            newSender.setContext(context);
            newSender.start();
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Using NewSender:" + newSender);
            given.incrementAndGet();
            allSenders.put(newSender.id, newSender);
            return newSender;
		} finally {
			getters.decrementAndGet();
		}
	}
	private void slowDownIfTooManyOutstandingConnections() throws InterruptedException {
			while (given.get() > TransportProperties.getConnectionOutstandingLimit()) {
				Thread.sleep(25);
			}
	}
	
	public void returnSender(URI uri, Sender sender, boolean discardSender) {
		if (sender == null) return;
		given.decrementAndGet();
		if (discardSender) {
			// can get very noisy
			NettySenderImpl s = (NettySenderImpl) sender;
			LOGGER.warn("Discarding:" + sender);
			senders.discard((NettySenderImpl) sender);
			allSenders.remove(((NettySenderImpl) sender).id);
			s.stop();
		} else {
			senders.put(uri, (NettySenderImpl) sender);
		}
	}
	
	public int currentLiveConnectionCount() {
		return given.get();
	}

	public void start() {
		this.state = State.STARTED;
	}

	public void stop() {
		if (this.state == State.STOPPED) return;
		this.state = State.STOPPED;
		IS_STOPPED = true;
		
		for (NettySenderImpl sender : this.allSenders.values()) {
			sender.stop();
		}
		Collection<NettySenderImpl> values = senders.values();
		for (NettySenderImpl nettySenderImpl : values) {
			nettySenderImpl.stop();
		}
		senders.stop();
		senders = null;
	}
	public String dumpStats() {
		return this.toString();
	}
	public String toString() {
		return "NettySendFactory senderPool:" + senders.size() + " live:" + this.getGivenConnections() + " allSenders:" + allSenders;
	}


    public Multipool<URI, NettySenderImpl> getMpool() {
        return this.senders;
    }
}
