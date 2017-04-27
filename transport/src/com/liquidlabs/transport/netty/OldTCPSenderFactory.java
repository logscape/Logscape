package com.liquidlabs.transport.netty;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.liquidlabs.common.collection.Multipool;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.Sender;
import com.liquidlabs.transport.SenderFactory;
import com.liquidlabs.transport.tcp.TcpRestrictedSender;

public class OldTCPSenderFactory implements SenderFactory {
	Multipool<URI, Sender> senders;
	private final ExecutorService executorService;
	
	public OldTCPSenderFactory(ExecutorService executorService) {
		this.executorService = executorService;
		senders = new Multipool<URI, Sender>(com.liquidlabs.common.concurrent.ExecutorService.newScheduledThreadPool("services"));
	}


	public Sender getSender(URI uri, boolean logIt, boolean remoteOnly, String context) throws IOException, InterruptedException {
		Sender tcpSender = senders.get(uri);
		if (tcpSender == null) {
			TcpRestrictedSender sender = new TcpRestrictedSender(null, executorService);
			sender.start();
			return sender;
		}
		return tcpSender;
	}

	public void returnSender(URI uri, Sender sender, boolean discardSender) {
		if (discardSender) sender.stop();
		else {
			senders.put(uri, sender);
		}
	}

	public void start() {
	}

	public void stop() {
		for (Sender sender : senders.values()) {
			try {
				sender.stop();
			} catch (Throwable t){
			}
		}
	}
	public int currentLiveConnectionCount() {
		return 0;
	}
	public String dumpStats() {
		return toString();
	}
	public String toString() {
		return this.getClass().getSimpleName() + " live:" + this.currentLiveConnectionCount() + " pool:" + this.senders.size();
	}
}
