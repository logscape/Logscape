package com.liquidlabs.transport.tcp;

import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.EndPoint;
import com.liquidlabs.transport.MultiPeerSender;
import com.liquidlabs.transport.PeerListener;
import com.liquidlabs.transport.PeerSender;
import com.liquidlabs.transport.Receiver;
import com.liquidlabs.transport.protocol.Type;

import java.util.Collection;
import java.util.concurrent.ExecutorService;

public class TcpEndPoint implements EndPoint {
	
	private final URI address;
	private TcpSender tcpSender;
	private TcpReceiver tcpReceiver;
	private PeerSender multiPeerSender;
	private final Receiver receiver;

	public TcpEndPoint(URI address, Receiver receiver, ExecutorService executor) {
		this.address = address;
		this.receiver = receiver;
		this.tcpSender = new TcpSender(address);
		this.multiPeerSender = new MultiPeerSender(address, tcpSender);
		this.tcpReceiver = new TcpReceiver(address, receiver, executor);
	}

	public URI getAddress() {
		return address;
	}
	public Receiver getReceiver() {
		return receiver;
	}
	public void addPeerListener(PeerListener peerListener) {
		multiPeerSender.addPeerListener(peerListener);
	}

	public byte[] send(String protocol, URI endPoint, byte[] bytes, Type type, boolean isReplyExpected, long timeoutSeconds, String info, boolean allowlocalRoute) throws InterruptedException {
		return tcpSender.send("tcp", endPoint, bytes, type, isReplyExpected, timeoutSeconds, "methodName", true);
	}
	public boolean addPeer(URI peer) {
		return multiPeerSender.addPeer(peer);
	}
	public void removePeer(URI peer) {
		multiPeerSender.removePeer(peer);
	}
	public Collection<URI> getPeerNames() {
		return multiPeerSender.getPeerNames();
	}
	public void sendToPeers(byte[] data, boolean verbose) {
		multiPeerSender.sendToPeers(data, verbose);
	}
	public void start() {
		tcpSender.start();
		tcpReceiver.start();
	}

	public void stop() {
		tcpSender.stop();
		tcpReceiver.stop();
	}

	public byte[] receive(byte[] payload, String remoteAddress, String remoteHostname) {
		return null;
	}

	public void dumpStats() {
	}

	public boolean isForMe(Object payload) {
		throw new RuntimeException("Not implemented");
	}
	public byte[] receive(Object payload, String remoteAddress, String remoteHostname) {
		throw new RuntimeException("Not implemented");
	}

}
