package com.liquidlabs.transport;

import com.liquidlabs.common.net.URI;

import java.util.Collection;

public interface PeerSender {

	boolean addPeer(URI peer);
	
	void addPeerListener(PeerListener peerListener);

	void removePeer(URI peer);

	void sendToPeers(byte[] data, boolean verbose);

	Collection<URI> getPeerNames();

}
