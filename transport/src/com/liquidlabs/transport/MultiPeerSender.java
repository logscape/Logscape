package com.liquidlabs.transport;

import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.log4j.Logger;

import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.protocol.Type;



public class MultiPeerSender implements PeerSender{
	Map<URISet, URISet> peers = new ConcurrentHashMap<URISet, URISet>();
	static final Logger LOGGER = Logger.getLogger(MultiPeerSender.class);
	
	private final Sender sender;

	private final URI address;
	private List<PeerListener> peerListeners = new CopyOnWriteArrayList<PeerListener>();
	public MultiPeerSender(URI address, Sender sender) {
		this.address = address;
		this.sender = sender;
	}
	
	public void addPeerListener(PeerListener peerListener) {
		this.peerListeners.add(peerListener);
	}

    public boolean addPeer(final URI peer) {
    	if (isThisSameEndpoint(peer)) {
    		return false;
    	}
		if (peer.getPort() == -1) {
            LOGGER.warn("Invalid Peer Address:" + peer, new RuntimeException("InvalidPeer"));
            throw new RuntimeException("InvalidPeer:" + peer);
        }
//    	URI cleanPeer = getURLValueOnly(peer);
    	if (!this.peers.containsKey(new URISet(peer))) {
    		LOGGER.info(getAddress() + " - Adding PEER:" + peer + " ALL:" + this.peers.keySet() + " Listeners:" + this.peerListeners);
    		this.peers.put(new URISet(peer), new URISet(peer));
    		// allow callbacks to occur they need to filter on the URI params part - even though the endpoint might have the same address
    		for (PeerListener peerListener : this.peerListeners) {
    			peerListener.peerAdded(peer, getPeerURIs());
    		}
    		return true;

    	} else {
    		// seen it before
    		LOGGER.info(getAddress() + " - NOT-Adding PEER:" + peer + " ALL:" + this.peers.keySet());
    		return false;
    	}
    
	}
	private URI getURLValueOnly(URI uri) {
		try {
			return new URI(uri.getScheme() + "://" + uri.getHost() + ":" + uri.getPort());
		} catch (URISyntaxException e) {
			e.printStackTrace();
		} 
		throw new RuntimeException("Failed to get URI");
	}

	private boolean isThisSameEndpoint(URI peer) {
		return getAddress().getHost().equals(peer.getHost()) && getAddress().getPort() == peer.getPort();
	}

	public void removePeer(URI peer) {
		LOGGER.info("hash:" + this.hashCode() + "/" + getAddress() + " - Removing PEER:" + peer + " list:" + this.peers.keySet());
		URI cleanPeer = getURLValueOnly(peer);
		URISet remove = this.peers.remove(new URISet(cleanPeer));
		if (remove != null) {
			for (PeerListener peerListener : this.peerListeners) {
				peerListener.peerRemoved(peer, getPeerURIs());
			}
		} else {
			LOGGER.info("Already Removed:" + peer);
		}
	}
	public Collection<URI> getPeerNames() {
		HashSet<URI> results = getPeerURIs();
		return results;
	}
	public int peerCount() {
		return peers.size();
	}

	private HashSet<URI> getPeerURIs() {
		HashSet<URI> results = new HashSet<URI>();
		for (URISet uri : this.peers.keySet()) {
			results.add(uri.uri);
		}
		return results;
	}
	public void sendToPeers(byte[] data, boolean verbose) {
		
		for (URISet peer : peers.keySet()) {
			try {
				if (verbose) LOGGER.info(this.address + " Sending TO:" + peer + " f:" + peer.failed);
//				if (peer.failed > 50) {
//					LOGGER.info("Removing PEER:" + peer);
//					peers.remove(peer);
//				}	else 
					sender.send("tcp", peer.uri, data, Type.REQUEST, false, 1, "MultiPeerSender.sendToPeer[" + peer + "]", true);
			} catch (Throwable e) {
				if (verbose) LOGGER.info(this.address + " FAILED Sending TO:" + peer + " ex:" + e.getMessage());
				peer.failed++;
			}
		}
	}
	public URI getAddress() {
		return address;
	}
	public static class URISet {
		public URI uri;
		public int failed;
		public URISet(URI peer) {
			this.uri = peer;
		}
		public boolean equals(Object obj) {
			return uri.equals(	((URISet) obj).uri);
		}
		public int hashCode() {
			return uri.hashCode();
		}
		public String toString() {
			return uri.toString();
		}
	}

}
