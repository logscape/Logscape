package com.liquidlabs.transport;

import java.util.Set;

import com.liquidlabs.common.net.URI;

public interface PeerListener {
	void peerAdded(URI peer, Set<URI> peers);

	void peerRemoved(URI peer, Set<URI> keySet);

}
