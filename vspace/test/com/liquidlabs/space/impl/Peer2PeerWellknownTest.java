package com.liquidlabs.space.impl;

import org.junit.Assert;
import org.junit.Test;

import com.liquidlabs.common.net.URI;
import com.liquidlabs.space.Space;
import com.liquidlabs.transport.TransportProperties;

public class Peer2PeerWellknownTest {

	private boolean isClustered = true;
	private boolean reuseClusterPort = true;

	@Test
	public void testPeersShouldDiscoverEachOtherAndPassData() throws Exception {
		TransportProperties.setMCastEnabled(false);
		SpacePeer spacePeerA = new SpacePeer(new URI("tcp://localhost:15010") );
		Space spaceA = spacePeerA.createSpace(SpacePeer.DEFAULT_SPACE, isClustered, reuseClusterPort);
		spacePeerA.start();
		SpacePeer spacePeerB = new SpacePeer(new URI("tcp://localhost:15020"));
		Space spaceB = spacePeerB.createSpace(SpacePeer.DEFAULT_SPACE, isClustered, reuseClusterPort);
		spacePeerB.start();
		
		spaceA.addPeer(spacePeerB.getReplicationURI());
		spaceB.addPeer(spacePeerA.getReplicationURI());
		
		Thread.sleep(1000);
		spaceA.write("aKey", "aValue", -1);
		Thread.sleep(500);
		String read = spaceB.read("aKey");
		Assert.assertNotNull("Should have found a value!", read);
		spacePeerA.stop();
		spacePeerB.stop();
	}
	
}
