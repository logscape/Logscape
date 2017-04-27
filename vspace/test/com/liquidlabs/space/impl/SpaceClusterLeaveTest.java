package com.liquidlabs.space.impl;

import com.liquidlabs.space.Space;
import com.liquidlabs.space.lease.Lease;
import com.liquidlabs.transport.proxy.events.Event;
import com.liquidlabs.transport.proxy.events.EventListener;
import com.liquidlabs.transport.proxy.events.Event.Type;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;


/**
 * validate operations go through the basic cluster
 *
 */
public class SpaceClusterLeaveTest extends SpaceBaseFunctionalTest {

	long timeout = 300;
	@Before
	public void setUp() throws Exception {
		System.setProperty(Lease.PROPERTY, "1");
		super.setUp();
	}
	int count = 0;

	@After
	public void tearDown() throws Exception {
		super.tearDown();
	}
	@Test	
	public void testUpdatesMakeItToPeer() throws Exception {
		String key = "makesItToPeer" + "A";
		spaceA.write(key, "A|B|C".replaceAll("\\|", Space.DELIM), expires);
		pause();
		String updatedValue = spaceB.read(key);
		assertNotNull(updatedValue);
		
		
		// now remove the peer from both spaces
		spaceA.removePeer(spaceB.getReplicationURI());
		spaceB.removePeer(spaceA.getReplicationURI());
		
		pause();
		
		String key2 = "makesItToPeer" + "2222";
		
		spaceA.write(key2, "A|B|C".replaceAll("\\|", Space.DELIM), expires);
		
		String updatedValue2 = spaceB.read(key2);
		assertNull(updatedValue2);
		
		
	}

}
