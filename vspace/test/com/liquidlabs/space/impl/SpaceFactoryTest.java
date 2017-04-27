package com.liquidlabs.space.impl;

import static org.junit.Assert.*;

import org.junit.Test;

import com.liquidlabs.common.net.URI;

public class SpaceFactoryTest {
	
	
	@Test
	public void shouldMatchPeer() throws Exception {
		SpaceFactory spaceFactory = new SpaceFactory();
		//stcp://192.168.70.68:11020?svc=LogSpace
		URI uri = new URI("stcp://192.168.70.68:11020?svc=LogSpace");
		assertTrue(spaceFactory.isPeerURLMatch("LogSpace-SPACE", uri));
		assertTrue(spaceFactory.isPeerURLMatch("LogSpace", uri));
		
		URI uri2 = new URI("stcp://192.168.70.68:11020?svc=LogSpace-SPACE");
		assertTrue(spaceFactory.isPeerURLMatch("LogSpace-SPACE", uri2));
		assertTrue(spaceFactory.isPeerURLMatch("LogSpace", uri2));
		
		URI uri3 = new URI("stcp://192.168.70.68:11020?svc=LogSpaceNOT");
		assertFalse(spaceFactory.isPeerURLMatch("MonitorSpace", uri3));
	}
}
