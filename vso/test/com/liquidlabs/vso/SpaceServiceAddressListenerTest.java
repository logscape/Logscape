package com.liquidlabs.vso;


import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.liquidlabs.common.net.URI;

public class SpaceServiceAddressListenerTest {

	private SpaceServiceAddressListener listener;

	@Before
	public void setUp() throws Exception {
		listener = new SpaceServiceAddressListener();
	}

	@Test
	public void shouldNotIgnoreMe() throws Exception {
		listener.setReplAddress(new URI("stcp://192.168.70.164:11044?svc=LogSpace"));
		
		Assert.assertFalse(listener.isSameSvc(new URI("stcp://192.168.70.164:13044?svc=LogSpace2&_startTime=14-Feb-12_11-54-39")));
		Assert.assertTrue(listener.isSameSvc(new URI("stcp://192.168.70.164:13044?svc=LogSpace&_startTime=14-Feb-12_11-54-39")));
	}

}
