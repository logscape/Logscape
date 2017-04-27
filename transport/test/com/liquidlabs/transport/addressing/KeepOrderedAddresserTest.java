package com.liquidlabs.transport.addressing;

import static org.junit.Assert.*;

import org.junit.Test;

import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.proxy.addressing.KeepOrderedAddresser;
import com.liquidlabs.transport.proxy.addressing.AddressHandler.RefreshAddrs;

public class KeepOrderedAddresserTest {
	
	
	@Test
	public void shouldFlipToAnotherEndPointInValidateWithRefreshTask() throws Exception {
		KeepOrderedAddresser handler = new KeepOrderedAddresser();
		String address1 = "stcp://192.168.70.8:111111/?svc=SHARED&host=logscape03&_startTime=03-Jul-11_11-11-11&udp=0";
		final String address2 = "stcp://192.168.71.17:222222/?svc=SHARED&host=logscape03&_startTime=03-Jul-22_22-22-22&udp=0";
		handler.addEndPoints(address1, address2);
		handler.registerAddressRefresher(new RefreshAddrs() {

			public String[] getAddresses() {
				System.out.println(">>>>>>>> HA >>>>>");
				System.out.println("<<<<<<< HA <<<<< Given EPS:" + address2);
				return new String[] { address2 } ;
			}
		});
		System.out.println("--- 1:" + handler.getEndPointURISafe());
		handler.registerFailure(handler.getEndPointURISafe());
		System.out.println("--- 2:" + handler.getEndPointURISafe());
		handler.registerFailure(handler.getEndPointURISafe());
		System.out.println("--- 3:" + handler.getEndPointURISafe());
		
		System.out.println("--- 4:" + handler.getEndPointURISafe());
		
	}

	@Test
	public void shouldReplayWhenNewVersionOfSameURIIsGiven() throws Exception {
		KeepOrderedAddresser handler = new KeepOrderedAddresser();
		String address1 = "stcp://10.54.172.60:11111?serviceName=AggSpace&host=LON-XPNAVERY3/_startTime=0000";
		handler.addEndPoints(address1);
		
		assertEquals(1, handler.getEndPointURIs().size());
		
		handler.resetReplayFlag();
		
		// add the same again - replay will be false
		handler.addEndPoints(address1);
		assertFalse(handler.isReplayRequired());
		
		String address2 = "stcp://10.54.172.60:11111?serviceName=AggSpace&host=LON-XPNAVERY3/_startTime=999999";
		handler.addEndPoints(address2);
		assertTrue(handler.isReplayRequired());
		
		assertEquals(1, handler.getEndPointURIs().size());
	}
	
	@Test
	public void shouldOnlyAddAddrOnce() throws Exception {
		KeepOrderedAddresser handler = new KeepOrderedAddresser();
		String address1 = "stcp://10.54.172.60:11111?serviceName=AggSpace&host=LON-XPNAVERY3/_startTime=10000&udp=0";
		handler.addEndPoints(address1);
		String address2 = "stcp://10.54.172.60:11111?serviceName=AggSpace&host=LON-XPNAVERY3/_startTime=20000&udp=0";
		handler.addEndPoints(address2);
		String address3 = "stcp://10.54.172.60:11111?serviceName=AggSpace&host=LON-XPNAVERY3/_startTime=300000&udp=0";
		handler.addEndPoints(address3);

		assertEquals(1, handler.getEndPointURIs().size());
		
	}
	
	@Test
	public void shouldHandleURIsAndManualRemoval() throws Exception {
		KeepOrderedAddresser handler = new KeepOrderedAddresser();
		String address1 = "stcp://10.54.172.60:11111?serviceName=AggSpace";
		String address2 = "stcp://10.54.172.60:11222?serviceName=AggSpace";
		String address3 = "stcp://10.54.172.60:11333?serviceName=AggSpace";
		handler.addEndPoints(address1, address2, address3);
		
		assertEquals(3, handler.getEndPointURIs().size());
		handler.remove(address2);
		handler.remove(address2);
		handler.remove(address2);
		assertEquals(2, handler.getEndPointURIs().size());
		handler.remove(address1);
		handler.remove(address1);
		handler.remove(address1);
		assertEquals(1, handler.getEndPointURIs().size());
		assertEquals(new URI(address3).getPort(), handler.getEndPointURI().getPort());
		
		assertEquals(0, handler.blackList().size());
	}
	
	@Test
	public void testShouldGiveGoodAddressAndStateAtStart() throws Exception {
		KeepOrderedAddresser handler = new KeepOrderedAddresser();
		handler.addEndPoints("tcp://one:0");

		assertNotNull(handler.getEndPointURISafe());
		assertFalse(handler.getEndPointURIs().isEmpty());
		assertTrue(handler.isReplayRequired());

		assertEquals("tcp://one:0", handler.getEndPointURI().toString());
		assertEquals(1, handler.getEndPointURIs().size());
	}

	@Test
	public void testShouldGiveGoodAddressAndStateAtStartWith2Address() throws Exception {
		KeepOrderedAddresser handler = new KeepOrderedAddresser();
		handler.addEndPoints("tcp://one:0", "tcp://two:0");

		assertNotNull(handler.getEndPointURISafe());
		assertFalse(handler.getEndPointURIs().isEmpty());
		assertTrue(handler.isReplayRequired());

		assertEquals("tcp://one:0", handler.getEndPointURI().toString());
		assertEquals(2, handler.getEndPointURIs().size());
	}

	@Test
	public void testShouldRotateOnFailureScenario() throws Exception {
		KeepOrderedAddresser handler = new KeepOrderedAddresser();
		handler.addEndPoints("tcp://one:0", "tcp://two:0");

		handler.registerFailure(handler.getEndPointURI());

		assertTrue(handler.isReplayRequired());
		assertNotNull(handler.getEndPointURISafe());
		assertNotNull(handler.getEndPointURISafe());
		assertEquals(1, handler.getEndPointURIs().size());

		assertEquals("tcp://two:0", handler.getEndPointURISafe().toString());
	}

	@Test
	public void testShouldWantReplayWhenFirstItemIsRemoved() throws Exception {

		KeepOrderedAddresser handler = new KeepOrderedAddresser();
		handler.addEndPoints("tcp://one:0", "tcp://two:0");

		handler.remove("tcp://one:0");

		assertTrue(handler.isReplayRequired());

		assertNotNull(handler.getEndPointURISafe());
		assertEquals("tcp://two:0", handler.getEndPointURI().toString());
		assertEquals(1, handler.getEndPointURIs().size());
	}

	@Test
	public void testShouldNOTWantReplayWhenSecondItemIsRemoved() throws Exception {

		KeepOrderedAddresser handler = new KeepOrderedAddresser();
		handler.addEndPoints("tcp://one:0", "tcp://two:0");

		handler.remove("tcp://two:0");

		assertTrue(handler.isReplayRequired());

		assertEquals("tcp://one:0", handler.getEndPointURI().toString());
		assertEquals(1, handler.getEndPointURIs().size());
	}

	@Test
	public void testShouldOnlyIgnoreDuplicated() throws Exception {
		KeepOrderedAddresser handler = new KeepOrderedAddresser();
		handler.addEndPoints("tcp://9", "tcp://1", "tcp://9", "tcp://1", "tcp://9", "tcp://1");
		assertEquals(2, handler.getEndPointURIs().size());
	}

	@Test
	public void testShouldRotateSortedlyOnFailureWithAddressesTask() throws Exception {
		KeepOrderedAddresser handler = new KeepOrderedAddresser();
		handler.addEndPoints("tcp://1:0","tcp://2:0", "tcp://3:0", "tcp://4:0", "tcp://5:0", "tcp://6:0", "tcp://7:0", "tcp://9:0");
		assertEquals("tcp://1:0", handler.getEndPointURISafe().toString());
		handler.registerFailure(handler.getEndPointURISafe());
		assertEquals("tcp://2:0", handler.getEndPointURISafe().toString());
		handler.registerFailure(handler.getEndPointURISafe());
		assertEquals("tcp://3:0", handler.getEndPointURISafe().toString());
		handler.registerFailure(handler.getEndPointURISafe());
		assertEquals("tcp://4:0", handler.getEndPointURISafe().toString());
		assertEquals(5, handler.getEndPointURIs().size());
	}

	@Test
	public void testShouldRotateSortedlyOnFailure() throws Exception {
		KeepOrderedAddresser handler = new KeepOrderedAddresser();
		handler.addEndPoints("tcp://1:0", "tcp://2:0", "tcp://3:0","tcp://6:0","tcp://7:0", "tcp://9:0");
		assertEquals("tcp://1:0", handler.getEndPointURISafe().toString());
		handler.registerFailure(handler.getEndPointURISafe());
		assertEquals("tcp://2:0", handler.getEndPointURISafe().toString());
		handler.registerFailure(handler.getEndPointURISafe());
		assertEquals("tcp://3:0", handler.getEndPointURISafe().toString());
		handler.registerFailure(handler.getEndPointURISafe());
		assertEquals("tcp://6:0", handler.getEndPointURISafe().toString());
		assertEquals(3, handler.getEndPointURIs().size());
	}
	
	
	@Test
	public void testShouldFailBack() throws Exception {
//		Removed address[tcp://alteredcarbon.local:11500/LookupSpace] ==> ePoint:tcp://alteredcarbon.local:11500 eps[tcp://alteredcarbon.local:11000, tcp://alteredcarbon.local:11500]
		KeepOrderedAddresser handler = new KeepOrderedAddresser();
		handler.addEndPoints("tcp://alteredcarbon.local:11000", "tcp://alteredcarbon.local:11500");
		
		// 11000
		assertTrue(handler.getEndPointURI().toString().contains("11000"));
		
		// fail to 11500
		handler.registerFailure(handler.getEndPointURI());
		handler.resetReplayFlag();
		assertTrue(handler.getEndPointURI().toString().contains("11500"));
	}

	@Test
	public void testShouldHandleEmptyAddressStartUp() throws Exception {
		KeepOrderedAddresser handler = new KeepOrderedAddresser();
		handler.addEndPoints();
		
		assertFalse("Should have returned false - no endpoint set", handler.isEndPointAvailable());
		try {
			handler.getEndPointURI();
			fail("should have blown up");
		} catch (Throwable t) {
		}
	}
}
