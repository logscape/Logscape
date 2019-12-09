package com.liquidlabs.common;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

import org.junit.Test;

public class NetworkUtilsTest {


	String file = "===========================================================================\n" +
			"Interface List\n" +
			" 10...b4 99 ba b5 db e2 ......HP NC382i DP Multifunction Gigabit Server Adapter #11\n" +
			"  1...........................Software Loopback Interface 1\n" +
			"===========================================================================\n" +
			"\n" +
			"IPv4 Route Table\n" +
			"===========================================================================\n" +
			"Active Routes:\n" +
			"Network Destination        Netmask          Gateway       Interface  Metric\n" +
			"          0.0.0.0          0.0.0.0   10.243.115.254   10.243.112.156    266\n" +
			"===========================================================================\n" +
			"Persistent Routes:\n" +
			"  Network Address          Netmask  Gateway Address  Metric\n" +
			"          0.0.0.0          0.0.0.0   10.243.115.254  Default \n" +
			"===========================================================================\n" +
			"\n" +
			"IPv6 Route Table\n" +
			"===========================================================================\n" +
			"Active Routes:\n" +
			"  None\n" +
			"Persistent Routes:\n" +
			"  None\n";

	@Test
	public void shouldCachehostnameLookup() throws Exception {
		InetSocketAddress addr = new InetSocketAddress(8811);
		NetworkUtils.remoteAddrToHostname.clear();
		String host = NetworkUtils.resolveHostname(addr);
		assertNotNull(host);
		String host2 = NetworkUtils.resolveHostname(addr);
		assertEquals(1, NetworkUtils.remoteAddrToHostname.size());
		assertNotNull(host2);
		
	}
	@Test
	public void shouldSeeIFPortInUse() throws Exception {
		
		int determinePort1 = NetworkUtils.determinePort(11000);
		ServerSocket socket = new ServerSocket(determinePort1);
		int determinePort2 = NetworkUtils.determinePort(determinePort1);
		
		System.out.println("1 GotPort:" + determinePort1);
		System.out.println("2 GotPort:" + determinePort2);
		socket.close();
		assertNotSame(Integer.valueOf(determinePort1), Integer.valueOf(determinePort2));
	}
	
	
	@Test
	public void shouldGetHostName() throws Exception {
		String hostname = NetworkUtils.getHostname();
		assertNotNull(hostname);
	}
	
	@Test
	public void shouldGetIP() throws Exception {
//		System.setProperty("net.iface", "net5");
		String addr = NetworkUtils.getIPAddress();
		assertNotNull(addr);
	}

}
