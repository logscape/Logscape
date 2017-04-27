package com.liquidlabs.vso;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class TestHostLookup {
	
	public static void main(String[] args) {
		try {
			
			InetAddress localHost = InetAddress.getLocalHost();
			System.out.println("addr:" + localHost);
			String hostName = localHost.getHostName();
			System.out.println("hostname is:" + hostName);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}

}
