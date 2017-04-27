package com.liquidlabs.space.impl;

import java.util.Arrays;

import com.liquidlabs.common.net.URI;


public class SpaceMain {
	public static void main(String[] args) {
		System.out.println("SpaceMain:" + Arrays.toString(args));
		int port = 11000;
		if (args.length == 0 || args[0].equals("--help")) {
			System.out.println("general usage: app-exe tcpport");
			System.out.println("automatically selecting a port, searching from:" + port);
		} else {
			port = Integer.parseInt(args[0]);
		}
		try {
			SpacePeer spacePeer = new SpacePeer(new URI("udp://localhost:" + port));
			spacePeer.createSpace(SpacePeer.DEFAULT_SPACE, true, false);
			spacePeer.start();
			while (true) {
				Thread.sleep(5000);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	
	}
}
