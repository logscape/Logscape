/**
 * 
 */
package com.liquidlabs.space.map;

import java.util.Date;

import org.prevayler.Transaction;

public class Put implements Transaction {

	private final String newKey;
	private final String newValue;
	private final boolean passEventToPeers;

	public Put(String newKey, String newValue, boolean passEventToPeers) {
		this.newKey = newKey;
		this.newValue = newValue;
		this.passEventToPeers = passEventToPeers;
	}

	public void executeOn(Object arg0, Date arg1) {
		Map map = (Map) arg0;
		try {
			map.put(newKey, newValue, passEventToPeers);
		} catch (MapIsFullException e) {
			throw new RuntimeException(e);
		}
	}
	
}