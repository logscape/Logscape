/**
 * 
 */
package com.liquidlabs.space.map;

import java.util.Date;

import org.prevayler.SureTransactionWithQuery;

class Remove implements SureTransactionWithQuery {

	private final boolean passEventToPeers;
	private final String key;

	public Remove(String key, boolean passEventToPeers) {
		this.key = key;
		this.passEventToPeers = passEventToPeers;
	}

	public Object executeAndQuery(Object arg0, Date arg1) {
		Map map = (Map) arg0;
		return map.remove(key, passEventToPeers);
	}
	
}