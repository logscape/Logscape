/**
 * 
 */
package com.liquidlabs.log.search.facetmap;

import org.prevayler.Transaction;

import java.util.Date;
import java.util.Map;

public class PutTransaction<K,V> implements Transaction {

	private final K newKey;
	private final V newValue;

	public PutTransaction(K newKey, V newValue) {
		this.newKey = newKey;
		this.newValue = newValue;
	}

	public void executeOn(Object arg0, Date arg1) {
		Map<K,V> map = (Map<K,V>) arg0;
		map.put(newKey, newValue);
	}
}