package com.liquidlabs.log.search.functions.txn;

import java.util.Map;

import com.carrotsearch.hppc.IntArrayList;

public class Fastest extends Slowest {

	public Fastest(int count) {
		super(count);
		TAG = "Fastest";
	}
	

	protected String getSpecificTxn(Map<String, IntArrayList> txns) {
		String result = txns.keySet().iterator().next();
		int minSize = Integer.MAX_VALUE;
		for (String key : txns.keySet()) {
			IntArrayList list = txns.get(key);
			if (list.size() > 0) {
				int elapsed = list.get(list.size()-1) - list.get(0);
				if (elapsed < minSize) {
					result = key;
					minSize = elapsed;
				}
			}
		}
		this.txn = result;
		return result;
	}

}
