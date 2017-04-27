package com.liquidlabs.log.search.functions.txn;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.carrotsearch.hppc.IntArrayList;
import com.liquidlabs.log.search.Query;
import com.liquidlabs.log.search.functions.ValueSetter;

public class Slowest implements SynthFunction {

	protected String TAG = "Slowest";
	private final int totalBucketCount;

	public Slowest(int totalBucketCount) {
		this.totalBucketCount = totalBucketCount;
	}
	List<Long> accumulated = new ArrayList<Long>();
	String txn = "";

	public void apply(int currentPos, Map<String, IntArrayList> txns, ValueSetter valueSetter, long bucketStart, long bucketEnd, long requestStartTimeMs, Query query) {
		// iterate over all txns and sum currPos + currPos -1
		if (currentPos == 0) {
			accumulated.add(0l);
			getSpecificTxn(txns);
			valueSetter.set(TAG + "-" + txn, 0, false);
		} else {
			long avgElapsed = getAvgElapsed(currentPos, txns, requestStartTimeMs);
			if (avgElapsed != -1) {
				accumulated.add(avgElapsed);
				valueSetter.set(TAG + "-" + txn, avgElapsed, false);
			}
		}	
		
		if (currentPos == totalBucketCount -1) {
			accumulated.clear();
		}
	}

	private long getAvgElapsed(int currentPos, Map<String, IntArrayList> txns, long requestStartTimeMs) {
//		int total = 0;
		try {
			String slowest = getSpecificTxn(txns);
			IntArrayList txnSet = txns.get(slowest);
			if (currentPos >= txnSet.size()) return -1;
			long prevTime = ((long)txnSet.get(0)) * SyntheticTransAccumulate.DIVISOR;
			long now = ((long)txnSet.get(currentPos)) * SyntheticTransAccumulate.DIVISOR;
			return now - prevTime;
		} catch (Throwable t) {
			t.printStackTrace();
			return 0;
			
		}
//		return accumulated.get(accumulated.size()-1) +  total/txns.size();
	}

	protected String getSpecificTxn(Map<String, IntArrayList> txns) {
		String result = txns.keySet().iterator().next();
		long maxSize = 0;
		for (String key : txns.keySet()) {
			IntArrayList list = txns.get(key);
			if (list.size() > 0) {
				long elapsed = new Long(list.get(list.size()-1)) * SyntheticTransAccumulate.DIVISOR - new Long(list.get(0)) * SyntheticTransAccumulate.DIVISOR;
				if (elapsed > maxSize) {
					result = key;
					maxSize = elapsed;
				}
			}
		}
		this.txn = result;
		return result;
	}
}
