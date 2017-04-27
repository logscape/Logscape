package com.liquidlabs.log.search.functions.txn;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.carrotsearch.hppc.IntArrayList;
import com.liquidlabs.log.search.Query;
import com.liquidlabs.log.search.functions.ValueSetter;

public class Avg implements SynthFunction {

	private final int totalBucketCount;

	public Avg(int totalBucketCount) {
		this.totalBucketCount = totalBucketCount;
	}
	List<Long> accumulated = new ArrayList<Long>();

	// pull out avg value for step-X of all txns
	public void apply(int currentPos, Map<String, IntArrayList> txns, ValueSetter valueSetter, long bucketStart, long bucketEnd, long requestStartTimeMs, Query query) {
		// iterate over all txns and sum currPos + currPos -1
		if (currentPos == 0) {
			accumulated.add(0l);
			valueSetter.set("Avg", 0, false);
		} else {
			long avgElapsed = getAvgElapsed(currentPos, txns, requestStartTimeMs);
			if (avgElapsed != -1) {
				accumulated.add(avgElapsed);
				valueSetter.set("Avg", avgElapsed, false);
			}
		}	
		
		if (currentPos == totalBucketCount -1) {
			accumulated.clear();
		}

	}

	private long getAvgElapsed(int currentPos, Map<String, IntArrayList> txns, long requestStartTimeMs) {
		long total = 0;
		int hitCount = 0;
		Set<String> keySet = txns.keySet();
		for (String string : keySet) {
			try {
				IntArrayList txnSet = txns.get(string);
				if (currentPos >= txnSet.size()) continue;
				hitCount++;
				long  prevTime = ((long)txnSet.get(currentPos -1)) * SyntheticTransAccumulate.DIVISOR;
				long now = ((long)txnSet.get(currentPos)) * SyntheticTransAccumulate.DIVISOR;
				total += (now - prevTime);
			} catch (Throwable t) {
				t.printStackTrace();
			}
		}
		if (hitCount == 0) return -1;
		if (accumulated.size() == 0) return 0;
		return accumulated.get(accumulated.size()-1) +  total/txns.size();
	}
}
