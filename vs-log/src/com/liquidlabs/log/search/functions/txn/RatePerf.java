package com.liquidlabs.log.search.functions.txn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.carrotsearch.hppc.IntArrayList;
import com.liquidlabs.log.search.Query;
import com.liquidlabs.log.search.functions.ValueSetter;

/**
 * Plots line chart with Y value showing elapsed, min, max, avg, values
 * i.e.
 * |     o---------o
 * | o---o         o----oo----o
 * |---12:00---13:00---14:00--15:00
 * @author Neil
 *
 */
public class RatePerf implements SynthFunction {

	private final int totalBucketCount;

	public RatePerf(int totalBucketCount) {
		this.totalBucketCount = totalBucketCount;
	}
	Map<String, List<Long>> txnStartEndElapsed = new HashMap<String, List<Long>>();

	public void apply(int currentPos, Map<String, IntArrayList> txns, ValueSetter valueSetter, long bucketStart, long bucketEnd, long requestStartTimeMs, Query query) {
		
		// rebuild the txns so we only do it once
		if (currentPos == 0) {
			txnStartEndElapsed.clear();
		}
		Set<String> keySet = txns.keySet();
		List<Integer> elapsedTimes = new ArrayList<Integer>();
		for (String txnId : keySet) {
			List<Long> startEndElapsed = getStartEndElapsed(txnId, txns.get(txnId), requestStartTimeMs);
			// endTime
			if (isInBucket(bucketStart,bucketEnd, startEndElapsed.get(1))) {
				elapsedTimes.add(startEndElapsed.get(2).intValue());
			}
			// cache it for the next runs through
			if (currentPos == 0) txnStartEndElapsed.put(txnId, startEndElapsed);
		}
		List<Long> minMaxAvg = getMinMaxAvg(elapsedTimes);
		valueSetter.set("Min", minMaxAvg.get(0), false);
		valueSetter.set("Max", minMaxAvg.get(1), false);
		valueSetter.set("Avg", minMaxAvg.get(2), false);
		
		if (currentPos == totalBucketCount -1) {
			txnStartEndElapsed.clear();
		}
	}


	private List<Long> getMinMaxAvg(List<Integer> elapsedTimes) {
		long min = Long.MAX_VALUE;
		long max = 0;
		long avg = 0;
		long total = 0;
		if (elapsedTimes.size() == 0) return Arrays.asList(0L, 0L, 0L);
		for (Integer integer : elapsedTimes) {
			total += integer;
			if (min > integer) min = integer;
			if (max < integer) max = integer;
		}
		if (elapsedTimes.size() > 0) avg = total/elapsedTimes.size();
		return Arrays.asList(min, max, avg);
	}


	private boolean isInBucket(long bucketStart, long bucketEnd, Long time) {
		return (time >= bucketStart && time <= bucketEnd);
	}

	private List<Long> getStartEndElapsed(String txnId, IntArrayList list, long requestStartTimeMs) {
		if (txnStartEndElapsed.containsKey(txnId)) return txnStartEndElapsed.get(txnId);
		long startTime = requestStartTimeMs + ((long)list.get(0)) * SyntheticTransAccumulate.DIVISOR;
		long endTime = requestStartTimeMs + ((long)list.get(list.size()-1)) * SyntheticTransAccumulate.DIVISOR;
		return Arrays.asList(startTime, endTime, (endTime - startTime));
	}
}
