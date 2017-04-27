package com.liquidlabs.log.search.functions.txn;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.carrotsearch.hppc.IntArrayList;
import com.liquidlabs.log.search.Query;
import com.liquidlabs.log.search.functions.ValueSetter;

/**
 * Plots line chart with Y value showing the number of txns alive i..e 5txns
 * i.e.
 * |     o---------o
 * | o---o         o----oo----o
 * |---12:00---13:00---14:00--15:00
 * @author Neil
 *
 */
public class Rate implements SynthFunction {

	private final int totalBucketCount;

	public Rate(int totalBucketCount) {
		this.totalBucketCount = totalBucketCount;
	}
	Map<String, List<Long>> txnStartEndElapsed = new HashMap<String, List<Long>>();

	public void apply(int currentPos, Map<String, IntArrayList> txns, ValueSetter valueSetter, long bucketStart, long bucketEnd, long requestStartTimeMs, Query query) {
		
		// rebuild the txns so we only do it once
		if (currentPos == 0) {
			txnStartEndElapsed.clear();
		}
		Set<String> keySet = txns.keySet();
		int rateCountForThisBucket = 0;
		for (String txnId : keySet) {
			List<Long> startEndElapsed = getStartEndElapsed(txnId, txns.get(txnId), requestStartTimeMs);
			// startTime Hit
//			String txInfo = currentPos + " tx:" + txnId + "\t" + DateUtil.shortDateTimeFormat7.print(startEndElapsed.get(0)) + " -" + DateUtil.shortDateTimeFormat7.print(startEndElapsed.get(1));
//			String bInfo = "b:" + DateUtil.shortDateTimeFormat7.print(bucketStart) + " -" + DateUtil.shortDateTimeFormat7.print(bucketEnd);
//			String info = txInfo + " " + bInfo;
			if (isInBucket(bucketStart,bucketEnd, startEndElapsed.get(0))) {
//				System.out.println("START:" + info);
				rateCountForThisBucket++;
			}
			// endTime
			else if (isInBucket(bucketStart,bucketEnd, startEndElapsed.get(1))) {
//				System.out.println("END:" + info);
				rateCountForThisBucket++;
		    // txStart < bStart & txEnd > bStart
			} else if (startEndElapsed.get(0) < bucketStart && startEndElapsed.get(1) > bucketStart) {
//				System.out.println("IN:" + info);
				rateCountForThisBucket++;
			} 
			// txEnd > bEnd & txStart < bEnd
			else if (startEndElapsed.get(1) > bucketEnd && startEndElapsed.get(0) < bucketEnd) {
				rateCountForThisBucket++;
			}
			// cache it for the next runs through
			if (currentPos == 0) txnStartEndElapsed.put(txnId, startEndElapsed);
		}
		valueSetter.set("Rate", rateCountForThisBucket, false);
		if (currentPos == 0 || currentPos == totalBucketCount-1) valueSetter.set("Rate-Trend", rateCountForThisBucket, false);	
		if (currentPos == totalBucketCount -1) {
			txnStartEndElapsed.clear();
		}

	}


	private boolean isInBucket(long bucketStart, long bucketEnd, Long time) {
		return (time >= bucketStart && time <= bucketEnd);
	}

	private List<Long> getStartEndElapsed(String txnId, IntArrayList list, long requestStartTimeMs) {
		if (txnStartEndElapsed.containsKey(txnId)) return txnStartEndElapsed.get(txnId);
		if (list.size() == 0) return Arrays.asList(0l,0l,0l);
		long startTime = requestStartTimeMs + ((long) list.get(0)) * SyntheticTransAccumulate.DIVISOR;
		long endTime = requestStartTimeMs + ((long) list.get(list.size()-1)) * SyntheticTransAccumulate.DIVISOR;
		return Arrays.asList(startTime, endTime, (endTime - startTime));
	}
}
