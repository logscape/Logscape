package com.liquidlabs.log.search.functions.txn;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.liquidlabs.log.search.Query;

import com.carrotsearch.hppc.IntArrayList;
import com.liquidlabs.common.DateUtil;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.search.functions.ValueSetter;

/**
 * Plots simple table view of txn information
 * i.e.
 * | txnId, start, end, steps, durationMs
 * | 12334 | now | then | 100 | 200    
 * | ererr | now | then | 222 | eee
 * @author Neil
 *
 */
public class Table implements SynthFunction {

	public Table(int totalBucketCount) {
	}
	public void apply(int currentPos, Map<String, IntArrayList> txns, ValueSetter valueSetter, long bucketStart, long bucketEnd, long requestStartTimeMs, Query query) {
		
		for (String txnId : txns.keySet()) {
			List<Long> startEndElapsedSteps = getStartEndElapsedSteps(txnId, txns.get(txnId), requestStartTimeMs);

// from the By function
//			if (getTag().length() > 0) value = getTag() + LogProperties.getFunctionSplit() + value;
//			valueSetter.set(value  +  ".by." +currentGroupKey, -1, true);
// TODO: prob want ability to extract other params for this table...params from txnId - i.e. user:(*), trade or something from the uid - i.e. some collected state
			
			valueSetter.set("Start-" + LogProperties.getFunctionSplit() + DateUtil.longDTFormatter.print(startEndElapsedSteps.get(0)) + ".by." + txnId, 0, false);
			valueSetter.set("End-" + LogProperties.getFunctionSplit() + DateUtil.longDTFormatter.print(startEndElapsedSteps.get(1)) + ".by." + txnId, 0, false);
			valueSetter.set("ElapsedMs-" + LogProperties.getFunctionSplit() + startEndElapsedSteps.get(2) + ".by." + txnId, 0, false);
			valueSetter.set("Steps-" + LogProperties.getFunctionSplit() + startEndElapsedSteps.get(3) + ".by." + txnId, 0, false);
		}
	}


	private boolean isInBucket(long bucketStart, long bucketEnd, Long time) {
		return (time >= bucketStart && time <= bucketEnd);
	}

	private List<Long> getStartEndElapsedSteps(String txnId, IntArrayList list, long requestStartTimeMs) {
		long startTime = requestStartTimeMs + ((long)list.get(0)) * SyntheticTransAccumulate.DIVISOR;
		long endTime = requestStartTimeMs + ((long)list.get(list.size()-1)) * SyntheticTransAccumulate.DIVISOR;
		return Arrays.asList(startTime, endTime, (endTime - startTime), Long.valueOf(list.size()));
	}
}
