package com.liquidlabs.log.search.functions.txn;

import java.util.Map;

import com.carrotsearch.hppc.IntArrayList;
import com.liquidlabs.log.search.Query;
import com.liquidlabs.log.search.functions.ValueSetter;

public interface SynthFunction {

	void apply(int currentBucketPos, Map<String, IntArrayList> txns, ValueSetter valueSetter, long bucketStart, long bucketEnd, long requestStartTimeMs, Query query);

}
