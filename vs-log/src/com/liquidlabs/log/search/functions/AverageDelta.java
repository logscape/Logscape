package com.liquidlabs.log.search.functions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.space.LogRequest;

public class AverageDelta extends Average {
	
	public Function create() {
		return new AverageDelta(tag, groupByGroup, applyToGroup);
	}
	
	public AverageDelta() {}

	public AverageDelta(String tag, String groupType, String apply) {
		super(tag, groupType, apply);
	}

	public void extractResult(FieldSet fieldSet, String currentGroupKey, String key, Map<String, Object> sourceDataForBucket, ValueSetter valueSetter,
                              long start, long end, int currentBucketPos, int querySourcePos, int totalBucketCount, Map<String, Object> sharedFunctionData, long requestStartTimeMs, LogRequest request) {
        String sharedStateKey = this.toStringId() + " qpos:" + querySourcePos;
        ArrayList sharedState = (ArrayList) sharedFunctionData.get(sharedStateKey);

        if (sharedState == null) {
            sharedState = new ArrayList<Data>();
            for (int i = 0; i < totalBucketCount; i++) {
                sharedState.add(new Data());
            }
            sharedFunctionData.put(sharedStateKey, sharedState);
        }

        Data data = (Data) sharedState.get(currentBucketPos);
        data.add(start, sourceDataForBucket, valueSetter);
	}
	
	protected Map<String, Double> getDeltaFromBucketDatas(Data previousBucket, Data myBucket) {
		HashSet<String> allKeys = new HashSet<String>(previousBucket.dataValues.keySet());
		allKeys.addAll(myBucket.dataValues.keySet());
		
		HashMap<String, Double> result = new HashMap<String, Double>();
		for (String key : allKeys) {
			if (previousBucket.dataValues.containsKey(key) && !myBucket.dataValues.containsKey(key)) {
				Double integerA = previousBucket.dataValues.get(key);
//				result.put(key, integerA * -1);
				result.put(key, 0.0);
			} else if (!previousBucket.dataValues.containsKey(key) && myBucket.dataValues.containsKey(key)) {
				Double integerB = myBucket.dataValues.get(key);
				// if no previous value - then put 0
				//result.put(key, integerB);
				result.put(key, 0.0);
			} else {
				// both contain values
				double prevValue = previousBucket.dataValues.get(key).doubleValue();
				double currValue = myBucket.dataValues.get(key).doubleValue();
				double delta = calculateDelta(currValue,prevValue);
				// the use case is for cumulative values - so rule out 0 as a delta
				if (prevValue == 0 && currValue > 0) delta = 0;
				result.put(key, delta);
			}
		}
		
		return result;
	}

    protected double calculateDelta(double currValue, double prevValue) {
        return currValue - prevValue;
    }

    public void flushAggResultsForBucket(int querySourcePos, Map<String, Object> sharedFunctionData) {
		String sharedStateKey = this.toStringId() + " qpos:" + querySourcePos;
		ArrayList sharedState = (ArrayList) sharedFunctionData.get(sharedStateKey);
		
		if (sharedState == null) return;

		for (int currentBucketPos = 1; currentBucketPos < sharedState.size(); currentBucketPos++) {
			Data start = ((Data)sharedState.get(currentBucketPos));
		
			Map<String, Double> delta = getDeltaFromBucketDatas((Data)sharedState.get(currentBucketPos-1), (Data)sharedState.get(currentBucketPos));
			for (String deltaKey : delta.keySet()) {
				if (start.valueSetter != null) {
					Double deltaValue = delta.get(deltaKey);
					start.valueSetter.set(applyTag(tag, deltaKey), deltaValue, true);
				}
			}
		}
	}
	public static class Data {
		
		public Data(){};
		public Data(long timeMs, Map<String, Object> sourceDataForBucket, ValueSetter valueSetter) {
			add(timeMs, sourceDataForBucket, valueSetter);
		}
		public void add(long timeMs, Map<String, Object> sourceDataForBucket, ValueSetter valueSetter) {
			this.timeMs = timeMs;
			this.valueSetter = valueSetter;
			for (String key : sourceDataForBucket.keySet()) {
				dataValues.put(key, ((Averager) sourceDataForBucket.get(key)).average());
			}
		}
		
		ValueSetter valueSetter;
		long timeMs;
		Map<String, Double> dataValues = new HashMap<String, Double>();
		public String toString() {
			return String.format("%s %s", DateUtil.shortTimeFormat.print(timeMs), dataValues);
		}
	}

}
