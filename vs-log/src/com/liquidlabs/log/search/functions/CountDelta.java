package com.liquidlabs.log.search.functions;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.filters.Filter;
import com.liquidlabs.log.space.LogRequest;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 
 * CountDelta instances of a group - 
 * useful for ascertaining differences in activity.
 * i.e. 
 * Time 0
 * engine:3301 joining
 * engine:3301 joining
 * engine:3302 joining..
 * Time 1
 * engine:3302 joining..
 * engine:3302 joining..
 * engine:3302 joining..
 * engine:3302 joining..
 * 
 * 
 * Resulting value 
 * Time 0 
 *  	engine:3301:1 
 *  	engine:3302:1 
 * Time 1
 *  	engine:3301:-2 
 *  	engine:3301:3
 * 	
 */

public class CountDelta extends FunctionBase implements Function, FunctionFactory {
	
	private static final int MaxAggSize = LogProperties.getMaxCountAgg();
	
	private static final long serialVersionUID = 1L;
	
	// WARNING
	protected String groupByGroup;
	
	// "This is a warning message" (i.e. count types of warning messages)
	protected String applyToGroup;

	private Map<String, Map<String, Integer>> groups = new ConcurrentHashMap<String, Map<String, Integer>> ();
	protected String tag; 	
	
	public CountDelta(){
	}
	public CountDelta(String tag, String groupByGroup, String applyToGroup) {
        super(CountDelta.class.getSimpleName());
		this.tag = tag;
		this.groupByGroup = groupByGroup;
		this.applyToGroup = applyToGroup;
	}
	
	public boolean isBucketLevel() {
		return true;
	}

	public String group() {
		return applyToGroup;
	}

	
	public boolean execute(FieldSet fieldSet, String[] fields, String pattern, long time, MatchResult matchResult, String rawLineData, long requestStartTimeMs, int lineNumber) {
		
		String[] applyAndGroup = super.getValues(matchResult, fieldSet, fields, applyToGroup, groupByGroup, tag, lineNumber);

        execute(applyAndGroup[0], applyAndGroup[1]);
		
		return true;
	}
    public void execute(String apply, String group) {
        Map<String, Integer> grouping = getGroups(apply);
        if (group == null) return;
        Integer integer = getCount(group, grouping);
        grouping.put(apply, increment(integer));
    }
	protected int increment(Integer integer) {
		return integer + 1;
	}
	private Integer getCount(String key, Map<String, Integer> map) {
		Integer integer = map.get(key);
		if (integer == null) {
			integer = 0;
		}
		return integer;
	}
	private Map<String, Integer> getGroups(String groupBy) {
		Map<String, Integer> map = groups.get(groupBy);
		if (map == null) {
			if (groups.size() > MaxAggSize) return null;
			map = new HashMap<String, Integer>();
			groups.put(groupBy, map);
		}
		return map;
	}
	
	public String getTag() {
		return tag;
	}

	public Function create() {
		return new CountDelta(tag, groupByGroup, applyToGroup);
	}
	
	@SuppressWarnings("unchecked")
	public Map getResults() {
		return groups;
	}
	
	////////// Used to agg from other buckets with same data
	public void updateResult(String groupName, Map<String, Object> otherGroupCountMap) {
		for (String mapKey : otherGroupCountMap.keySet()) {
			Number otherCountValue = (Number) otherGroupCountMap.get(mapKey);
			
			// update our value with the new one
			Map<String, Integer> countMap = getGroups(groupName);
			if (countMap == null) continue;
			Integer myCount = getCount(mapKey, countMap);
			countMap.put(mapKey, updateResultIncrement(otherCountValue, myCount));
		}
	}
	
	public void updateResult(String groupBy, Number value) {
		Map<String, Integer> group = getGroups(groupBy);
		if (group == null) return;
		Integer integer = group.get(groupBy);
		if (integer == null) integer = 0;
		group.put(groupBy, updateResultIncrement(integer.intValue(), value.intValue()));
	}
	
	////////// Used to convert to HistoItemXML
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
	

	public void flushAggResultsForBucket(int querySourcePos, Map<String, Object> sharedFunctionData) {
		String sharedStateKey = this.toStringId() + " qpos:" + querySourcePos;
		ArrayList sharedState = (ArrayList) sharedFunctionData.get(sharedStateKey);

		for (int currentBucketPos = 1; currentBucketPos < sharedState.size(); currentBucketPos++) {
			Data start = ((Data)sharedState.get(currentBucketPos));
		
			Map<String, Integer> delta = getDeltaFromBucketDatas((Data)sharedState.get(currentBucketPos-1), (Data)sharedState.get(currentBucketPos));
			for (String deltaKey : delta.keySet()) {
				if (start.valueSetter != null) {
					Integer deltaValue = delta.get(deltaKey);
					start.valueSetter.set(applyTag(tag, deltaKey), deltaValue, true);
				}
			}
		}
	}
    Map<String, Integer> getDeltaFromBucketDatas(Data previousBucket, Data myBucket) {
        HashSet<String> allKeys = new HashSet<String>(previousBucket.dataValues.keySet());
        allKeys.addAll(myBucket.dataValues.keySet());

        HashMap<String, Integer> result = new HashMap<String, Integer>();
        for (String key : allKeys) {
            if (previousBucket.dataValues.containsKey(key) && !myBucket.dataValues.containsKey(key)) {
                Integer integerA = previousBucket.dataValues.get(key);
                result.put(key, integerA * -1);
            } else if (!previousBucket.dataValues.containsKey(key) && myBucket.dataValues.containsKey(key)) {
                Integer integerB = myBucket.dataValues.get(key);
                result.put(key, integerB);
            } else {
                // both contain values
                Integer integerA = previousBucket.dataValues.get(key);
                Integer integerB = myBucket.dataValues.get(key);
                result.put(key, integerA.intValue() - integerB.intValue());
            }
        }

        return result;
    }

	protected int updateResultIncrement(Number otherCountValue, Integer myCount) {
		return myCount + otherCountValue.intValue();
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
				dataValues.put(key, (Integer) sourceDataForBucket.get(key));
			}
		}
		
		ValueSetter valueSetter;
		long timeMs;
		Map<String, Integer> dataValues = new HashMap<String, Integer>();
		public String toString() {
			return String.format("%s %s", DateUtil.shortTimeFormat.print(timeMs), dataValues);
		}
	}
	public String getApplyToField() {
		return applyToGroup;
	}
	public String groupByGroup() {
		return groupByGroup;
	}


}
	