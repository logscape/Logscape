package com.liquidlabs.log.search.functions;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.filters.Filter;
import com.liquidlabs.log.space.LogRequest;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * 
 * CountUnique instances of a group - 
 * useful for ascertaining membership where each member writes to its own log line.
 * i.e. 
 * engine:3301 joiningA
 * engine:3301 joiningA
 * engine:3302 joiningA
 * engine:3302 joiningA
 * engine:3302 joiningB.
 * 
 * Results:
 * countUnique(1) == 2
 * countUnique(1,2) = 3301=1 3302=2
 *
 * TODO: HLL Impl - maybe IntHyperLogLogCounterArray
 *
 */
public class CountUnique extends FunctionBase implements Function, FunctionFactory {
	
	private final static Logger LOGGER = Logger.getLogger(CountUnique.class);
	
	private static final long serialVersionUID = 1L;
	public static final int SUMM_MAX = 50;
	// This is specified/used for the AutoCount Bucket
	int maxAggSize = 5000;//LogProperties.getMaxCountAgg();
	
	// WARNING
	public String groupByGroup;
	
	// "This is a warning message" (i.e. count types of warning messages)
	public String applyToGroup;
	
	public String tag; 	

	private Map<String, HyperLogLog> groups = new HashMap<String, HyperLogLog> ();
	
	public CountUnique(){
	}
	public CountUnique(String tag, String groupByGroup, String applyToGroup) {
        super(CountUnique.class.getSimpleName());
		this.tag = tag;
		this.groupByGroup = groupByGroup;
		this.applyToGroup = applyToGroup;
		if (this.groupByGroup.equals(applyToGroup)) this.groupByGroup = EMPTY_STRING;
	}
	public boolean isBucketLevel() {
		return true;
	}
	
	public String group() {
		return applyToGroup;
	}
	
	public String toString() {
		return hashCode() + " " + toStringId();
	}

	public boolean execute(FieldSet fieldSet, String[] fields, String pattern, long time, MatchResult matchResult, String rawLineData, long requestStartTimeMs, int lineNumber) {
		
		if (getGroups().size() > maxAggSize) return false;

		final String[] applyAndGroup = super.getValues(matchResult, fieldSet, fields, applyToGroup, groupByGroup, tag, lineNumber);
        execute(applyAndGroup[0], applyAndGroup[1]);
        return true;

	}
    public void execute(String apply, String group) {

        if (groupByGroup.length() == 0) {
            // we are counting unique values on the applyTo field - in which can groupBy will be the tag. i.e. user.countUnique(,TAG);
            if (group == null) return ;
            apply(apply, group);
        } else {
            // we are counting unique values with proper groupsi.e. user.countUnique(groups,TAG);
            apply(apply,  group);
        }

    }
	private boolean apply(String groupBy, String applyTo) {
		if (applyTo == null) return false;
		if (groupBy == null) return false;
        ICardinality group = getGroups(applyTo);
		if (group == null) return false;
		group.offer(groupBy);
		return true;
	}
	protected int increment(Integer integer) {
		return 1;
	}
	private HyperLogLog getGroups(String groupBy) {
        HyperLogLog map = getGroups().get(groupBy);
		if (map == null) {
			if (getGroups().size() > maxAggSize) return null;
			map = new HyperLogLog(10);
			getGroups().put(groupBy, map);
        }
		return map;
	}
	
	public String getTag() {
		return tag;
	}

	public Function create() {
		return new CountUnique(tag, groupByGroup, applyToGroup);
	}
	
	public Map getResults() {
		Map<String, HyperLogLog> groups2 = getGroups();
		HashMap<String, ICardinality> results = new HashMap<String, ICardinality>();
		
		for (String key : groups2.keySet()) {
            ICardinality c = groups2.get(key);
			results.put(key, c);
		}
		return results;
	}
	
	////////// Used to agg from other buckets with same data
	public void updateResult(String groupName, ICardinality otherGroupCountMap) {
        throw new RuntimeException("Not implemented");


	}
	private Map<String,HyperLogLog> getGroups() {
		if (this.groups == null) this.groups = new HashMap<String, HyperLogLog> ();
		return groups;
	}
	public void updateResult(String key, Map<String, Object> otherFunctionMap) {
        if (!getGroups().containsKey(key)) {
            getGroups().put(key, (HyperLogLog) otherFunctionMap.get(key));
        } else {
            try {
                ICardinality merged = getGroups().get(key).merge((HyperLogLog) otherFunctionMap.get(key));
                getGroups().put(key, (HyperLogLog) merged);
            } catch (CardinalityMergeException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
	}
	
	public void updateResult(String groupBy, Number value) {
        throw new RuntimeException("Not implemented");
	}
	
	public void extractResult(FieldSet fieldSet, String currentGroupKey, String key, Map<String, Object> map, ValueSetter valueSetter, long start, long end, int currentBucketPos, int querySourcePos, int totalBucketCount, Map<String, Object> sharedFunctionData, long requestStartTimeMs, LogRequest request) {
        if (valueSetter != null) {
            String mapKey = key;
            valueSetter.set(mapKey, ((ICardinality)map.get(currentGroupKey)).cardinality(), false);
        }
    }
	public void extractResult(String key, Set<String> otherFunctionMap, ValueSetter valueSetter, long start, long end,
			int currentBucketPos, int querySourcePos, int totalBucketCount, Map<String, Object> sharedFunctionData) {
	}
	public void flushAggResultsForBucket(int querySourcePos, Map<String, Object> sharedFunctionData) {
	}
	
	public void extractResult(String key, Number value, ValueSetter valueSetter, Map<String, Object> sharedFunctionData, int querySourcePos, int currentBucketPos, int totalBucketCount, Map<String, Object> sourceDataForBucket, long start) {
        LOGGER.info("Not Implemented");
	}
	protected int updateResultIncrement(Number otherCountValue, Integer myCount) {
		if (myCount + otherCountValue.intValue() > 0) return 1;
		return 0;
	}
	public String getApplyToField() {
		return applyToGroup;
	}
	public String groupByGroup() {
		return groupByGroup;
	}
}
	