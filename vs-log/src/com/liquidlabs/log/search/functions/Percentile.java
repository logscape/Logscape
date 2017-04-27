package com.liquidlabs.log.search.functions;

import com.clearspring.analytics.stream.quantile.TDigest;
import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.space.LogRequest;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * 
 *
 */
public class Percentile extends FunctionBase implements Function, FunctionFactory {

    private final static double[] DEFAULT_PERCENTS = new double[] { 1, 5, 25, 50, 75, 95, 99 };

	private final static Logger LOGGER = Logger.getLogger(Percentile.class);

	private static final long serialVersionUID = 1L;

	public String groupByGroup;

	public String applyToGroup;

	public String tag;

	private Map<String, TDigest> groups = new HashMap<String, TDigest> ();
	private transient int calls;

	public Percentile(){
	}
	public Percentile(String tag, String groupByGroup, String applyToGroup) {
        super(Percentile.class.getSimpleName());
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
		super.executeAgainstValues(matchResult, fieldSet, fields, applyToGroup, groupByGroup, tag, lineNumber);
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
		TDigest group = getGroups(applyTo);
		if (group == null) return false;
		Double value = StringUtil.isDouble(groupBy);
		if (value != null) group.add(value);
        applyCompression();
        return true;
	}

    private void applyCompression() {
        if (calls++ % 10000 == 0) {
            Map<String, TDigest> groups = getGroups();
            for (TDigest tDigest : groups.values()) {
                tDigest.compress();
            }
        }
    }

    protected int increment(Integer integer) {
		return 1;
	}
	private TDigest getGroups(String groupBy) {
		TDigest map = getGroups().get(groupBy);
		if (map == null) {
			map = new TDigest(100);
			getGroups().put(groupBy, map);
        }
		return map;
	}
	
	public String getTag() {
		return tag;
	}

	public Function create() {
		return new Percentile(tag, groupByGroup, applyToGroup);
	}
	
	public Map getResults() {
        applyCompression();
		Map<String, TDigest> groups2 = getGroups();
		HashMap<String, TDigest> results = new HashMap<String, TDigest>();

		for (String key : groups2.keySet()) {
			TDigest c = groups2.get(key);
			results.put(key, c);
		}
		return results;
	}
	
	////////// Used to agg from other buckets with same data
	public void updateResult(String groupName, TDigest otherGroupCountMap) {
        throw new RuntimeException("Not implemented");


	}
	private Map<String,TDigest> getGroups() {
		if (this.groups == null) this.groups = new HashMap<String, TDigest> ();
		return groups;
	}
	public void updateResult(String key, Map<String, Object> otherFunctionMap) {
        if (!getGroups().containsKey(key)) {
            getGroups().put(key, (TDigest) otherFunctionMap.get(key));
        } else {
            try {
                getGroups().get(key).add((TDigest) otherFunctionMap.get(key));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
	}
	
	public void updateResult(String groupBy, Number value) {
        throw new RuntimeException("Not implemented");
	}
	
	public void extractResult(FieldSet fieldSet, String currentGroupKey, String key, Map<String, Object> map, ValueSetter valueSetter, long start, long end, int currentBucketPos, int querySourcePos, int totalBucketCount, Map<String, Object> sharedFunctionData, long requestStartTimeMs, LogRequest request) {
        if (valueSetter != null) {
            String mapKey = currentGroupKey;
			//  use the TAG to guess cardinality
			Double aDouble = StringUtil.isDouble(tag);
            try {
                if (aDouble != null) {
                    valueSetter.set("p" + Integer.valueOf(aDouble.intValue()).toString() + "th", ((TDigest) map.get(currentGroupKey)).quantile(aDouble / 100.0), false);
                } else {
                    for (double defaultPercent : DEFAULT_PERCENTS) {
                            valueSetter.set(currentGroupKey + "_" + defaultPercent + "%", ((TDigest) map.get(currentGroupKey)).quantile(defaultPercent / 100.0), false);

                    }
                }
            } catch (Throwable t) {
                valueSetter.set(currentGroupKey + "_not_enough_data", 100, true);
            }
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
	public String getApplyToField() {
		return applyToGroup;
	}
	public String groupByGroup() {
		return groupByGroup;
	}
}
	