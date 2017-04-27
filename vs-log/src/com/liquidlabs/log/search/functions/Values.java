package com.liquidlabs.log.search.functions;

import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.filters.Filter;
import com.liquidlabs.log.space.LogRequest;
import org.apache.log4j.Logger;

import java.util.*;


/**
 * 
 *
 */
public class Values extends FunctionBase implements Function, FunctionFactory {

	private final static Logger LOGGER = Logger.getLogger(Values.class);

	private static final long serialVersionUID = 1L;
	private int maxUnique = Integer.getInteger("countU.max", 500);
	public static final int SUMM_MAX = 50;
	// This is specified/used for the AutoCount Bucket
	int maxAggSize = 5000;//LogProperties.getMaxCountAgg();

	// WARNING
	public String groupByGroup;

	// "This is a warning message" (i.e. count types of warning messages)
	public String applyToGroup;

	public String tag;

	private Map<String, Set<String>> groups = new HashMap<String, Set<String>> ();

	public Values(){
	}
	public Values(String tag, String groupByGroup, String applyToGroup) {
        super(Values.class.getSimpleName());
		this.tag = tag;
		this.groupByGroup = groupByGroup;
		this.applyToGroup = applyToGroup;
		if (this.groupByGroup.equals(applyToGroup)) this.groupByGroup = EMPTY_STRING;
	}
	public void setMaxAggSize(int max) {
		this.maxAggSize = max;
	}
	public void setMaxUnique(int maxUnique) {
		this.maxUnique = maxUnique;
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

		final String[] groupApply = super.getValues(matchResult, fieldSet, fields, applyToGroup, groupByGroup, tag, lineNumber);
		
		if (groupByGroup.length() == 0) {
			// we are counting unique values on the applyTo field - in which can groupBy will be the tag. i.e. user.countUnique(,TAG);
            if (groupApply[1] == null) return false;
			execute(groupApply[0], groupApply[1]);
		} else {
			// we are counting unique values with proper groupsi.e. user.countUnique(groups,TAG);
			execute(groupApply[0],  groupApply[1]);
		}
        return true;
	}
	public void execute(String applyTo, String groupBy) {
		if (applyTo == null) return;
		if (groupBy == null) return;
		Set<String> group = getGroups(applyTo);
		if (group == null) return;
		if (group.size() < maxUnique) group.add(groupBy);
	}
	protected int increment(Integer integer) {
		return 1;
	}
	private Set<String> getGroups(String groupBy) {
		Set<String> map = getGroups().get(groupBy);
		if (map == null) {
			if (getGroups().size() > maxAggSize) return null;
			map = new HashSet<String>();
			getGroups().put(groupBy, map);
		} else if (map.size() > maxAggSize) return null;
		return map;
	}
	
	public String getTag() {
		return tag;
	}

	public Function create() {
		return new Values(tag, groupByGroup, applyToGroup);
	}
	
	public Map getResults() {
		Map<String, Set<String>> groups2 = getGroups();
		HashMap<String, Set<String>> results = new HashMap<String, Set<String>>();
		
		
		// BEWARE - new HashSet(copy) - can sometimes gointo an infinite loop - so we need to break it out and if the 'pos' goes nuts then exit.
		int pos = 0;
		for (String key : groups2.keySet()) {
			Set<String> c = groups2.get(key);
			HashSet<String> copyHashSet = new HashSet<String>();
			for (String item : c) {
				if (!copyHashSet.contains(item)) {
					copyHashSet.add(item);
				}
				if (pos++ > 100 * 1024) {
					LOGGER.warn("CUnique:Breakout needed:" + item + " pos:" + pos);
					return results;
				}
				
			}
			results.put(key, copyHashSet);
		}
		return results;
	}
	
	////////// Used to agg from other buckets with same data
	public void updateResult(String groupName, Set<String> otherGroupCountMap) {
		if (!getGroups().containsKey(groupName)) {
			getGroups().put(groupName, otherGroupCountMap);
		} else {
			getGroups().get(groupName).addAll(otherGroupCountMap);
		}
	}
	private Map<String, Set<String>> getGroups() {
		if (this.groups == null) this.groups = new HashMap<String, Set<String>> ();
		return groups;
	}

	public void extractResult(String key, Set<String> otherFunctionMap, ValueSetter valueSetter, long start, long end,
			int currentBucketPos, int querySourcePos, int totalBucketCount, Map<String, Object> sharedFunctionData) {
		// 			int number = ((Set<String>) map.get(mapKey)).size();
		
		if (valueSetter != null) {
            for (String s : otherFunctionMap) {
                valueSetter.set(key  + ".value." + s, otherFunctionMap.size(), false);
            }
		}
	}
	public String getApplyToField() {
		return applyToGroup;
	}
	public String groupByGroup() {
		return groupByGroup;
	}



}
	