package com.liquidlabs.log.search.functions;

import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.filters.Filter;
import com.liquidlabs.log.space.LogRequest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Sum extends FunctionBase implements FunctionFactory, Function {
	
	private static final long serialVersionUID = 966756058010729597L;
	String tag;
	String groupByGroup;
	String applyToGroup;
	
	//Map<String, Averager> groups = new ConcurrentHashMap<String, Averager>();
	Map<String, Averager> groups = new ConcurrentHashMap<String, Averager>();
	transient int maxAgg = LogProperties.getMaxCountAgg();

	public Sum() {}
	
	public Sum(String tag, String groupByGroup, String applyToGroup) {
        super(Sum.class.getSimpleName());
		this.tag = tag;
		this.groupByGroup = groupByGroup;
		this.applyToGroup = applyToGroup;
	}
	
	public Function create() {
		return new Sum(tag, groupByGroup, applyToGroup);
	}
	public boolean isBucketLevel() {
		return true;
	}
	public boolean execute(FieldSet fieldSet, String[] fields, String pattern, long time, MatchResult matchResult, String rawLineData, long requestStartTimeMs, int lineNumber) {

		super.executeAgainstValues(matchResult, fieldSet, fields, applyToGroup, groupByGroup, tag, lineNumber);
		return false;
	}

	// bytes.sum(_hostname, TO_GB) TO_GB.execute(bytes / 1024 / 1024)
	public void execute(String applyValue, String groupValue) {
		if (applyValue == null || applyValue.length() == 0) return;
		if (groupValue == null && groupByGroup != null && groupByGroup.length() > 0) return;

		String groupBy = groupValue == null ? "" : groupValue;

		Averager averager = groups.get(groupBy);
		if (averager == null) {
			if (groups.size() > maxAgg) return;
			averager = new Averager();
			groups.put(groupBy, averager);
		}
        String val = applyValue;
		try {
			if (val.indexOf(",") != -1) val = val.replaceAll(",", "");
			Double double1 = StringUtil.isDouble(val);
			if (double1 != null) averager.add(double1);
			else averager.add(1);
		} catch (Throwable t ) {
			// else it wasnt a numberic - so increment by 1 - 
			//i.e. might be wanting to sum non-numeric instances. i.e. host.sum(resType)
			averager.add(1);
		}
	}

	@SuppressWarnings("unchecked")
	public Map getResults() {
		Map<String, Double> sums = new HashMap<String, Double>();
		for (Entry<String, Averager> entry : groups.entrySet()) {
			sums.put(entry.getKey(), entry.getValue().sum());
		}
		return sums;
	}

	public String getTag() {
		return tag;
	}

	public void updateResult(String key, Map<String, Object> map) {
		for (String mapKey : map.keySet()) {
			String aggKey = key + LogProperties.getFunctionSplit() + mapKey;
			Averager averager = groups.get(aggKey);
			if (averager == null) {
				averager = new Averager();
				groups.put(aggKey, averager);
			}
			averager.add(((Double) map.get(aggKey)).doubleValue());
		}
		
	}

	public void updateResult(String key, Number value) {
		Averager averager = groups.get(key);
		if (averager == null) {
			averager = new Averager();
			groups.put(key, averager);
		}
		averager.add(value.doubleValue());
	}
	
	public void extractResult(String key, Number value, ValueSetter valueSetter, Map<String, Object> sharedFunctionData, int querySourcePos, int currentBucketPos, int totalBucketCount, Map<String, Object> sourceDataForBucket, long start) {
        Double myValue = groups.containsKey(key) ? groups.get(key).sum() : null;
        super.extractResults(applyTag(tag, key), value, valueSetter, myValue);
	}
	
	public String group() {
		return applyToGroup;
	}
	@Override
	public String getApplyToField() {
		return applyToGroup;
	}
	public String groupByGroup() {
		return groupByGroup;
	}
	
	public void reset() {
		groups = new ConcurrentHashMap<String, Averager>();
	}
}
