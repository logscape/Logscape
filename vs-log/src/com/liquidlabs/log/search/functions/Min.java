package com.liquidlabs.log.search.functions;

import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.filters.Filter;
import com.liquidlabs.log.space.LogRequest;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Min extends FunctionBase implements FunctionFactory, Function {

	private static final long serialVersionUID = 1L;
	
	private static final Logger LOGGER = Logger.getLogger(Min.class);
	private static final int MaxAggSize = LogProperties.getMaxCountAgg();

	String tag;
	String groupByGroup;
	String applyToGroup;
	
	
	private Map<String, Double> groups = new ConcurrentHashMap<String, Double>();

	public Min() {}
	
	public Min(String tag, String groupByGroup, String applyToGroup) {
        super(Min.class.getSimpleName());
		this.tag = tag;
		this.groupByGroup = groupByGroup;
		this.applyToGroup = applyToGroup;
	}
	
	public boolean isBucketLevel() {
		return true;
	}
	public Function create() {
		return new Min(tag, groupByGroup, applyToGroup);
	}
	public String group() {
		return applyToGroup;
	}

	public boolean execute(FieldSet fieldSet, String[] fields, String pattern, long time, MatchResult matchResult, String rawLineData, long requestStartTimeMs, int lineNumber) {
		super.executeAgainstValues(matchResult, fieldSet, fields, applyToGroup, groupByGroup, tag, lineNumber);
		return false;
	}
    public void execute(String apply, String group) {
        try {
            String val = apply;
            if (val != null && val.length() > 0) {
                if (val.indexOf(",") != -1) val = val.replaceAll(",", "");

                Double double1 = StringUtil.isDouble(val);
                if (double1 == null) {
                    Double max = groups.get(group);
                    if (max == null) max = 1.0;
                    double1 = max;
                }

                addToGroup(group, double1);
            }
        } catch (NumberFormatException e) {
        }
    }

	private void addToGroup(String group, Double value) {
		Double min = groups.get(group);
		if (min == null || value < min) {
			groups.put(group, value);
		}
	}

	@SuppressWarnings("unchecked")
	public Map getResults() {
		return groups;
	}

	public String getTag() {
		return tag;
	}

	public void updateResult(String group, Number value) {
		if (this.tag != null && this.tag.length() > 0 && !group.startsWith(this.tag)) group = this.tag + LogProperties.getFunctionSplit() + group;
		addToGroup(group, value.doubleValue());
	}
	
	public void extractResult(String key, Number value, ValueSetter valueSetter, Map<String, Object> sharedFunctionData, int querySourcePos, int currentBucketPos, int totalBucketCount, Map<String, Object> sourceDataForBucket, long start) {
        Double myValue = groups.containsKey(key) ? groups.get(key) : null;
        super.extractResults(applyTag(tag, key), value, valueSetter, myValue);
    }
	public String getApplyToField() {
		return applyToGroup;
	}
	public String groupByGroup() {
		return groupByGroup;
	}
	public void reset() {
		groups = new ConcurrentHashMap<String, Double>();
	}
}
