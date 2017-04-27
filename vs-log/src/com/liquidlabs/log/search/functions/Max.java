package com.liquidlabs.log.search.functions;

import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.filters.Filter;
import com.liquidlabs.log.space.LogRequest;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Max extends FunctionBase implements FunctionFactory, Function  {

	private static final long serialVersionUID = 1L;
	
	private static final int MaxAggSize = LogProperties.getMaxCountAgg();
	private static final Logger LOGGER = Logger.getLogger(Max.class);
	String tag;
	String groupByGroup;
	String applyToGroup;
	
	
	private Map<String, Double> groups = new HashMap<String, Double>();

	public Max() {}
	
	public Max(String tag, String groupByGroup, String applyToGroup) {
        super(Max.class.getSimpleName());
		this.tag = tag;
		this.groupByGroup = groupByGroup;
		this.applyToGroup = applyToGroup;
	}

	public Function create() {
		return new Max(tag, groupByGroup, applyToGroup);
	}
	
	public boolean isBucketLevel() {
		return true;
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
                    if (max == null) max = 0.0;
                    double1 = max + 1;
                }

                addToGroup(group, double1);
                return;
            }
        } catch (NumberFormatException e) {
        }
    }

	protected void addToGroup(String group, Double value) {
		Double max = groups.get(group);
		if (max == null || value > max) {
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

	public void updateResult(String key, Number value) {
		if (this.tag != null && this.tag.length() > 0 && !key.startsWith(this.tag)) key = this.tag + LogProperties.getFunctionSplit() + key;
		addToGroup(key, value.doubleValue());
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
		this.groups = new ConcurrentHashMap<String, Double>();
	}



}
