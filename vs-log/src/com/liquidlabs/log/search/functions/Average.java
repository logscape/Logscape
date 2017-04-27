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
import java.util.Map.Entry;
import java.util.Set;

public class Average  extends FunctionBase implements FunctionFactory, Function {
	
	private static final int MaxAggSize = LogProperties.getMaxCountAgg();
	private final static Logger LOGGER = Logger.getLogger(Average.class);


	private static final long serialVersionUID = 1L;
	
	String tag;
	public String groupByGroup;
	public String applyToGroup;
	
	// keep total + count for each name
	// when get results is called translate map
	
	private Map<String, Averager> groups;

	
	public Average() {}

	public Average(String tag, String groupByGroup, String applyToGroup) {
        super(Average.class.getSimpleName());
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
	
	public Function create() {
		return new Average(tag, groupByGroup, applyToGroup);
	}
	
	public boolean execute(FieldSet fieldSet, String[] fields, String pattern, long time, MatchResult matchResult, String rawLineData, long requestStartTimeMs, int lineNumber) {
		super.executeAgainstValues(matchResult, fieldSet, fields, applyToGroup, groupByGroup, tag, lineNumber);
		//final String[] applyAndGroup = super.getValues(matchResult, fieldSet, fields, applyToGroup, groupByGroup, tag, lineNumber);
		//execute(applyAndGroup[0], applyAndGroup[1]);
        return true;
	}

	public void execute(String applyField, final String groupField) {
		if (applyField == null || applyField.length() == 0) return;
		if (groupField == null && groupByGroup != null && groupByGroup.length() > 0) return;

		try {

			String groupBy = groupField == null ? "" : groupField;
			Averager averager = getAverager(groupBy);
			if (applyField.indexOf(",") != -1) applyField = applyField.replaceAll(",", "");

			Double double1 = StringUtil.isDouble(applyField);
			if (double1 == null) double1 = 1.0;
			averager.add(double1);

		} catch(NumberFormatException nfe) {
			// ignore - cause can happen when given some dodgy line that has loose-group matching
		}
	}

	@SuppressWarnings("unchecked")
	public Map getResults() {
        return getGroups();
	}

	public String getTag() {
		return tag;
	}

	//////////// Bucket to Bucket version
	public void updateResult(String key, Map<String, Object> map) {

		for (Entry<String, Object> entry : map.entrySet()) {
			Averager averager = getAverager(entry.getKey());
			averager.add( ((Averager) entry.getValue()));
		}
	}

	public void updateResult(String groupName, Number value) {
        getAverager(groupName).add(value.doubleValue());
	}
	
	Averager getAverager(String aggKey) {
		Averager averager = getGroups().get(aggKey);
		if (averager == null) {
			averager = new Averager();
			getGroups().put(aggKey, averager);
		}
		return averager;
	}
	
	/////// Bucket to HistoItemXML Version
	public void extractResult(FieldSet fieldSet, String currentGroupKey, String key, Map<String, Object> map, ValueSetter valueSetter, long start, long end, int currentBucketPos, int querySourcePos, int totalBucketCount, Map<String, Object> sharedFunctionData, long requestStartTimeMs, LogRequest request) {
		for (Entry<String, Object> entry : map.entrySet()) {
			if (getGroups().size() > MaxAggSize) {
				LOGGER.warn("Skipping result due to excessive dataload:" + key);
                throw new RuntimeException("Too much load, failing");
			}
            String entyKey = entry.getKey();
		if (valueSetter != null) valueSetter.set(applyTag(tag, entyKey), ((Averager) entry.getValue()).average(), true);
		}
	}

    public Map<String, Averager> getGroups() {
		if (this.groups == null) {
			this.groups = new HashMap<String,Averager>();
		}
		return groups;
	}
	@Override
	public String getApplyToField() {
		return applyToGroup;
	}
	public String groupByGroup() {
		return groupByGroup;
	}

	

}
