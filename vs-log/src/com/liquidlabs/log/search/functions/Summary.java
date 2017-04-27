package com.liquidlabs.log.search.functions;

import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.filters.Filter;
import com.liquidlabs.log.space.LogRequest;
import org.apache.commons.math3.stat.descriptive.StorelessUnivariateStatistic;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Summary extends FunctionBase implements FunctionFactory, Function {

	String tag;
	String groupByGroup;
	String applyToGroup;

	Map<String, SummaryStatistics> groups = new ConcurrentHashMap<String, SummaryStatistics>();

	public Summary() {}

	public Summary(String tag, String groupByGroup, String applyToGroup) {
		this.tag = tag;
		this.groupByGroup = groupByGroup;
		this.applyToGroup = applyToGroup;
	}
	
	public Function create() {
		return new Summary(tag, groupByGroup, applyToGroup);
	}
	public boolean isBucketLevel() {
		return true;
	}

	public boolean execute(FieldSet fieldSet, String[] fields, String pattern, long time, MatchResult matchResult, String rawLineData, long requestStartTimeMs, int lineNumber) {
		
		final String[] groupApply = super.getValues(matchResult, fieldSet, fields, applyToGroup, groupByGroup, tag, lineNumber);

        if (groupApply[1] == null) {
            execute(groupByGroup, groupApply[0]);
        } else {
		    execute(groupApply[0], groupApply[1]);
        }
        return false;
	}

	public void execute(String apply, String group) {
		if (group == null || apply == null) return;
        SummaryStatistics summary = groups.get(group);
		if (summary == null) {
			summary = new SummaryStatistics();
			groups.put(group, summary);
		}
		try {
            String val = apply;
			if (val.indexOf(",") != -1) val = val.replaceAll(",", "");
			Double double1 = StringUtil.isDouble(val);
			if (double1 != null) summary.addValue(double1);
		} catch (Throwable t ) {
            t.printStackTrace();
		}
	}
    public void calculate(String fieldValue) {
        SummaryStatistics summary = groups.get(groupByGroup);
        if (summary == null) {
            summary = new SummaryStatistics();
            groups.put(groupByGroup, summary);
        }
        try {
            if (fieldValue.indexOf(",") != -1) fieldValue = fieldValue.replaceAll(",", "");
            Double double1 = StringUtil.isDouble(fieldValue);
            if (double1 != null) summary.addValue(double1);
        } catch (Throwable t ) {
            t.printStackTrace();
        }

    }

	@SuppressWarnings("unchecked")
	public Map getResults() {
		Map<String, SummaryStatistics> sums = new HashMap<String, SummaryStatistics>();
		for (Entry<String, SummaryStatistics> entry : groups.entrySet()) {
			sums.put(entry.getKey(), entry.getValue());
		}
		return sums;
	}

	public String getTag() {
		return tag;
	}



    public void updateResult(String key, Map<String, Object> map) {
        if (!groups.containsKey(key)) {
            groups.put(key, (SummaryStatistics) map.get(key));
        } else {
            try {
                SummaryStatistics thisAgg  = groups.get(key);
                SummaryStatistics otherAgg = (SummaryStatistics) map.get(key);
                merge(thisAgg, otherAgg);
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
	}

    transient Field nField = null;

    public void merge(SummaryStatistics thisAgg, SummaryStatistics otherAgg) {
        // TODO:
//        thisAgg.addValue(??);
        thisAgg.getMaxImpl().increment(otherAgg.getMax());
        thisAgg.getMinImpl().increment(otherAgg.getMin());
        thisAgg.getMeanImpl().increment(otherAgg.getMean());
        thisAgg.getSumImpl().increment(otherAgg.getSum());
        StorelessUnivariateStatistic varianceImpl = thisAgg.getVarianceImpl();

        setNFieldAccessor();
        try {
            nField.set(thisAgg, thisAgg.getN() + otherAgg.getN());
        } catch (IllegalAccessException e) {
        }

    }

    private void setNFieldAccessor() {
        if (nField == null) {
        Field field = null;
        try {
            field = SummaryStatistics.class.getDeclaredField("n");
        } catch (NoSuchFieldException e) {
        }
        field.setAccessible(true);
            nField = field;
        }
    }

    public void updateResult(String key, Number value) {
        throw new RuntimeException("Not Implemented");
	}
	
	public void extractResult(FieldSet fieldSet, String currentGroupKey, String key, Map<String, Object> map, ValueSetter valueSetter, long start, long end, int currentBucketPos, int querySourcePos, int totalBucketCount, Map<String, Object> sharedFunctionData, long requestStartTimeMs, LogRequest request) {
        if (valueSetter != null) {
            SummaryStatistics summaryStatistics = (SummaryStatistics) map.get(currentGroupKey);
            valueSetter.set(currentGroupKey + "_MAX", summaryStatistics.getMax(), false);
            valueSetter.set(currentGroupKey + "_SUM", summaryStatistics.getMax(), false);
            valueSetter.set(currentGroupKey + "_MIN", summaryStatistics.getMax(), false);
            valueSetter.set(currentGroupKey + "_MEAN", summaryStatistics.getMax(), false);
        }

	}

	public String group() {
		return applyToGroup;
	}

	@Override
	final public String getApplyToField() {
		return applyToGroup;
	}
	final public String groupByGroup() {
		return groupByGroup;
	}
	
	public void reset() {
        groups.clear();
	}
}
