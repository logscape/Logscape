package com.liquidlabs.log.search.functions;

import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.filters.Filter;
import com.liquidlabs.log.space.LogRequest;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.Map.Entry;

public class AverageMoving extends FunctionBase implements FunctionFactory, Function {

	private static final int MaxAggSize = LogProperties.getMaxCountAgg();
	private final static Logger LOGGER = Logger.getLogger(AverageMoving.class);


	private static final long serialVersionUID = 1L;

	String tag;
	public String groupByGroup;
	public String applyToGroup;

	// keep total + count for each name
	// when get results is called translate map

	private transient Map<String, Averager> groups;


	public AverageMoving() {}

	public AverageMoving(String tag, String groupByGroup, String applyToGroup) {
        super(AverageMoving.class.getSimpleName());
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
		return new AverageMoving(tag, groupByGroup, applyToGroup);
	}
	
	public boolean execute(FieldSet fieldSet, String[] fields, String pattern, long time, MatchResult matchResult, String rawLineData, long requestStartTimeMs, int lineNumber) {
		super.executeAgainstValues(matchResult, fieldSet, fields, applyToGroup, groupByGroup, tag, lineNumber);
        return true;
	}

	final public void execute(String applyField, final String groupField) {
		if (applyField == null || applyField.length() == 0) return;
		try {
			// nothing to do if the group by returns null
			if (groupField == null && groupByGroup != null && groupByGroup.length() > 0) return;
			
			String groupBy = groupField == null ? tag : groupField;
			Averager averager = getAverager(groupBy);
			if (applyField.indexOf(",") != -1) applyField = applyField.replaceAll(",", "");
			Double double1 = StringUtil.isDouble(applyField);
			if (double1 != null) averager.add(double1);
			else averager.add(1);
			return;
		} catch(NumberFormatException nfe) {
			// ignore - cause can happen when given some dodgy line that has loose-group matching
			return;
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
        sharedFunctionData.put(currentBucketPos+"", getGroups());
        sharedFunctionData.put("Size", totalBucketCount);
        sharedFunctionData.put(currentBucketPos+"_vs", valueSetter);
	}

	public Map<String, Averager> getGroups() {
		if (this.groups == null) {
			this.groups = new HashMap<String,Averager>();
		}
		return groups;
	}
	public void flushAggResultsForBucket(int querySourcePos, Map<String, Object> sharedFunctionData) {
        Integer totalBuckets = (Integer) sharedFunctionData.get("Size");
        TreeMap<String, String> keys = new TreeMap<String, String>();
        for (int i = 0; i < totalBuckets; i++) {
            Map<String, Averager> avgr = (Map<String, Averager>) sharedFunctionData.get(i + "");
            if (avgr != null) {
                for (String key : avgr.keySet()) {
                    keys.put(key, key);
                }
            }
        }
        // now we have the keys... iterate each bucket
        for (String key : keys.keySet()) {
            Window window3 = new Window(20);
//            Window window5 = new Window(5);
            Window window10 = new Window(10);
            for (int i = 0; i < totalBuckets; i++) {
                try {
                    Map<String, Averager> avgr = (Map<String, Averager>) sharedFunctionData.get(i + "");
                    apply(i, key, window3, avgr, sharedFunctionData);
//                    apply(i, key, window5, avgr, sharedFunctionData);
                    apply(i, key, window10, avgr, sharedFunctionData);

//                  Dont show average - let the user add it in...
//                    ValueSetter vs = (ValueSetter) sharedFunctionData.get(i + "_vs");
//                    if (vs != null && avgr != null) {
//                        Averager averager = avgr.get(key);
//                        if (averager != null) vs.set(key+ "_00", averager.average(), true);
//                        else vs.set(key + "_00", 0, true);
//                    }
                } catch (Throwable t)  {
                    t.printStackTrace();
                }

            }
        }
	}

    private void apply(int i, String key, Window window, Map<String, Averager> avgr, Map<String, Object> sharedFunctionData) {
        if (avgr != null && avgr.get(key) != null) window.add(avgr.get(key).average());
        ValueSetter vs = (ValueSetter) sharedFunctionData.get(i + "_vs");
        if (vs != null) vs.set(key + "_" + window.sizeString(), window.avg(), true);

    }

    private static class Window {
        int windowSize = 3;
        List<Double> values = new ArrayList<Double>();
        public Window(int size) {
            this.windowSize = size;
        }
        String sizeString() {
            if (windowSize < 10) return "0" + windowSize;
            else return Integer.toString(windowSize);
        }
        void add(double d) {
            values.add(d);
            if (values.size() > windowSize) values.remove(0);
        }
        double avg() {
            double sum = 0;
            for (Double value : values) {
                sum += value;
            }
            return sum/values.size();
        }
    }
	@Override
	public String getApplyToField() {
		return applyToGroup;
	}
	public String groupByGroup() {
		return groupByGroup;
	}

	

}
