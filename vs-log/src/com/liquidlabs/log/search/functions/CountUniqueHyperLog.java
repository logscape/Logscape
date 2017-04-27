package com.liquidlabs.log.search.functions;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.google.common.base.Splitter;
import com.liquidlabs.common.MurmurHash3;
import com.liquidlabs.common.StringUtil;
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
 * CountUnique instances of a group -
 * useful for ascertaining membership where each member writes to its own log line.
 * i.e.
 * engine:3301 joiningA
 * engine:3301 joiningA
 * engine:3302 joiningA
 * engine:3302 joiningA
 * engine:3302 joiningB.
 * <p/>
 * Results:
 * countUnique(1) == 2
 * countUnique(1,2) = 3301=1 3302=2
 */
public class CountUniqueHyperLog extends FunctionBase implements Function, FunctionFactory, Counter {

    private final static Logger LOGGER = Logger.getLogger(CountUniqueHyperLog.class);
    private final static String CLASSNAME = CountUniqueHyperLog.class.getSimpleName();

    private static final long serialVersionUID = 1L;

    // "This is a warning message" (i.e. count types of warning messages)
    public String applyToGroup;

    public String tag;

    private ICardinality counter;

    public CountUniqueHyperLog() {
    }

    public CountUniqueHyperLog(String tag, String applyToGroup) {
        super(CLASSNAME);
        this.tag = tag.intern();
        this.applyToGroup = applyToGroup.intern();
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

        String fieldValue = fieldSet.getFieldValue(applyToGroup, fields);
        if (fieldValue == null) return false;

        calculate(fieldValue);
        return false;
    }
    public void execute(String apply, String group) {
        calculate(apply);
    }

    transient Splitter splitComma = null;
    final public void calculate(String fieldValue) {

        checkCounter();

        if ((this.applyToGroup.length() > 0 && this.applyToGroup.charAt(0) == '_') && StringUtil.indexOf(fieldValue, ',') != -1 ) {
            if (splitComma == null) splitComma = StringUtil.getCommaSplitter();
            Iterable<String> strings = splitComma.split(fieldValue);
            for (String string : strings) {
                counter.offerHashed(MurmurHash3.hashString(string.trim(), 2));
            }
        } else {
            counter.offerHashed(MurmurHash3.hashString(fieldValue, 2));
        }
    }

    @Override
    public void increment(String fieldName, String fieldValue, int numberOfEvents) {
        checkCounter();
        counter.offerHashed(MurmurHash3.hashString(fieldValue, 2));
    }

    protected int increment(Integer integer) {
        return 1;
    }


    public String getTag() {
        return tag;
    }

    public Function create() {
        return new CountUniqueHyperLog(tag, applyToGroup);
    }

    public Map getResults() {
        HashMap hashMap = new HashMap();
        if (counter != null) hashMap.put(tag, counter);
        return hashMap;
    }

    ////////// Used to agg from other buckets with same data
    public void updateResult(String groupName, HyperLogLog otherGroupCountMap) {
        try {
            checkCounter();
            counter = counter.merge(otherGroupCountMap);
        } catch (CardinalityMergeException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    public void updateResult(String key, Map<String, Object> otherFunctionMap) {
        for(Object o : otherFunctionMap.values()) {
            updateResult(key, (HyperLogLog) o);
        }
    }


    public void extractResult(FieldSet fieldSet, String currentGroupKey, String key, Map<String, Object> map, ValueSetter valueSetter, long start, long end, int currentBucketPos, int querySourcePos, int totalBucketCount, Map<String, Object> sharedFunctionData, long requestStartTimeMs, LogRequest request) {
        try {
            if(valueSetter != null) {
                valueSetter.set(currentGroupKey, ((HyperLogLog)map.get(currentGroupKey)).cardinality(), false);
            }
        } catch (Exception e) {
            //
        }
    }

    final public String getApplyToField() {
        return applyToGroup;
    }


    @Override
    public long count() {
        try {
            checkCounter();
            return counter.cardinality();
        } catch (Exception e) {
            return -1;
        }
    }

    final private void checkCounter() {
        if (counter == null) counter = new HyperLogLog(10);
    }

    public void reset() {
        this.counter = null;
    }

}
