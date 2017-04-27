package com.liquidlabs.log.search.functions;

import com.google.common.base.Splitter;
import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.collection.CompactCharSequence;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.filters.Filter;
import com.liquidlabs.log.space.LogRequest;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.procedure.TObjectIntProcedure;
import org.apache.log4j.Logger;

import java.util.*;

/**
 *      make use less mem by
 *      1. using short-strings
 *      2. using different map - i.e. trove
 * 3 use cases,
 * 1. count(tag, 0) = counts all lines with a match
 * 2. count(tag, 2) = counts all group(2) items
 * 3. count(tag, 1,2) = counts using a groupBy() - i.e. user/action
 *
 * 1. Count 
 * a b
 * a c
 * 2. count(1,2) into 
 * a_b=1
 * a_c=2
 * count(1) into
 * a=2
 *
 * @author Neil
 *
 */
public class CountFaster extends FunctionBase implements Function, FunctionFactory {

    static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(CountFaster.class);
    private final static String CLASSNAME = CountFaster.class.getSimpleName();
    public final static char[] EOL_ChaRS = "\r\n".toCharArray();
    public String tag;


    private  int countLimit = LogProperties.getCountLimit();


    // "This is a warning message" (i.e. count types of warning messages)
    public String countingField = EMPTY_STRING;


    TObjectIntHashMap<CompactCharSequence> groups;

    public CountFaster(){
    }
    public CountFaster(String tag, String applyToGroup) {
        super(CLASSNAME);
        this.tag = tag.intern();
        this.countingField = applyToGroup.intern();
    }
    public void setTopLimit(int topLimit) {

    }
    public void setMaxAggSize(int maxAggSize) {
        this.countLimit = maxAggSize;
    }

    public String group() {
        return countingField;
    }
    public String groupByGroup() {
        return "";
    }
    public String getTag() {
        return tag;
    }

    transient Splitter splitComma;
    public boolean execute(final FieldSet fieldSet, final String[] fields, final String pattern, final long time, final MatchResult matchResult, String rawLineData, long requestStartTimeMs, int lineNumber) {
        String fieldValue = fieldSet.getFieldValue(countingField, fields);
        if (fieldValue == null) return false;
        calculate(fieldValue);
        return false;
    }

    final public void calculate(String fieldValue) {
        final TObjectIntHashMap<CompactCharSequence> groups = getGroups();

        // only split on _tag
        if (this.countingField.length() > 1 && this.countingField.charAt(0) == '_' && this.countingField.charAt(1) == 't' && StringUtil.indexOf(fieldValue,new char[] {','}) != -1) {
            if (splitComma == null) splitComma = StringUtil.getCommaSplitter();
            Iterable<String> strings = splitComma.split(fieldValue);
            for (String string : strings) {
                incrementIt(groups, string.trim());
            }
        } else {
            incrementIt(groups, fieldValue);

        }

    }
    public void execute(String apply, String group) {
        calculate(apply);
    }

    private void incrementIt(final TObjectIntHashMap<CompactCharSequence> groups, String countKey) {
        if (countingField.length() > 0 && countingField.charAt(0) != '_') {
            countKey = fixKeyLength(countKey);
        }

        if (groups.size() > countLimit) return;
        groups.adjustOrPutValue(new CompactCharSequence(countKey), 1, 1);
    }
    @Override
    public void increment(String fieldName, String fieldValue, int numberOfEvents) {
        TObjectIntHashMap groups = getGroups();

        if (groups.size() > countLimit) return;
        groups.adjustOrPutValue(new CompactCharSequence(fieldValue), numberOfEvents, numberOfEvents);
    }

    /**
     * Try and prevent silly keys from killing the system. lengths under 50 are ok. bigger need more checking
     * @param countKey
     * @return
     */
    private String fixKeyLength(String countKey) {
        int length = countKey.length();
        if (length < 50) return countKey;
        if (length > 256) countKey = countKey.substring(0, 250);
        int eol = StringUtil.indexOf(countKey, EOL_ChaRS);
        if (eol != -1) countKey = countKey.substring(0, eol);
        return countKey;
    }

    public Function create() {
        return new CountFaster(tag, countingField);
    }

    @SuppressWarnings("unchecked")
    public Map getResults() {

        TObjectIntHashMap<CompactCharSequence> groups = getGroups();

        final Map<String, Integer> results = new HashMap<>();
        groups.forEachEntry(new TObjectIntProcedure<CompactCharSequence>() {
            @Override
            public boolean execute(CompactCharSequence s, int i) {
                results.put(s.toString(), i);
                return true;
            }
        });
        return results;
    }

    /**
     * Aggregate other results into this map
     */
    public void updateResult(String groupName, Map<String, Object> otherCountGroupsMap) {
        Integer otherCountValue = (Integer) otherCountGroupsMap.get(groupName);
        TObjectIntHashMap<CompactCharSequence> groups = getGroups();

        if (groups.size() > countLimit) {
            if (groups.containsKey(new CompactCharSequence(groupName))) groups.adjustOrPutValue(new CompactCharSequence(groupName), otherCountValue.intValue(), otherCountValue.intValue());
        } else {
            groups.adjustOrPutValue(new CompactCharSequence(groupName), otherCountValue.intValue(), otherCountValue.intValue());
        }
    }

    final private TObjectIntHashMap<CompactCharSequence> getGroups() {
        if (groups == null) {
            groups = new TObjectIntHashMap<>();
        }
        return groups;
    }
    public void updateResult(String groupName, Number value) {
        if (groups.size() > countLimit) {
            if (groups.containsKey(new CompactCharSequence(groupName))) groups.adjustOrPutValue(new CompactCharSequence(groupName), value.intValue(), value.intValue());
        } else {
            groups.adjustOrPutValue(new CompactCharSequence(groupName), value.intValue(), value.intValue());
        }
        getGroups().put(new CompactCharSequence(groupName), value.intValue());
    }

    public void extractResult(FieldSet fieldSet, String currentGroupKey, String key, Map<String, Object> map, ValueSetter valueSetter, long start, long end, int currentBucketPos, int querySourcePos, int totalBucketCount, Map<String, Object> sharedFunctionData, long requestStartTimeMs, LogRequest request) {

        Integer number = (Integer) map.get(currentGroupKey);

        if (valueSetter != null && number != null) {

            // if a post-processing filter is supplied - check the value qualifies -
            setValueAccordingToMyFilters(valueSetter, currentGroupKey, number, fieldSet);
        }
    }
    private void setValueAccordingToMyFilters(ValueSetter valueSetter, String mapKey, Integer number, FieldSet fieldSet) {
        if (mapKey.contains("'")) mapKey = mapKey.replaceAll("'", "");
        if (this.tag != null && this.tag.length() > 0) mapKey = this.tag + LogProperties.getFunctionSplit() + mapKey;
        if (this.allFilters() != null && this.allFilters().size() > 0) {
            for (Filter filter : this.allFilters()) {
                if (filter.isNumeric() && filter.execute(Integer.valueOf(number.intValue()).toString())) {
                    Number viewedValue = fieldSet != null ? fieldSet.mapToView(this.countingField,number.intValue()) : number.intValue();
                    valueSetter.set(mapKey, viewedValue, false);
                }
            }
        } else {

            Number viewedValue = fieldSet != null  ? fieldSet.mapToView(this.countingField,number.intValue()) : number.intValue();
            valueSetter.set(mapKey, viewedValue, false);
        }
    }
    final public String getApplyToField() {
        return countingField;
    }
    public void reset() {
        this.groups = null;
        getGroups();
    }
}
