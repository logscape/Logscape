package com.liquidlabs.log.search.functions;

import com.google.common.base.Splitter;
import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.filters.Filter;
import com.liquidlabs.log.space.LogRequest;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.procedure.TObjectIntProcedure;
import javolution.util.FastMap;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 
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
public class Count extends FunctionBase implements Function, FunctionFactory {
	
		private static final String NEWLINE_R = "\r";
		private static final String EOL = "\n";
		public String tag = "";
		private  int countLimit = LogProperties.getCountLimit();
		
		// WARNING
		public String groupByGroup = EMPTY_STRING;
		
		// "This is a warning message" (i.e. count types of warning messages)
		public String applyToGroup = EMPTY_STRING;
		
		private int topLimit = countLimit;

	
        private final static String SIMPLENAME = Count.class.getSimpleName();

		TObjectIntHashMap<String> groups;
		
		public Count(){
		}
		public Count(String tag, String groupByGroup, String applyToGroup) {
            super(SIMPLENAME);
            this.tag = tag;
			if (!applyToGroup.equals("0") && groupByGroup.equals("*")) {
				this.applyToGroup = applyToGroup;
				this.groupByGroup = groupByGroup;
			} else  if (applyToGroup.equals("0") || groupByGroup.equals("*")) {
				this.groupByGroup = EMPTY_STRING;
				this.applyToGroup = EMPTY_STRING;
			} else {
				this.groupByGroup = groupByGroup;
				this.applyToGroup = applyToGroup;
				if (this.groupByGroup.equals(applyToGroup)) this.groupByGroup = EMPTY_STRING;
			}
		}
		public void setTopLimit(int topLimit) {
			this.topLimit = topLimit;
		}
		public void setMaxAggSize(int maxAggSize) {
			this.countLimit = maxAggSize;
		}
		

		public String group() {
			return applyToGroup;
		}
		public String groupByGroup() {
			return groupByGroup;
		}
		public String getTag() {
			return tag;
		}

		public boolean execute(final FieldSet fieldSet, final String[] fields, final String pattern, final long time, final MatchResult matchResult, String rawLineData, long requestStartTimeMs, int lineNumber) {
            super.executeAgainstValues(matchResult, fieldSet, fields, applyToGroup, groupByGroup, tag, lineNumber);
			return true;
	}
    public void execute(String apply, String group) {

        if (applyToGroup.length() == 0) {
            // only count where apply has a value - it may be nulled
            executeUsingSingleCount();
        }
        else if (groupByGroup.equals("*")) {
            executeUsingGroupApply(null, tag);
        }
        else if (groupByGroup.length() != 0 && applyToGroup.length() != 0) {
            // nothing to do if the group by returns null && we need a value
            if (groupByGroup != null && groupByGroup.length() > 0) {
                if (group == null) return;
            }
            executeUsingGroupApply(group, apply);
        } else {

            executeUsingApplyOnly(apply);
        }

    }
		
		private void executeUsingSingleCount() {
			TObjectIntHashMap<String> groups = getGroups();
            groups.adjustOrPutValue(tag, 1, 1);
		}

		private boolean executeUsingGroupApply(String groupBy, String applyTo) {
			
			if (applyTo == null) return false;
            boolean groupByComma = applyTo.startsWith("_") && groupBy.indexOf(',',0) != -1;
            boolean applyToComma = applyTo.startsWith("_") && groupBy.indexOf(',',0) != -1;

            if (applyToComma && !groupByComma) {
                Iterable<String> split = splitComma.split(applyTo);
                for (String value : split) {
                    applyCount(value, groupBy);
                }
            } else if (groupByComma && !applyToComma) {
                Iterable<String> split = splitComma.split(groupBy);
                for (String value : split) {
                    applyCount(applyTo, value);
                }
            } else {
                applyCount(applyTo, groupBy);
            }

			return true;
		}
        transient Splitter splitComma = StringUtil.getCommaSplitter();
		final protected boolean executeUsingApplyOnly(final String applyToGroupName) {
			
			// error check the results are sufficient
			//String applyToGroupName = fieldSet.getField(applyToGroup, fields);
			
			if (applyToGroupName == null || applyToGroupName.length() == 0) {
				return false;
			}
			
			// BEWARE - this will suck if you are using fields like 'time' in a summary
//			if (!this.applyToGroup.contains(TIME) && !this.applyToGroup.contains(DATE) && applyToGroupName.contains(COMMA)) {
//				countArray(EMPTY_STRING, Arrays.split(COMMA, applyToGroupName));
//			} else if (applyToGroupName.contains(SEMI)) {
//				countArray(EMPTY_STRING, Arrays.split(SEMI, applyToGroupName));
//			} else {
            if (applyToGroup.startsWith("_") && applyToGroupName.indexOf(',') != -1) {
                Iterable<String> split = splitComma.split(applyToGroupName);
                for (String value : split) {
                    applyCount(value, null);
                }
            } else {
				applyCount(applyToGroupName, null);
            }
//			}
				return true;
		}
		final public void applyCount(final String applyToGroupName, final String groupBy) {
			final TObjectIntHashMap<String> groups = getGroups();
			String countKey = applyToGroupName;
			countKey = adjustCountKey(applyToGroupName, groupBy, countKey); 
			
			incrementIt(groups, countKey);
		}
		private void incrementIt(final TObjectIntHashMap<String> groups, String countKey) {

			if (groups.size() > countLimit) return;
            groups.adjustOrPutValue(fixKeyLength(countKey), 1, 1);
		}
		
		/**
		 * Try and prevent silly keys from killing the system. lengths under 50 are ok. bigger need more checking
		 * @param countKey
		 * @return
		 */
		private String fixKeyLength(String countKey) {
			if (countKey.length() < 50) return countKey;
			if (countKey.length() > 256) countKey = countKey.substring(0, 250); 
			if (countKey.contains(EOL)) countKey = countKey.substring(0, countKey.indexOf(EOL));
			else if (countKey.contains(NEWLINE_R)) countKey = countKey.substring(0, countKey.indexOf(NEWLINE_R));
			return countKey;
		}
		private String adjustCountKey(final String applyToGroupName, final String groupBy, String countKey) {
			if (groupBy != null && groupBy.length() > 0) {
				if (LogProperties.isFlipAgs()) countKey = format(groupBy, applyToGroupName);
				else countKey = format(groupBy, applyToGroupName);
			}
			return countKey;
		}



    public Function create() {
			return new Count(tag, groupByGroup, applyToGroup);
		}
		
    @SuppressWarnings("unchecked")
    public Map getResults() {
        TObjectIntHashMap<String> groups = getGroups();

        final Map<String, Integer> results = new HashMap<>();
        groups.forEachEntry(new TObjectIntProcedure<String>() {
            @Override
            public boolean execute(String s, int i) {
                results.put(s, i);
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
			TObjectIntHashMap<String> groups = getGroups();
			groups.adjustOrPutValue(groupName, otherCountValue, otherCountValue);
		}
		
		final private TObjectIntHashMap<String> getGroups() {
			if (groups == null) {
				groups = new TObjectIntHashMap<String>();
				}
			return groups;
		}
		public void updateResult(String groupBy, Number value) {
			getGroups().adjustOrPutValue(groupBy, value.intValue(), value.intValue());
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
			//if (this.tag != null && this.tag.length() > 0) mapKey = this.tag + Function.TAG_SEPARATOR + mapKey;
			if (this.allFilters() != null && this.allFilters().size() > 0) {
				for (Filter filter : this.allFilters()) {
					if (filter.isNumeric() && filter.execute(Integer.valueOf(number.intValue()).toString())) {
						Number viewedValue = fieldSet != null ? fieldSet.mapToView(this.applyToGroup,number.intValue()) : number.intValue();
						valueSetter.set(applyTag(tag, mapKey), viewedValue, false);
					}
				}
			} else {

				Number viewedValue = fieldSet != null  ? fieldSet.mapToView(this.applyToGroup,number.intValue()) : number.intValue();
				valueSetter.set(applyTag(tag, mapKey), viewedValue, false);
			}
		}
		public String getApplyToField() {
			return applyToGroup;
		}
		public void reset() {
			this.groups = null;
			getGroups();
		}
}
