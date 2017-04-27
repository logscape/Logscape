package com.liquidlabs.log.search.functions;

import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.filters.Filter;
import com.liquidlabs.log.space.LogRequest;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * 
 * Shows X/y values i.e. DistributedCache.ha.status
 * 
 */
public class By extends FunctionBase implements Function, FunctionFactory {

	public String tag;
	private int countLimit = LogProperties.getCountLimit();

	// WARNING
	public String groupByGroup = EMPTY_STRING;

	// "This is a warning message" (i.e. count types of warning messages)
	public String applyToGroup = EMPTY_STRING;

	private final static Logger LOGGER = Logger.getLogger(By.class);

    // determine if first() or last() functionality
    private boolean isLast = true;

	transient Map<String, String> groups;

	public By() {
	}

	public By(String tag, String groupByGroup, String applyToGroup, boolean isLast) {
        super(By.class.getSimpleName());
		this.tag = tag;
        this.isLast = isLast;
		if (!applyToGroup.equals("0") && groupByGroup.equals("*")) {
			this.applyToGroup = applyToGroup;
			this.groupByGroup = groupByGroup;
		} else if (applyToGroup.equals("0") || groupByGroup.equals("*")) {
			this.groupByGroup = EMPTY_STRING;
			this.applyToGroup = EMPTY_STRING;
		} else {
			this.groupByGroup = groupByGroup;
			this.applyToGroup = applyToGroup;
			if (this.groupByGroup.equals(applyToGroup))
				this.groupByGroup = EMPTY_STRING;
		}
	}

	public String name() {
		return "By";
	}

	public void setMaxAggSize(int maxAggSize) {
		this.countLimit = maxAggSize;
	}

	public String group() {
		return applyToGroup;
	}

	public String getTag() {
		return tag;
	}

	public boolean execute(final FieldSet fieldSet, final String[] fields,
                           final String pattern, final long time,
                           final MatchResult matchResult, String rawLineData, long requestStartTimeMs,
                           int lineNumber) {

		final String[] groupApply = super.getValues(matchResult, fieldSet,
				fields, applyToGroup, groupByGroup, tag, lineNumber);

		if (applyToGroup.length() == 0) {
			return false;
		} else if (groupByGroup.equals("*")) {
			return false;
		} else if (groupByGroup.length() != 0 && applyToGroup.length() != 0) {
			execute(groupApply[0], groupApply[1]);
		} 
		return false;
	}

	public void execute(String apply, String group) {

		if (apply == null)
			return;

		applyCount(apply, group);
		return;
	}

	transient int mapSize = 0;
	private int hitLimitLast = 1000;

	final public void applyCount(final String applyToGroupName, final String groupBy) {

		if (mapSize < countLimit && groupBy != null && applyToGroup != null) {
            if (isLast) getGroups().put(groupBy, applyToGroupName);
            else {
                if (!getGroups().containsKey(groupBy)) getGroups().put(groupBy, applyToGroupName);

            }
		}
	}

	public Function create() {
		return new By(tag, groupByGroup, applyToGroup, isLast);
	}

	@SuppressWarnings("unchecked")
	public Map getResults() {
		return new HashMap<String, String>(getGroups());
	}

	/**
	 * Aggregate other results into this map
	 */
	public void updateResult(String groupName, Map<String, Object> otherCountGroupsMap) {
		String otherValue = (String) otherCountGroupsMap.get(groupName);
		Map<String, String> groups = getGroups();
		groups.put(groupName, otherValue);
	}

	final private Map<String, String> getGroups() {
		if (groups == null) {
			groups = Collections.synchronizedMap(new LinkedHashMap<String, String>(){
				protected boolean removeEldestEntry(java.util.Map.Entry<String, String> eldest) {
					return groups.size() > hitLimitLast;
				}
			});
		}
		return groups;
	}

	public void updateResult(String groupBy, Number value) {
		// getGroups().put(groupBy, new IntValue(value.intValue()));
	}

	public void extractResult(FieldSet fieldSet, String currentGroupKey,
			String key, Map<String, Object> map, ValueSetter valueSetter,
			long start, long end, int currentBucketPos,
			int querySourcePos, int totalBucketCount,
			Map<String, Object> sharedFunctionData, long requestStartTimeMs, LogRequest request) {
		
		String value = (String) map.get(currentGroupKey);
		if (getTag().length() > 0) value = getTag() + LogProperties.getFunctionSplit() + value;
		valueSetter.set(value  +  ".by." +currentGroupKey, -1, true);
		
//		for (String mapKey : map.keySet()) {
//			String value = (String) map.get(mapKey);
//			if (valueSetter != null) {
//				//valueSetter.set(mapKey + ".by." + value, -1, true);
//				valueSetter.set(new StringBuilder(value).append(".by.").append(mapKey).toString(), -1, true);
//			}
//		}
	}

	public String getApplyToField() {
		return applyToGroup;
	}
	public String groupByGroup() {
		return groupByGroup;
	}
	public boolean isIgnoringHitLimit() {
		return true;
	}
	public void setHitLimit(int limit) {
		this.hitLimitLast = limit;
	}
	public void reset() {
		this.groups = null;
		getGroups();
	}

}
