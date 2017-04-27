package com.liquidlabs.log.search.functions;

import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.filters.Filter;
import com.liquidlabs.log.space.LogRequest;
import com.liquidlabs.vso.VSOProperties;

import java.util.*;

public class FunctionBase implements Function {
	
	protected static final int MaxAggSize = LogProperties.getMaxCountAgg();
	protected static final String COMMA = ",";
	protected static final String SEMI = ";";
	protected static final String EMPTY_STRING = "";
    private String funcName = "";
    private static String postAggFunctionsHeadChar;

    public FunctionBase(){};

    public FunctionBase(String funcName){
        this.funcName = funcName;// funcName.intern();
    };

    public static String getPostAggFunctionsHeadChar() {
        return "+";
    }

    public boolean isBucketLevel() {
		return true;
	}
    private transient String name;
	public String name(){
        if (name == null) name =getClass().getSimpleName();
		return name;
	}
	public void flushAggResultsForBucket(int querySourcePos, Map<String, Object> sharedFunctionData) {
	}

	public List<Filter> filters() {
		return null;
	}

	public void reset() {
	}

	final protected boolean isUsingMatchResult(final MatchResult matchResult, final String group) {
		return matchResult != null && matchResult.groups() > 0 && groupIsInt(group);
	}
	protected boolean groupIsInt(String groupByGroup2) {
		return StringUtil.isInteger(groupByGroup2) != null;
	}
	protected String escapeOurSpecialChars(String group) {
		if (group.indexOf(LogProperties.getFunctionSplit()) != -1) {
			group = group.replaceAll("\\" + LogProperties.getFunctionSplit(), "_");
		}
		return group;
	}
	transient Boolean oldSchoolBasedApply;
	transient Boolean oldSchoolBasedGroup;
    public void execute(String apply, String group) {

	}
	final protected String[] getValues(final MatchResult matchResult, final FieldSet fieldSet, final String[] fields, final String applyToGroup, final String groupByGroup, final String tag, int lineNumber) {
		String applyField;
		String groupField;
		if (oldSchoolBasedApply == null) oldSchoolBasedApply = isUsingMatchResult(matchResult, applyToGroup);
		if (oldSchoolBasedGroup == null) oldSchoolBasedGroup = isUsingMatchResult(matchResult, groupByGroup);
		
		if (oldSchoolBasedApply) applyField = matchResult.getGroup(Integer.parseInt(applyToGroup));
		else applyField = fieldSet.getFieldValue(applyToGroup, fields, matchResult);
		
		if (groupByGroup == null || groupByGroup.length() == 0)  groupField = tag;
		else if (oldSchoolBasedGroup) groupField = matchResult.getGroup(Integer.parseInt(groupByGroup));
		else groupField = fieldSet.getFieldValue(groupByGroup, fields, matchResult);
		
		if (groupField != null) groupField = escapeOurSpecialChars(groupField);
		if (applyField != null) applyField = escapeOurSpecialChars(applyField);
		return new String[] { applyField, groupField };
	}

	public void updateResult(String groupName, Map<String, Object> otherCountGroupsMap) {}
	public void updateResult(String groupBy, Number value) {}
	public void updateResult(String key, Set<String> otherFunctionMap) {}
	public boolean isIgnoringHitLimit() { return false;}

	@Override
	public List<Filter> allFilters() {
		return null;
	}
    protected String format(String first, String second) {
        return first + LogProperties.getFunctionSplit() + second;
    }


    public boolean execute(FieldSet fieldSet, String[] fields, String pattern, long time, MatchResult matchResult, String rawLineData, long requestStartTimeMs, int lineNumber) {
		return false;
	}
    public void calculate(String fieldValue) {
    }

	public void extractResult(FieldSet fieldSet, String currentGroupKey, String key, Map<String, Object> otherFunctionMap, ValueSetter valueSetter, long start, long end, int currentBucketPos,
			int querySourcePos, int totalBucketCount, Map<String, Object> sharedFunctionData, long requestStartTimeMs, LogRequest request) {
	}

	public void extractResult(String key, Number object, ValueSetter valueSetter, Map<String, Object> sharedFunctionData, int querySourcePos, int currentBucketPos, int totalBucketCount,
			Map<String, Object> sourceDataForBucket, long start) {
	}

	public void extractResult(String key, Set<String> otherFunctionMap, ValueSetter valueSetter, long start, long end, int currentBucketPos, int querySourcePos, int totalBucketCount,
			Map<String, Object> sharedFunctionData) {
	}
    protected void extractResults(String key, Number value, ValueSetter valueSetter, Number myValue) {
        if (valueSetter != null){
            valueSetter.set(key, (myValue != null ? myValue : value), true);
        }
    }


    public String getApplyToField() {
		return "";
	}
    protected String applyTag(String tag, String entryKey) {
        if (tag.equals(entryKey)) return tag;
		if (entryKey.length() == 0 && tag != null) return tag;
        if (entryKey.startsWith(tag + LogProperties.getFunctionSplit())) return entryKey;
        return tag != null && tag.length() > 0 ? tag + TAG_SEPARATOR + entryKey : entryKey;
    }


    /**
	 * Copy/Clone out the results here because the function cache will be cleared once flushed
	 */
	public Map getResults() {
		return null;
	}

	public String getTag() {
		return "";
	}

	public String group() {
		return null;
	}

	public String groupByGroup() {
		return "";
	}

	public Function create() {
		return null;
	}

    public int hashCode() {
        return toStringId().hashCode();
    }
    public boolean equals(Object obj) {
        if (!(obj instanceof FunctionBase)) return false;
        String otherId = ((Function) obj).toStringId();
        return this.toStringId().equals(otherId);
    }

    public String toString() {
        return toStringId();
    }
    transient String stringId = null;
    public String toStringId() {
        if (stringId == null)  stringId = getStringId(funcName,getTag(),groupByGroup(),getApplyToField());
        return stringId;
    }


	public String getStringId(String... items) {
		StringBuilder result = new StringBuilder();
		for (String string : items) {
			if (string.length() > 0) result.append(string.trim()).append(" ");
		}
		return result.toString().trim();
	}

    static public List<Function> getPostAggFunctions(Function srcFunction, List<Function> functions) {
        ArrayList<Function> results = new ArrayList<Function>();
        if (functions != null) {
            for (Function function : functions) {
                if (function.getApplyToField().charAt(0) == FunctionBase.getPostAggFunctionsHeadChar().charAt(0)) {
                    if (function != srcFunction && function.getApplyToField().substring(1).equals(srcFunction.getTag())) {
                        function.reset();
                        results.add(function);
                    }
                }
            }
        }

        return results;
    }

    public static List<Function> sortPostAggFunctions(List<Function> functions) {
        List<Function> results = new ArrayList<Function>();
        List<Function> aggs = getPostAggFunctionsFor(functions);

        for (Function function : functions) {
            if (!aggs.contains(function)) results.add(function);
        }
        results.addAll(aggs);
        return results;
    }

    private static List<Function> getPostAggFunctionsFor(List<Function> functions) {
        List<Function> aggs = new ArrayList<Function>();
        for (Function function : functions) {
            aggs.addAll(getPostAggFunctions(function, functions));
        }
        return aggs;
    }

    public static boolean isPostAggFunction(Function aggFunction, List<Function> functions) {
        List<Function> postAggFunctionsFor = getPostAggFunctionsFor(functions);
        return postAggFunctionsFor.contains(aggFunction);
    }

	@Override
	public void increment(String fieldName, String fieldValue, int numberOfEvents) {
		// No-op
	}

	/**
	 * Split into regular functions and global - i.e. apply field == 0
	 * @param functions
	 * @return
	 */
	public static List<GlobalFunction> getGlobalFunctions(List<Function> functions) {
		ArrayList<GlobalFunction> results = new ArrayList<>();
		for (Function function : functions) {
			if (function instanceof GlobalFunction && function.getApplyToField().equals("0")) results.add((GlobalFunction) function);
		}
		functions.removeAll(results);
		return results;
	}

	public void executeAgainstValues(MatchResult matchResult, FieldSet fieldSet, String[] values, String applyToGroup, String groupByGroup, String tag, int lineNumber) {

		if (fieldSet.isMultiField(applyToGroup)) {
			List<String> applyFields = fieldSet.getMultiFields(applyToGroup);
			for (String applyField : applyFields) {
				final String[] applyAndGroup = getValues(matchResult, fieldSet, values, applyField, groupByGroup, tag, lineNumber);
				execute(applyAndGroup[0], applyAndGroup[1]);
			}

		} else {
			final String[] applyAndGroup = getValues(matchResult, fieldSet, values, applyToGroup, groupByGroup, tag, lineNumber);
			execute(applyAndGroup[0], applyAndGroup[1]);
		}
	}
}
